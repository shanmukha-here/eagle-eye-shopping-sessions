"""Time fresh processes. Validate output outside the clock on every run."""
import os
import fcntl
import resource
import ctypes
import mmap
import uuid
import select
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from board_model import select as select_model, estimate

data = Path(os.environ["BOARD_DATA"])
image = os.environ.get("BOARD_IMAGE")
if image:
    metadata = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))[0]
    if metadata["Os"] != "linux" or metadata["Architecture"] != "amd64" or metadata["Size"] > 8_000_000_000:
        sys.exit("invalid image platform or size")
# Fit separate cold measurements on each host. Never apply a warm fit to cold data.
calibration = data / ("docker-cpu-calibration.json" if image else "native-cpu-calibration.json")
calibrate = sys.argv[1:] == ["--calibrate"]
quick = sys.argv[1:] == ["--trial"]
tuning = sys.argv[1:] == ["--tune"]
rerank = sys.argv[1:] == ["--rerank"]
public = sys.argv[1:] == ["--public"]
if sys.argv[1:] and not (tuning or rerank or public or calibrate or quick):
    sys.exit("usage: localboard.sh [--tune | --rerank | --public | --calibrate | --trial]")
env = dict(os.environ, NCPU="8", MEM_GB="24", PYTHONDONTWRITEBYTECODE="1")
cg = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().split("0::")[-1].strip().lstrip("/")
assert len(os.sched_getaffinity(0)) == 8
assert int((cg/"memory.max").read_text()) == 24 * 1024**3
assert int((cg/"memory.swap.max").read_text()) == 0
assert (cg/"cpu.max").read_text().split() == ["800000", "100000"]
assert int((cg/"pids.max").read_text()) == 4096


# Docker keeps the systemd parent slice after its container cgroup disappears.
# Its cumulative CPU charge includes every worker and the process exit path.
slice_name = f"shoppingboard{os.getuid()}.slice"
if image:
    lock_file = Path(os.environ["XDG_RUNTIME_DIR"]) / "shopping-localboard.lock"
    clock_lock = lock_file.open("a")
    fcntl.flock(clock_lock, fcntl.LOCK_EX)
    if subprocess.check_output(["docker", "info", "--format", "{{.CgroupDriver}}"], text=True).strip() != "systemd":
        sys.exit("Docker CPU profiling requires the systemd cgroup driver")


def cpu_seconds():
    if image:
        stat = Path("/sys/fs/cgroup") / slice_name / "cpu.stat"
        if not stat.exists():
            return 0.0
        return int(dict(line.split() for line in stat.read_text().splitlines())["usage_usec"]) / 1e6
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def evict(inp):
    """Evict just these inputs. mincore inspects mappings without faulting their pages."""
    pages = 0
    libc = ctypes.CDLL(None, use_errno=True)
    files = []
    for path in sorted(inp.rglob("*")):
        if not path.is_file() or not path.stat().st_size:
            continue
        files.append(path)
        with path.open("rb") as f:
            # Dirty pages can survive DONTNEED on freshly generated inputs.
            os.fsync(f.fileno())
            count = (os.fstat(f.fileno()).st_size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
            pages += count
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_COPY) as region:
                addr = ctypes.addressof(ctypes.c_char.from_buffer(region))
                vec = (ctypes.c_ubyte * count)()
                for attempt in range(5):
                    os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                    if libc.mincore(ctypes.c_void_p(addr), ctypes.c_size_t(len(region)), vec):
                        raise OSError(ctypes.get_errno(), "mincore")
                    if 1 not in bytes(vec).translate(PAGE_BITS):
                        break
                    time.sleep(.05)
                else:
                    raise RuntimeError(f"could not evict {path}")
    if shutil.which("fincore"):
        for start in range(0, len(files), 128):
            counts = subprocess.check_output(
                ["fincore", "--bytes", "--noheadings", "--raw", "-o", "RES", *files[start:start+128]],
                text=True).split()
            if len(counts) != len(files[start:start+128]) or any(int(n) for n in counts):
                raise RuntimeError("fincore reported resident or missing inputs")
    return pages


PAGE_BITS = bytes(i & 1 for i in range(256))


def disk_bytes(out, scratch):
    total = 0
    for root in (out, scratch):
        for path in root.rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass  # A completed spill can disappear during sampling.
    return total


@contextmanager
def command(inp, out, scratch):
    if not image:
        yield [os.environ["BOARD_BINARY"]], None
        return
    out.chmod(0o777)
    scratch.chmod(0o777)
    container = "shopping-local-" + uuid.uuid4().hex
    args = ["docker", "run", "--cgroup-parent", slice_name, "--name", container, "--platform", "linux/amd64", "--cpus", "8",
            "--cpuset-cpus", ",".join(map(str, sorted(os.sched_getaffinity(0)))),
            "--memory", "24g", "--memory-swap", "24g", "--pids-limit", "4096",
            "--network", "none", "--read-only", "--user", "1000:1000",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--tmpfs", "/tmp:rw,size=64m,mode=1777",
            "-v", f"{inp.resolve()}:/data:ro", "-v", f"{out}:/output",
            "-v", f"{scratch}:/scratch", "-e", "DATA_DIR=/data", "-e", "OUTPUT_DIR=/output",
            "-e", "SCRATCH_DIR=/scratch", "-e", "HOME=/scratch", "-e", "NCPU=8", "-e", "MEM_GB=24"]
    if "SHOPPING_FORCE_SCALAR" in os.environ:
        args += ["-e", "SHOPPING_FORCE_SCALAR=1"]
    try:
        yield args + [image], container
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)


def docker_state(container):
    result = subprocess.run(["docker", "inspect", "--format", "{{json .State}}", container],
                            capture_output=True, text=True)
    return json.loads(result.stdout) if result.returncode == 0 else {}


def container_time(state):
    timestamp = lambda key: datetime.fromisoformat(state[key].replace("Z", "+00:00"))
    end = datetime.now(timezone.utc) if state["Running"] else timestamp("FinishedAt")
    return (end - timestamp("StartedAt")).total_seconds()


def run(name, inp, answer, limit, allow_cutoff=False):
    with tempfile.TemporaryDirectory(dir=data / "scratch") as tmp:
        base = Path(tmp)
        out, scratch = base / "out", base / "scratch"
        out.mkdir()
        scratch.mkdir()
        execution = dict(env, DATA_DIR=str(inp), OUTPUT_DIR=str(out),
                         SCRATCH_DIR=str(scratch), HOME=str(scratch), TMPDIR=str(scratch))
        with command(inp, out, scratch) as (argv, container), (base / "stdout").open("wb") as stdout, (base / "stderr").open("wb") as stderr:
            pages = evict(inp)
            cpu_start = cpu_seconds()
            start = time.perf_counter()
            proc = subprocess.Popen(argv, env=execution, cwd=scratch,
                                    stdout=stdout, stderr=stderr, start_new_session=True)
            fd = os.pidfd_open(proc.pid)
            try:
                while True:
                    remaining = start + limit - time.perf_counter()
                    if remaining <= 0 and container:
                        state = docker_state(container)
                        if state.get("Status") in ("running", "exited"):
                            remaining = limit - container_time(state)
                            if state["Status"] == "exited" and remaining >= 0:
                                remaining = .1
                        elif time.perf_counter()-start < limit+20:
                            remaining = .1  # Docker has not started the container clock.
                    if remaining <= 0:
                        if allow_cutoff:
                            return None
                        raise RuntimeError(f"{name} TIMEOUT")
                    if select.select([fd], [], [], min(0.5, remaining))[0]:
                        elapsed = time.perf_counter() - start
                        rc = proc.wait()
                        break
                    if disk_bytes(out, scratch) > 50_000_000_000 or stdout.tell() > 50_000_000:
                        raise RuntimeError(f"{name} disk or stdout limit")
            finally:
                os.close(fd)
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
            used_cpu = cpu_seconds() - cpu_start
            if container:
                state = docker_state(container)
                rc = rc or state["ExitCode"] or int(state["OOMKilled"])
                lifecycle = container_time(state)
            else:
                lifecycle = elapsed
            if rc:
                raise RuntimeError(f"{name} exit {rc}: {(base / 'stderr').read_text()[-2000:]}")
            if lifecycle > limit:
                if allow_cutoff:
                    return None
                raise RuntimeError(f"{name} TIMEOUT")
        if disk_bytes(out, scratch) > 50_000_000_000 or (base / "stdout").stat().st_size > 50_000_000:
            raise RuntimeError(f"{name} disk or stdout limit")
        subprocess.run([sys.executable, data / "compare.py", out, answer],
                       check=True, stdout=subprocess.DEVNULL, env=env)
        if name in ("practice", "final"):
            stages = (base / "stderr").read_text().strip().replace("\n", ", ")
            clock = f" lifecycle_s={lifecycle:.4f}" if container else ""
            print(f"{name} {elapsed:.4f}s exact cold=0/{pages} pages{clock} cpu_s={used_cpu:.5f} | {stages}", flush=True)
        return elapsed, lifecycle, used_cpu


def dataset(name, limit, allow_cutoff=False):
    return run(name, data/name, data/(name+"-answer"), limit, allow_cutoff)


def predict(times):
    # Rows: median container lifecycle (or native process time), CPU seconds, board seconds.
    points = json.loads(calibration.read_text()) if calibration.exists() else (
        [[2.5874, 8.59367, 2.06], [2.3138, 6.65119, 1.6], [1.3575, 6.44747, 1.5], [1.189, 5.21428, 1.01]] if image else
        [[4.8245, 12.37469, 2.06], [4.4715, 9.55942, 1.6], [4.0621, 7.6297, 1.5], [3.9095, 6.40911, 1.01]])
    mae, _, name, _, parameters = select_model(points)[0]
    wall = statistics.median(t[1] for t in times)
    cpu = statistics.median(t[2] for t in times)
    predicted = estimate(parameters, (wall, cpu))
    ratio = statistics.median(row[2]/row[0] for row in points)
    return (f"model={name!r} loo_mae_s={mae:.4f} cpu_s={cpu:.4f} "
            f"ratio_estimate_s={ratio*wall:.4f} predicted_board_s={predicted:.4f}")


if calibrate or quick:
    reps = 5 if calibrate else 3
    times = [dataset("practice", 300) for _ in range(reps)]
    measured = statistics.median(t[0] for t in times)
    print(f"cold_20M_median{reps}_s={measured:.4f}", flush=True)
    if quick:
        print(predict(times), flush=True)
    sys.exit(0)


fixtures = sorted(p for p in (data/"fixtures").iterdir() if p.is_dir())
assert len(fixtures) == 36
for fixture in fixtures:
    run(fixture.name, fixture/"input", fixture/"expected", 180)
print("fixtures 36/36 exact", flush=True)
dataset("dev", 180)
print("public dev exact", flush=True)
if public:
    sys.exit(0)
if not tuning:
    dataset("holdout", 180)
    print("reserved holdout exact", flush=True)
first = dataset("practice", 300)
times = [first]
if first[1] < 20:
    deadline = 300-first[1]
    for _ in range(2):
        t = dataset("practice", max(.001, deadline), True)
        times.append((300, 300, 0) if t is None else t)
        deadline = max(0, deadline-(deadline if t is None else t[1]))
official = statistics.median(t[1] for t in times)
while len(times) < 5:
    times.append(dataset("practice", 300))
median = statistics.median(t[0] for t in times)
final_times = [dataset("final", 180) for _ in range(1 if tuning else (5 if rerank else 3))]
print(f"practice_rule_s={official:.4f} local_20M_median5_s={median:.4f} "
      f"{predict(times)} "
      f"final_60M_s={statistics.median(t[0] for t in final_times):.4f} "
      f"final_rule_s={statistics.median(t[1] for t in final_times):.4f}", flush=True)
