"""Time fresh processes. Validate output outside the clock on every run."""
import os
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

data = Path(os.environ["BOARD_DATA"])
image = os.environ.get("BOARD_IMAGE")
if image:
    metadata = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))[0]
    if metadata["Os"] != "linux" or metadata["Architecture"] != "amd64" or metadata["Size"] > 8_000_000_000:
        sys.exit("invalid image platform or size")
# Fit separate cold measurements on each host. Never apply a warm fit to cold data.
calibration = data / ("docker-calibration.json" if image else "native-calibration.json")
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
    args = ["docker", "run", "--name", container, "--platform", "linux/amd64", "--cpus", "8",
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
            print(f"{name} {elapsed:.4f}s exact cold=0/{pages} pages{clock} | {stages}", flush=True)
        return elapsed, lifecycle


def dataset(name, limit, allow_cutoff=False):
    return run(name, data/name, data/(name+"-answer"), limit, allow_cutoff)


def predict(elapsed):
    if calibration.exists():
        points = json.loads(calibration.read_text())
    else:
        points = [[2.9604, 2.06], [2.6091, 1.60]] if image else [[5.3767, 2.06], [4.9941, 1.60]]
    (old, old_board), (recent, recent_board) = points
    if old <= recent or old_board <= recent_board:
        sys.exit("cold reference medians do not establish a positive slope")
    slope = (old_board-recent_board)/(old-recent)
    intercept = recent_board-slope*recent
    return f"intercept_s={intercept:.6f} slope={slope:.6f} predicted_board_s={intercept+slope*elapsed:.4f}"


if calibrate or quick:
    reps = 5 if calibrate else 3
    measured = statistics.median(dataset("practice", 300)[0] for _ in range(reps))
    print(f"cold_20M_median{reps}_s={measured:.4f}", flush=True)
    if quick:
        print(predict(measured), flush=True)
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
        times.append((300, 300) if t is None else t)
        deadline = max(0, deadline-(deadline if t is None else t[1]))
official = statistics.median(t[1] for t in times)
while len(times) < 5:
    times.append(dataset("practice", 300))
median = statistics.median(t[0] for t in times)
final_times = [dataset("final", 180) for _ in range(1 if tuning else (5 if rerank else 3))]
print(f"practice_rule_s={official:.4f} local_20M_median5_s={median:.4f} "
      f"{predict(median)} "
      f"final_60M_s={statistics.median(t[0] for t in final_times):.4f} "
      f"final_rule_s={statistics.median(t[1] for t in final_times):.4f}", flush=True)
