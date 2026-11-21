"""Time fresh processes. Validate output outside the clock on every run."""
import os
import select
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime
import json

data = Path(os.environ["BOARD_DATA"])
image = os.environ.get("BOARD_IMAGE")
if image:
    metadata = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))[0]
    if metadata["Os"] != "linux" or metadata["Architecture"] != "amd64" or metadata["Size"] > 8_000_000_000:
        sys.exit("invalid image platform or size")
ratio = float(os.environ.get("BOARD_NATIVE_RATIO", "0.96"))
tuning = sys.argv[1:] == ["--tune"]
rerank = sys.argv[1:] == ["--rerank"]
public = sys.argv[1:] == ["--public"]
if sys.argv[1:] and not (tuning or rerank or public):
    sys.exit("usage: localboard.sh [--tune | --rerank | --public]")
env = dict(os.environ, NCPU="8", MEM_GB="24", PYTHONDONTWRITEBYTECODE="1")
cg = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().split("0::")[-1].strip().lstrip("/")
assert len(os.sched_getaffinity(0)) == 8
assert int((cg/"memory.max").read_text()) == 24 * 1024**3
assert int((cg/"memory.swap.max").read_text()) == 0
assert (cg/"cpu.max").read_text().split() == ["800000", "100000"]
assert int((cg/"pids.max").read_text()) == 4096


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
    args = ["docker", "create", "--platform", "linux/amd64", "--cpus", "8",
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
    container = subprocess.check_output(args + [image], text=True).strip()
    try:
        yield ["docker", "start", "-a", container], container
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, stdout=subprocess.DEVNULL)


def run(name, inp, answer, limit, allow_cutoff=False):
    with tempfile.TemporaryDirectory(dir=data / "scratch") as tmp:
        base = Path(tmp)
        out, scratch = base / "out", base / "scratch"
        out.mkdir()
        scratch.mkdir()
        execution = dict(env, DATA_DIR=str(inp), OUTPUT_DIR=str(out),
                         SCRATCH_DIR=str(scratch), HOME=str(scratch), TMPDIR=str(scratch))
        with command(inp, out, scratch) as (argv, container), (base / "stdout").open("wb") as stdout, (base / "stderr").open("wb") as stderr:
            start = time.perf_counter()
            proc = subprocess.Popen(argv, env=execution, cwd=scratch,
                                    stdout=stdout, stderr=stderr, start_new_session=True)
            fd = os.pidfd_open(proc.pid)
            try:
                while True:
                    remaining = start + limit - time.perf_counter()
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
                state = json.loads(subprocess.check_output(["docker", "inspect", container]))[0]["State"]
                rc = rc or state["ExitCode"] or int(state["OOMKilled"])
                elapsed = (datetime.fromisoformat(state["FinishedAt"].replace("Z","+00:00")) -
                           datetime.fromisoformat(state["StartedAt"].replace("Z","+00:00"))).total_seconds()
            if rc:
                raise RuntimeError(f"{name} exit {rc}: {(base / 'stderr').read_text()[-2000:]}")
        if disk_bytes(out, scratch) > 50_000_000_000 or (base / "stdout").stat().st_size > 50_000_000:
            raise RuntimeError(f"{name} disk or stdout limit")
        subprocess.run([sys.executable, data / "compare.py", out, answer],
                       check=True, stdout=subprocess.DEVNULL, env=env)
        if name in ("practice", "final"):
            stages = (base / "stderr").read_text().strip().replace("\n", ", ")
            print(f"{name} {elapsed:.4f}s exact | {stages}", flush=True)
        return elapsed


def dataset(name, limit, allow_cutoff=False):
    return run(name, data/name, data/(name+"-answer"), limit, allow_cutoff)


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
if first < 20:
    deadline = 300-first
    for _ in range(2):
        t = dataset("practice", max(.001, deadline), True)
        times.append(300 if t is None else t)
        deadline = max(0, deadline-(deadline if t is None else t))
official = statistics.median(times)
while len(times) < 5:
    times.append(dataset("practice", 300))
median = statistics.median(times)
final_times = [dataset("final", 180) for _ in range(1 if tuning else (5 if rerank else 3))]
print(f"practice_rule_s={official:.4f} local_20M_median5_s={median:.4f} "
      f"native_ratio={ratio:.2f} predicted_board_s={median*ratio:.4f} "
      f"final_60M_s={statistics.median(final_times):.4f}", flush=True)
