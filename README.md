# Shopping Sessions

This Rust pipeline reads the mounted shopping events, ad clicks and identity links and writes the two daily Parquet tables. The reader schedules fixed-size byte ranges across workers and checks each physical CSV record before decoding it. For cold event and click inputs, it tries aligned 2 MiB reads with a 4 KiB overlap. It uses immutable mappings for cached inputs, unsupported direct reads, and records that extend past the overlap.

A union-find joins linked IDs and retains each component's smallest name. Event-ID partitions keep the earliest ingested copy, even when it is invalid. Each partition stores purchase details and distant ingest timestamps separately from its common event records. A second partition assigns each canonical person a local index shared by events and clicks. A reversible hash represents person IDs across shuffles. Narrow time windows use compact records, with a full-width path for wider windows or large category catalogs. A sliding window counts distinct assisting campaigns and resolves last-click credit. Each worker writes its user rows directly to Parquet. Campaign totals are combined across workers.

Build the Dockerfile for linux/amd64. The final image contains one statically linked x86-64 executable. The parser selects AVX2/BMI2/POPCNT at runtime and uses a portable scanner when those features are unavailable. Set `SHOPPING_FORCE_SCALAR=1` to exercise that path. The executable reads `DATA_DIR` (default `/data`), writes `OUTPUT_DIR` (default `/output`), and uses `NCPU` worker threads (default 8). The output directory must be writable by UID 1000.

## Local leaderboard

Run `./localboard.sh` on a Linux host with eight available logical CPUs, systemd user cgroups, the official checker and prepared datasets. Set `BOARD_BINARY` to the executable extracted from the candidate image and `BOARD_DATA` to the private evaluator directory. `BOARD_PYTHON` selects a Python installation with Polars. `BOARD_CPUS` selects the eight logical CPU IDs.

The evaluator directory contains the official `compare.py`, its sibling `run_fixtures.py`, the 36 `fixtures/`, and an empty writable `scratch/`. Each of the following directories has a matching `NAME-answer/` containing both golden tables. Prepare inputs offline with the published generator.

| Directory | Seed | Events | Clicks | Reference |
|---|---:|---:|---:|---|
| dev | 1 | 1,000,000 | 250,000 | Official dev answer |
| practice | 42 | 20,000,000 | 5,000,000 | Independent Python/DuckDB implementation |
| final | 43 | 60,000,000 | 15,000,000 | Independent Python/DuckDB implementation |
| holdout | 54819 | 1,500,000 | 600,000 | Independent Python/DuckDB implementation |

The reference implementation is preserved at commit `3c70613`. The default release run compares all fixture, dev, reserved holdout and scale outputs. Practice uses five fresh runs. Final uses three, or five with `--rerank`. The report includes the official practice repeat statistic, the calibrated board prediction in seconds, and the final median. A mismatch, crash or limit failure stops scoring.

Every run flushes dirty pages on its private input files and requests eviction with `POSIX_FADV_DONTNEED`. Linux `mincore` verifies zero resident input pages before starting the clock. If installed, `fincore` independently checks the result. Input verification, cgroup setup, correctness comparison, image inspection and container cleanup are outside the clock.

On a Docker host set `BOARD_IMAGE` to a local image tag and `BOARD_CPUS` to eight available CPUs. The simulator records the entire `docker run` wall clock and the container's start-to-exit time. The official repeat decision uses the latter. The native backend times the fresh executable through exit. The simulator measures CPU seconds in the Docker workload's own systemd slice, or with child process resource accounting on the native backend. It divides this by eight to estimate CPU service time. The difference from lifecycle time estimates uncovered wait. Seven simple transfer models compete on leave-one-out mean absolute error across scored references. The report includes that error and a median-ratio cross-check. All model parameters are fit on the training rows in each fold. To recalibrate a host, run `--calibrate` for each scored release and store at least three rows of `[median_lifecycle_seconds, median_cpu_seconds, board_seconds]` in `docker-cpu-calibration.json` or `native-cpu-calibration.json` under `BOARD_DATA`. Built-in rows belong to the original calibration machines. The Docker backend requires a local Linux daemon with the systemd cgroup driver. Four native release measurements are available. Leave-one-out error measures historical fit. A new implementation or host can have a larger prediction error.

Use `--public` for fixtures and dev, `--trial` for a three-run practice experiment, and `--tune` for practice and final development checks. These modes preserve the reserved release holdout.
