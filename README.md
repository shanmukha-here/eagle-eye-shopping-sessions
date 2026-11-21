# Shopping Sessions

This Rust pipeline reads the mounted shopping events, ad clicks and identity links and writes the two daily Parquet tables. Its CSV reader splits immutable memory mappings at newline boundaries and checks each physical record before decoding it.

A union-find joins linked IDs and retains each component's smallest name. Event-ID partitions keep the earliest ingested copy, even when it is invalid. A second partition by canonical person keeps the full event and click timeline together. A sliding window counts distinct assisting campaigns and resolves last-click credit. Each worker writes its user rows directly to Parquet. Campaign totals are combined across workers.

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

On a Docker host set `BOARD_IMAGE` to a local image tag and `BOARD_CPUS` to eight available CPUs. The simulator records the entire `docker run` wall clock and the container's start-to-exit time. The official repeat decision uses the latter. The native backend times the fresh executable through exit. Each backend fits board seconds to its own cold five-run medians for two scored releases. Use `--calibrate` to measure a reference and place the two pairs `[[older_local, older_board], [newer_local, newer_board]]` in `docker-calibration.json` or `native-calibration.json` under `BOARD_DATA`. The built-in pairs apply to the original calibration machines. Two points give no independent estimate of prediction error.

Use `--public` for fixtures and dev, `--trial` for a three-run practice experiment, and `--tune` for practice and final development checks. These modes preserve the reserved release holdout.
