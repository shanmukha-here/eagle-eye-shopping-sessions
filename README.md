# Shopping Sessions

Eagle Eye's CPU pipeline reads the event, click and identity-link CSVs from the mounted dataset and writes two daily Parquet tables. It uses Polars to validate physical CSV lines and partition IDs. DuckDB selects event winners, groups linked people into sessions and aggregates credit and seven-day assists.

Build the top-level Dockerfile for linux/amd64. The default command reads `DATA_DIR` (default `/data`), writes `OUTPUT_DIR` (default `/output`) and creates its temporary files under `SCRATCH_DIR` (default `/scratch`). Output and scratch must be writable by UID 1000. `NCPU` and `MEM_GB` configure the worker count and memory budget.
