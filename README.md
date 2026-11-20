# Shopping Sessions

This Rust pipeline reads the mounted shopping events, ad clicks and identity links and writes the two daily Parquet tables. Its CSV reader splits immutable memory mappings at newline boundaries and checks each physical record before decoding it.

A union-find joins linked IDs and retains each component's smallest name. Event-ID partitions keep the earliest ingested copy, even when it is invalid. A second partition by canonical person keeps the full event and click timeline together. A sliding window counts distinct assisting campaigns and resolves last-click credit. Each worker writes its user rows directly to Parquet. Campaign totals are combined across workers.

Build the Dockerfile for linux/amd64. The final image contains one statically linked x86-64-v3 executable. It reads `DATA_DIR` (default `/data`), writes `OUTPUT_DIR` (default `/output`), and uses `NCPU` worker threads (default 8). The output directory must be writable by UID 1000.
