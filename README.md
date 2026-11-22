# Shopping Sessions pipeline

Native Rust implementation of the Builders Buildathon "Shopping Sessions" rules: parallel mmap CSV parsing, sharded hash dedup and ID interning, union-find for linked IDs, sort-based sessionization and sliding-window attribution, Parquet output.

Build: `docker build -t ss .`  Run: reads `$DATA_DIR`, writes `$OUTPUT_DIR`.

MIT licensed. No data is included.
