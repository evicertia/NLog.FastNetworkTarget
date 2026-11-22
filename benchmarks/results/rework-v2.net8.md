# v2 Performance Report (.NET 8.0.11)

- **Date**: `2026-11-22 05:43:31 UTC`
- **OS**: `Unix 15.6.1` (64-bit)
- **Runtime**: `.NET 8.0.11`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Throughput (msg/s) | Bandwidth (MB/s) | CPU (ms/100k) | GC (0/1/2) | Alloc (B/msg) | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `SingleThread_Direct` | 81,828 | 35.6 | 6,433.0 | 12 / 9 / 2 | 1,460 | PASS |
| `Concurrent8_Direct` | 295,723 | 128.5 | 2,180.3 | 29 / 18 / 5 | 1,623 | PASS |
| `Concurrent16_Direct` | 356,731 | 155.1 | 1,340.1 | 28 / 13 / 4 | 1,973 | PASS |
| `Concurrent32_Direct` | 299,464 | 130.3 | 1,583.8 | 32 / 15 / 5 | 2,222 | PASS |
| `Batch1000_Direct` | 336,854 | 146.1 | 526.5 | 11 / 5 / 2 | 748 | PASS |
| `HermaE2E_FullPipeline` | 685,061 | 226.2 | 677.4 | 10 / 4 / 2 | 1,599 | PASS |
| `LargePayload_32KB` | 56,979 | 1,780.6 | 4,766.8 | 12 / 9 / 2 | 30,362 | PASS |
