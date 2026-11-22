# v2 Performance Report (.NET 8.0.11)

- **Date**: `2026-11-22 14:59:07 UTC`
- **OS**: `Unix 15.6.1` (64-bit)
- **Runtime**: `.NET 8.0.11`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Throughput (msg/s) | Bandwidth (MB/s) | CPU (ms/100k) | GC (0/1/2) | Alloc (B/msg) | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `SingleThread_Direct` | 389,412 | 169.2 | 1,457.1 | 9 / 4 / 1 | 1,456 | PASS |
| `Concurrent8_Direct` | 421,012 | 183.0 | 931.4 | 26 / 15 / 4 | 1,666 | PASS |
| `Concurrent16_Direct` | 303,646 | 132.1 | 1,323.3 | 29 / 14 / 6 | 2,086 | PASS |
| `Concurrent32_Direct` | 373,278 | 162.5 | 1,391.7 | 29 / 14 / 6 | 2,112 | PASS |
| `Batch1000_Direct` | 375,705 | 163.0 | 460.0 | 10 / 4 / 1 | 758 | PASS |
| `HermaE2E_FullPipeline` | 549,868 | 181.5 | 698.1 | 9 / 3 / 0 | 1,555 | PASS |
| `LargePayload_32KB` | 62,506 | 1,953.3 | 3,693.7 | 6 / 3 / 1 | 19,318 | PASS |
