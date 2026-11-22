# Baseline Performance Report (v1 - .NET 8.0.11)

- **Date**: `2026-11-22 05:28:40 UTC`
- **OS**: `Unix 15.6.1` (64-bit)
- **Runtime**: `.NET 8.0.11`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Throughput (msg/s) | Bandwidth (MB/s) | CPU (ms/100k) | GC (0/1/2) | Alloc (B/msg) | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `SingleThread_Direct` | 110,279 | 47.8 | 5,024.8 | 15 / 6 / 2 | 2,302 | PASS |
| `Concurrent8_Direct` | 229,111 | 99.6 | 1,700.1 | 31 / 13 / 5 | 2,251 | PASS |
| `Concurrent16_Direct` | 240,485 | 104.6 | 1,898.8 | 32 / 13 / 6 | 2,254 | PASS |
| `Concurrent32_Direct` | 236,488 | 102.9 | 1,988.1 | 32 / 13 / 6 | 2,261 | PASS |
| `Batch1000_Direct` | 262,881 | 114.0 | 970.5 | 21 / 7 / 3 | 1,540 | PASS |
| `HermaE2E_FullPipeline` | 4,948 | 0.6 | 841.4 | 22 / 10 / 8 | 1,743 | FAIL |
| `LargePayload_32KB` | 43,835 | 1,369.8 | 5,179.8 | 31 / 10 / 6 | 98,968 | PASS |
