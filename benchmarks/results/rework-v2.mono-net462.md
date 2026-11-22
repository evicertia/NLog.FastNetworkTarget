# v2 Performance Report (Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2))

- **Date**: `2026-11-22 14:59:20 UTC`
- **OS**: `Unix 24.6.0.0` (64-bit)
- **Runtime**: `Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2)`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Throughput (msg/s) | Bandwidth (MB/s) | CPU (ms/100k) | GC (0/1/2) | Alloc (B/msg) | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `SingleThread_Direct` | 81,908 | 35.6 | 2,554.2 | 19 / 2 / 2 | 1,067 | PASS |
| `Concurrent8_Direct` | 416,052 | 180.8 | 1,966.0 | 40 / 4 / 4 | 1,091 | PASS |
| `Concurrent16_Direct` | 468,586 | 204.2 | 1,949.6 | 48 / 5 / 5 | 1,283 | PASS |
| `Concurrent32_Direct` | 466,534 | 203.5 | 2,179.6 | 50 / 5 / 5 | 1,327 | PASS |
| `Batch1000_Direct` | 116,393 | 50.5 | 940.9 | 23 / 1 / 1 | 466 | PASS |
| `HermaE2E_FullPipeline` | 377,472 | 125.0 | 2,031.7 | 21 / 1 / 1 | 137 | PASS |
| `LargePayload_32KB` | 30,090 | 940.3 | 6,132.4 | 1 / 1 / 1 | 4,842 | PASS |
