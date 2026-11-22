# v2 Performance Report (Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2))

- **Date**: `2026-11-22 05:43:36 UTC`
- **OS**: `Unix 24.6.0.0` (64-bit)
- **Runtime**: `Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2)`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Throughput (msg/s) | Bandwidth (MB/s) | CPU (ms/100k) | GC (0/1/2) | Alloc (B/msg) | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `SingleThread_Direct` | 82,568 | 35.9 | 2,569.4 | 18 / 3 / 3 | 1,214 | PASS |
| `Concurrent8_Direct` | 390,006 | 169.5 | 1,948.0 | 41 / 5 / 5 | 1,345 | PASS |
| `Concurrent16_Direct` | 375,629 | 163.4 | 2,041.3 | 47 / 5 / 5 | 1,592 | PASS |
| `Concurrent32_Direct` | 413,398 | 180.0 | 2,419.3 | 52 / 6 / 6 | 1,526 | PASS |
| `Batch1000_Direct` | 118,587 | 51.4 | 924.9 | 22 / 1 / 1 | 408 | PASS |
| `HermaE2E_FullPipeline` | 238,155 | 78.7 | 2,283.7 | 21 / 1 / 1 | 291 | PASS |
| `LargePayload_32KB` | 29,440 | 920.0 | 6,104.6 | 2 / 2 / 2 | 15,846 | PASS |
