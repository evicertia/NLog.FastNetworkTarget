# Baseline Performance Report (v1 - Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2))

- **Date**: `2026-11-22 05:30:17 UTC`
- **OS**: `Unix 24.6.0.0` (64-bit)
- **Runtime**: `Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2)`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Throughput (msg/s) | Bandwidth (MB/s) | CPU (ms/100k) | GC (0/1/2) | Alloc (B/msg) | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `SingleThread_Direct` | 66,914 | 29.1 | 3,003.4 | 35 / 2 / 2 | 1,082 | PASS |
| `Concurrent8_Direct` | 61,994 | 26.9 | 3,781.5 | 70 / 4 / 4 | 1,081 | PASS |
| `Concurrent16_Direct` | 62,023 | 27.0 | 3,818.0 | 70 / 4 / 4 | 1,080 | PASS |
| `Concurrent32_Direct` | 60,745 | 26.5 | 4,285.3 | 70 / 4 / 4 | 1,078 | PASS |
| `Batch1000_Direct` | 66,278 | 28.7 | 2,839.8 | 52 / 1 / 1 | 422 | PASS |
| `HermaE2E_FullPipeline` | 4,900 | 1.4 | 2,464.1 | 53 / 6 / 6 | 287 | FAIL |
| `LargePayload_32KB` | 17,522 | 547.6 | 10,190.7 | 14 / 14 / 14 | 3,300 | PASS |
