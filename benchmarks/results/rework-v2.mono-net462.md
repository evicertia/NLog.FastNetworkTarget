# v2 Performance Report (Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2))

- **Date**: `2026-11-22 15:30:04 UTC`
- **OS**: `Unix 24.6.0.0` (64-bit)
- **Runtime**: `Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2)`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Attempted/received/missing | Delivered/s | MiB/s | CPU ms/100k delivered | GC (0/1/2) | B/msg* | Verified |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| `SingleThread_Direct` | 50000/50000/0 | 85,623 | 37.2 | 2,333.6 | 11 / 0 / 0 | 317 | PASS |
| `Concurrent8_Direct` | 100000/100000/0 | 452,253 | 196.6 | 1,489.8 | 23 / 1 / 1 | 285 | PASS |
| `Concurrent16_Direct` | 100000/100000/0 | 500,737 | 217.8 | 1,437.8 | 33 / 3 / 3 | 0 | PASS |
| `Concurrent32_Direct` | 100000/100000/0 | 506,352 | 220.9 | 1,458.2 | 33 / 3 / 3 | 626 | PASS |
| `Batch1000_Direct` | 100000/100000/0 | 122,438 | 53.1 | 903.5 | 23 / 1 / 1 | 432 | PASS |
| `HermaE2E_FullPipeline` | 50000/50000/0 | 374,846 | 124.1 | 2,072.1 | 21 / 1 / 1 | 180 | PASS |
| `LargePayload_32KB` | 2500/2500/0 | 23,920 | 747.5 | 7,175.7 | 1 / 1 / 1 | 9,985 | PASS |

* Mono uses live heap delta when cumulative allocation accounting is unavailable; not comparable to .NET allocation counts.
