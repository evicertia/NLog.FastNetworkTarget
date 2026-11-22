# rework-v2.mono-net462 (Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2))

- **Date**: `2026-11-22 16:41:09 UTC`
- **OS**: `Unix 24.6.0.0` (64-bit)
- **Runtime**: `Mono 6.12.0.188 (2020-02/ca8abcb6bc4 Thu Oct 13 14:26:22 EDT 2022) (.NET Framework 4.6.2)`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Attempted/received/missing | Delivered/s | MiB/s | CPU ms/100k delivered | GC (0/1/2) | B/msg* | Verified |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| `SingleThread_Direct` | 50000/50000/0 | 78,092 | 33.9 | 2,451.6 | 11 / 0 / 0 | 317 | PASS |
| `Concurrent8_Direct` | 100000/100000/0 | 392,835 | 170.8 | 1,566.8 | 23 / 1 / 1 | 306 | PASS |
| `Concurrent16_Direct` | 100000/100000/0 | 390,523 | 169.9 | 1,599.1 | 27 / 2 / 2 | 227 | PASS |
| `Concurrent32_Direct` | 100000/100000/0 | 342,672 | 149.5 | 1,527.5 | 35 / 3 / 3 | 0 | PASS |
| `Batch1000_Direct` | 100000/100000/0 | 111,773 | 48.5 | 980.4 | 23 / 1 / 1 | 438 | PASS |
| `HermaE2E_FullPipeline` | 50000/50000/0 | 342,029 | 113.2 | 2,008.6 | 21 / 1 / 1 | 323 | PASS |
| `LargePayload_32KB` | 2500/2500/0 | 29,794 | 931.1 | 6,094.2 | 1 / 1 / 1 | 5,920 | PASS |

* Mono uses live heap delta when cumulative allocation accounting is unavailable; not comparable to .NET allocation counts.
