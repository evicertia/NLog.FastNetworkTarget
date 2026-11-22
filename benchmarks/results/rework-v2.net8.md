# v2 Performance Report (.NET 8.0.11)

- **Date**: `2026-11-22 15:29:11 UTC`
- **OS**: `Unix 15.6.1` (64-bit)
- **Runtime**: `.NET 8.0.11`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Attempted/received/missing | Delivered/s | MiB/s | CPU ms/100k delivered | GC (0/1/2) | B/msg* | Verified |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| `SingleThread_Direct` | 50000/50000/0 | 280,475 | 121.9 | 1,403.7 | 4 / 1 / 0 | 710 | PASS |
| `Concurrent8_Direct` | 100000/100000/0 | 394,596 | 171.5 | 588.3 | 14 / 6 / 2 | 957 | PASS |
| `Concurrent16_Direct` | 100000/100000/0 | 301,036 | 130.9 | 634.0 | 18 / 8 / 4 | 1,298 | PASS |
| `Concurrent32_Direct` | 100000/100000/0 | 286,425 | 124.7 | 756.3 | 17 / 7 / 3 | 1,246 | PASS |
| `Batch1000_Direct` | 100000/100000/0 | 363,955 | 157.9 | 469.2 | 10 / 4 / 1 | 764 | PASS |
| `HermaE2E_FullPipeline` | 50000/50000/0 | 687,805 | 227.1 | 670.1 | 9 / 4 / 0 | 1,557 | PASS |
| `LargePayload_32KB` | 2500/2500/0 | 60,531 | 1,891.6 | 4,664.8 | 7 / 6 / 1 | 20,203 | PASS |

* Mono uses live heap delta when cumulative allocation accounting is unavailable; not comparable to .NET allocation counts.
