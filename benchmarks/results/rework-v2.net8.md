# rework-v2.net8 (.NET 8.0.11)

- **Date**: `2026-11-22 16:40:56 UTC`
- **OS**: `Unix 15.6.1` (64-bit)
- **Runtime**: `.NET 8.0.11`
- **Processors**: `12 logical cores`
- **NLog Version**: `4.0.0.0`

| Scenario | Attempted/received/missing | Delivered/s | MiB/s | CPU ms/100k delivered | GC (0/1/2) | B/msg* | Verified |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | :---: |
| `SingleThread_Direct` | 50000/50000/0 | 374,440 | 162.7 | 1,214.6 | 4 / 1 / 0 | 710 | PASS |
| `Concurrent8_Direct` | 100000/100000/0 | 345,947 | 150.4 | 589.3 | 14 / 6 / 2 | 951 | PASS |
| `Concurrent16_Direct` | 100000/100000/0 | 307,953 | 134.0 | 654.8 | 18 / 8 / 2 | 1,344 | PASS |
| `Concurrent32_Direct` | 100000/100000/0 | 292,693 | 127.4 | 671.9 | 20 / 8 / 3 | 1,494 | PASS |
| `Batch1000_Direct` | 100000/100000/0 | 372,296 | 161.5 | 473.6 | 10 / 4 / 1 | 761 | PASS |
| `HermaE2E_FullPipeline` | 50000/50000/0 | 672,132 | 221.9 | 665.7 | 10 / 4 / 1 | 1,593 | PASS |
| `LargePayload_32KB` | 2500/2500/0 | 60,276 | 1,883.6 | 4,194.2 | 6 / 5 / 1 | 19,959 | PASS |

* Mono uses live heap delta when cumulative allocation accounting is unavailable; not comparable to .NET allocation counts.
