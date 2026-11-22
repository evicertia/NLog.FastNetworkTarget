# Matched-workload TCP benchmark

Herma-style E2E results, median-throughput iteration of three on a 12-core Mac. Every iteration delivered 50,000 newline-delimited records for 50,000 attempted. CPU is **process CPU milliseconds per 100,000 received records**, including the in-process receiver.

| Target source | .NET 8 delivered/s | CPU ms/100k | Mono 6.12 delivered/s | CPU ms/100k |
|---|---:|---:|---:|---:|
| `608ce90` (v1) | 321,971 | 1,163.5 | 156,500 | 2,692.8 |
| `514fb76` | 644,811 | 647.2 | 382,274 | 2,114.0 |
| `7877a89` | 624,010 | 613.2 | 396,953 | 2,125.7 |
| Working tree after delivery fixes | 687,805 | 670.1 | 374,846 | 2,072.1 |

The current tree delivers about 2.14× v1 throughput on .NET 8 and 2.40× on Mono. Compared with `7877a89`, throughput rose 10% on .NET 8 while CPU per delivered record rose 9%; Mono throughput fell 6% while CPU per record fell 3%. These short loopback runs do not establish a production regression or a universal speedup.

Run `bash benchmarks/compare.sh 608ce90 514fb76 7877a89` from the repository root. The script exports each revision's library **source** to a temporary directory and builds it with the same project targeting settings, NLog 4.7.11, benchmark source, warmup, queue sizes and payloads. It never connects to a live collector. Reports named `<sha>-<framework>.md` contain all seven scenarios; `rework-v2.*.md` records the working tree. Historical `baseline-v1.*.md` reports used a different workload and include failed delivery checks. Do not use those rows for ratios.

The E2E workload uses 16 logging threads, Herma-style JSON, an async wrapper with batch size 1,000, an always-enabled filter, and a warm IPv4 loopback connection. Both queues are configured at 126,976 for this equal-delivery comparison; this is not a replay of live pillar limits, network latency, or exception-heavy traffic. Receiver verification counts lines, not unique event IDs. Direct tests preconstruct events before releasing workers and starting measurement. Mono live heap deltas are not cumulative allocated bytes.
