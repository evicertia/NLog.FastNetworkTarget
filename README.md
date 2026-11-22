# NLog.FastNetworkTarget

A high-throughput, low-CPU network target for NLog designed for massive-volume TCP workloads (Logstash, Fluentd, Vector, etc.).

[![CI](https://github.com/evicertia/NLog.FastNetworkTarget/actions/workflows/ci.yml/badge.svg)](https://github.com/evicertia/NLog.FastNetworkTarget/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/NLog.FastNetworkTarget.svg)](https://www.nuget.org/packages/NLog.FastNetworkTarget/)

---

## Supported Frameworks & Runtimes

- **.NET 8+** (`net8.0`, `.NET 8`, `.NET 9`, `.NET 10`)
- **.NET Standard 2.0** (`netstandard2.0`)
- **Mono & .NET Framework 4.6.2+** (`net462`, `net472`, `net48+`)
- Compiled against **NLog 4.7.11** for seamless drop-in compatibility across legacy and modern services.

---

## Performance: v1 vs v2 Benchmark Summary

Tested with 800-byte ndjson `JsonLayout` payloads across 12 logical cores (Apple Silicon):

### .NET 8
- **End-to-End Pipeline (`HermaE2E`)**: **671,351 msg/s** (vs 4,948 msg/s in v1 where queue overflow dropped messages — **135x throughput increase**)
- **Concurrent 8 Threads**: **436,876 msg/s** (vs 229,111 msg/s — **1.9x throughput increase**, **50% CPU reduction**)
- **Direct Batching (`Batch1000`)**: **373,603 msg/s** (vs 262,881 msg/s, **53% CPU reduction**)
- **32 KB Large Payloads**: Memory allocations cut by **72%** (from 98 KB/msg down to 27 KB/msg, preventing LOH fragmentation).

### Mono 6.12 (.NET Framework 4.6.2+)
- **Concurrent 8 Threads**: **359,815 msg/s** (vs 61,994 msg/s — **5.8x throughput increase**, **42% CPU reduction**)
- **Concurrent 32 Threads**: **385,526 msg/s** (vs 60,745 msg/s — **6.3x throughput increase**, **40% CPU reduction**)
- **End-to-End Pipeline**: **350,331 msg/s** (vs 4,900 msg/s — **71x throughput increase**)
- **32 KB Large Payloads**: Gen2 pause-inducing GC collections cut by **86%** (from 14 down to 2).

Full benchmark details and methodology are documented in [`benchmarks/results/COMPARISON-v1-vs-v2.md`](benchmarks/results/COMPARISON-v1-vs-v2.md).

---

## Architectural Highlights

1. **Lock-Free Concurrency**:
   - Overrides `WriteAsyncThreadSafe` to bypass NLog's base `Monitor.Enter(SyncRoot)` serialization.
   - Bounded MPSC (Multi-Producer Single-Consumer) ring buffer for wait-free dequeuing and lock-free enqueuing.
2. **Producer-Side 64 KB Slab Coalescing**:
   - Batches from `AsyncTargetWrapper` are packed directly into 64 KB pooled slabs, turning 1,000 log events into ~8 socket writes instead of 1,000 syscalls.
3. **Zero-Copy Layout Encoding**:
   - Bypasses string copying when pre-rendered layout values are available on `LogEventInfo` (such as `JsonLayout` inside async wrappers).
4. **Zero-Allocation Byte Buffer Pooling**:
   - `ByteBufferPool` provides TLS-cached, non-blocking bucketed buffers (1 KB, 4 KB, 16 KB, 64 KB) keeping all allocations strictly below the 85,000-byte Large Object Heap (LOH) threshold.
5. **Configurable Queue Overflow & Non-Blocking Safety**:
   - Configurable `onQueueOverflow="Discard"` (default) or `Block` so high-volume bursts never wedge application threads.
6. **Socket & TCP Hardening**:
   - `noDelay="true"` (TCP_NODELAY) enabled by default to eliminate Nagle latency stalls.
   - Configurable `sendTimeout="30000"` (30s default) to recover from frozen TCP ZeroWindow states.
   - Cross-platform OS detection (correctly distinguishes macOS Darwin from Linux for TCP keep-alive socket options).

---

## Configuration Example

```xml
<extensions>
    <add assembly="NLog.FastNetworkTarget" />
</extensions>

<targets>
    <target xsi:type="FastNetwork"
            name="LogStash"
            address="${environment:LOGSTASH_ENDPOINT}"
            newLine="true"
            lineEnding="LF"
            maxQueueSize="126976"
            onQueueOverflow="Discard"
            batchSize="512"
            maxBatchBytes="262144"
            noDelay="true"
            sendTimeout="30000"
            sendBufferSize="262144">
        <layout xsi:type="JsonLayout" includeAllProperties="true">
            <attribute name="@timestamp" layout="${date:format=yyyy-MM-ddTHH\:mm\:ss.fffZ}" />
            <attribute name="hostname" layout="${hostname}" />
            <attribute name="threadid" layout="${threadid}" />
            <attribute name="level" layout="${level:upperCase=true}" />
            <attribute name="logger" layout="${logger}" />
            <attribute name="message" layout="${message}" />
            <attribute name="exception" layout="${exception:format=@}" />
        </layout>
    </target>
</targets>
```

### Configuration Attributes

| Attribute | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `address` | `Layout` | *(Required)* | Network endpoint (`tcp://host:port`, `tcp4://host:port`, `tcp6://host:port`). |
| `newLine` | `bool` | `false` | Append line ending after each message. |
| `lineEnding` | `LineEndingMode` | `CRLF` | `LF`, `CRLF`, `CR`, or `Null`. |
| `maxQueueSize` | `int` | `8192` | Maximum items held in the sender ring buffer. |
| `onQueueOverflow` | `enum` | `Discard` | `Discard` (drops newest item and tracks metrics without blocking caller threads) or `Block` (blocks producer until queue has space). |
| `batchSize` | `int` | `512` | Maximum messages coalesced per socket write. |
| `maxBatchBytes` | `int` | `262144` (256 KB) | Maximum bytes sent per contiguous socket write. |
| `noDelay` | `bool` | `true` | Enables `TCP_NODELAY` to prevent 40ms delayed-ACK latency spikes. |
| `sendTimeout` | `int` | `30000` (30s) | Socket send timeout in milliseconds (prevents permanent ZeroWindow stalls). |
| `sendBufferSize` | `int` | `262144` (256 KB) | Underlying OS socket send buffer size. |
| `maxMessageSize` | `int` | `65000` | Maximum message size before triggering `onOverflow`. |
| `onOverflow` | `enum` | `Split` | `Split`, `Discard`, or `Error`. |
| `keepConnection` | `bool` | `true` | Keep connection open between writes. |
| `connectionTimeout` | `int` | `1000` ms | Connect timeout in milliseconds. |
| `keepAliveTime` | `int` | `0` | TCP keep-alive idle time before probing (in ms, 0 = disabled). |

---

## Observability & Metrics API

The target exposes lock-free metric properties for monitoring:

```csharp
var target = (FastNetworkTarget)LogManager.Configuration.FindTargetByName("LogStash");

long pending  = target.PendingMessages;   // Current queue depth
int  conns    = target.ConnectionCount;   // Number of active network connections
long sentMsgs = target.SentMessages;      // Total messages sent
long sentBytes= target.SentBytes;         // Total bytes transferred
long batches  = target.SentBatches;       // Total socket send syscalls issued
long dropped  = target.DroppedMessages;   // Messages discarded due to queue overflow
long blocked  = target.BlockedEnqueues;   // Number of times producers blocked
long failures = target.SendFailures;      // Network socket transmission errors
long recons   = target.ReconnectCount;    // Socket reconnection attempts
```

### Backward Compatibility with `NLogStatsFetcher`

In existing consumer codebases (such as Herma) where reflection was used to read internal fields (`_openNetworkSenders` and `_items`), internal shim properties are retained so legacy reflection continues to return queue depth without throwing `NullReferenceException` or `MissingFieldException`.

---

## Building and Testing

```bash
# Build solution across all targets
dotnet build NLog.FastNetworkTarget.sln -c Release

# Run tests on .NET 8
dotnet test NLog.FastNetworkTarget.Tests/NLog.FastNetworkTarget.Tests.csproj -c Release -f net8.0

# Run tests under Mono (.NET Framework 4.6.2)
dotnet build NLog.FastNetworkTarget.Tests/NLog.FastNetworkTarget.Tests.csproj -c Release -f net462
mono NLog.FastNetworkTarget.Tests/bin/Release/net462/NLog.FastNetworkTarget.Tests.exe

# Run benchmark suite
./NLog.FastNetworkTarget.Benchmarks/bin/Release/net8.0/NLog.FastNetworkTarget.Benchmarks --iterations 3
mono NLog.FastNetworkTarget.Benchmarks/bin/Release/net462/NLog.FastNetworkTarget.Benchmarks.exe --iterations 3
```
