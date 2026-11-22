# NLog.FastNetworkTarget

A high-throughput, low-CPU network target for NLog designed for massive-volume TCP workloads (Vector, Logstash, Fluentd, etc.).

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

In a matched 16-producer Herma-style loopback benchmark, all revisions delivered 50,000 of 50,000 records. Median throughput increased from 322k to 688k records/s on .NET 8 and from 157k to 375k on Mono. Process CPU per delivered record fell by 42% and 23% respectively versus v1.

Full benchmark details and methodology are documented in [`benchmarks/results/COMPARISON-v1-vs-v2.md`](benchmarks/results/COMPARISON-v1-vs-v2.md).

---

## Architectural Highlights

1. **Lock-Free Concurrency**:
   - Overrides `WriteAsyncThreadSafe` to bypass NLog's base `Monitor.Enter(SyncRoot)` serialization.
   - Cache-line-padded bounded MPSC (Multi-Producer Single-Consumer) ring buffer for wait-free dequeuing and lock-free enqueuing.
2. **Producer-Side 64 KB Slab Coalescing**:
   - Batches from `AsyncTargetWrapper` are packed directly into 64 KB pooled slabs (with dynamic per-event `Address` routing and `MaxMessageSize` enforcement), turning 1,000 log events into ~8 socket writes instead of 1,000 syscalls.
3. **Zero-Copy Layout Encoding**:
   - Bypasses `StringBuilder` and `char[]` copying when pre-rendered layout values are available on `LogEventInfo` (such as `JsonLayout` inside `AsyncTargetWrapper`), even when `newLine="true"`.
   - Uses SIMD `ReadOnlySpan<char>` / `StringBuilder.GetChunks()` encoding on `.NET 8+` and bounded streaming `Encoder` slices on `net462` / `netstandard2.0`.
4. **Zero-LOH Byte Buffer Pooling**:
   - `ByteBufferPool` provides TLS-cached, non-blocking bucketed buffers (`1 KB`, `4 KB`, `16 KB`, `32 KB`, `64 KB` + `ArrayPool` / shared large buckets) eliminating recurring Large Object Heap (`>= 85 KB`) Gen2 GC pauses.
5. **Newline-Safe & UTF-8-Safe `Split`**:
   - An oversized record is admitted or discarded as one event. By default, splitting limits socket writes without altering the byte stream: a JSON record still has one final delimiter. For plain-text fragment consumers only, `newLinePerSplitChunk="true"` inserts delimiters between fragments; these are not standalone JSON documents.
6. **Configurable Queue Overflow & Non-Blocking Safety**:
   - Configurable `onQueueOverflow="Discard"` (default) or `Block` so high-volume bursts never wedge application threads.
7. **Socket & TCP Hardening**:
   - Lazy socket connection (no idle reconnect storms before the first event is logged), full IPv4/IPv6 `tcp://` auto-detection, `noDelay="true"` (`TCP_NODELAY`) by default, `sendTimeout="30000"` (30s default) to recover from frozen TCP ZeroWindow states, and accurate macOS Darwin vs Linux TCP keep-alive socket option detection.

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
            maxMessageSize="262144"
            maxQueueSize="8192"
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
| `newLinePerSplitChunk` | `bool` | `false` | With `newLine`, delimit text fragments individually instead of preserving one record. |
| `lineEnding` | `LineEndingMode` | `CRLF` | `LF`, `CRLF`, `CR`, or `Null`. |
| `maxMessageSize` | `int` | `262144` (256 KB) | Maximum message size in bytes before triggering `onOverflow`. |
| `onOverflow` | `enum` | `Split` | `Split` (UTF-8 & newline-safe), `Discard`, or `Error`. |
| `maxQueueSize` | `int` | `8192` | Maximum items held in the sender ring buffer (rounded up to power of 2, max 1,048,576). |
| `onQueueOverflow` | `enum` | `Discard` | `Discard` (drops newest item and tracks metrics without blocking caller threads) or `Block` (blocks producer until queue has space). |
| `batchSize` | `int` | `512` | Maximum queue items coalesced per socket write. |
| `maxBatchBytes` | `int` | `262144` (256 KB) | Maximum bytes sent per contiguous socket write. |
| `noDelay` | `bool` | `true` | Enables `TCP_NODELAY` to prevent 40ms delayed-ACK latency spikes. |
| `sendTimeout` | `int` | `30000` (30s) | Socket send timeout in milliseconds (prevents permanent ZeroWindow stalls). |
| `sendBufferSize` | `int` | `262144` (256 KB) | Underlying OS socket send buffer size. |
| `keepConnection` | `bool` | `true` | Keep connection open between writes (cached up to `connectionCacheSize` endpoints with LRU eviction). |
| `connectionCacheSize` | `int` | `5` | Maximum cached open connections when using dynamic `address` layouts. |
| `maxConnections` | `int` | `0` | Maximum concurrent transient connections when `keepConnection="false"` (`0` = unlimited). |
| `onConnectionOverflow` | `enum` | `AllowNewConnnection` | `AllowNewConnnection`, `DiscardMessage`, or `Block` when `keepConnection="false"`. |
| `connectionTimeout` | `int` | `1000` ms | Connect timeout in milliseconds. |
| `keepAliveTime` | `int` | `0` | TCP keep-alive idle time before probing (in ms, `0` = disabled). |

---

## Observability & Metrics API

The target exposes lock-free monotonic metric properties (preserved across LRU connection evictions and shutdown):

```csharp
var target = (FastNetworkTarget)LogManager.Configuration.FindTargetByName("LogStash");

long pending  = target.PendingMessages;   // Current queue depth
int  conns    = target.ConnectionCount;   // Number of active network connections
long sentMsgs = target.SentMessages;      // Total logical log messages sent
long sentBytes= target.SentBytes;         // Total bytes transferred
long batches  = target.SentBatches;       // Total socket send syscalls issued
long dropped  = target.DroppedMessages;   // Logical messages discarded due to queue/message/connection overflow
long blocked  = target.BlockedEnqueues;   // Number of times producers waited under onQueueOverflow="Block"
long failures = target.SendFailures;      // Network socket transmission/connect errors
long recons   = target.ReconnectCount;    // Socket connection/reconnection count
```

### Backward Compatibility with `NLogStatsFetcher`

In existing consumer codebases (such as Herma) where reflection was used to read internal fields (`_openNetworkSenders` and `_items`), internal shim collections are retained so legacy reflection continues to return queue depth without throwing `NullReferenceException` or `MissingFieldException`.

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
