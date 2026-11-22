using System;

namespace NLog.FastNetworkTarget.Benchmarks
{
	public sealed class BenchmarkResult
	{
		public string Scenario { get; set; }
		public int MessageCount { get; set; }
		public long Received { get; set; }
		public long Missing => MessageCount - Received;
		public long TotalBytes { get; set; }
		public double ElapsedMs { get; set; }
		public double MessagesPerSec => Verified ? Received * 1000.0 / ElapsedMs : double.NaN;
		public double MegabytesPerSec => ElapsedMs > 0 ? ((TotalBytes / (1024.0 * 1024.0)) / (ElapsedMs / 1000.0)) : 0;
		public double CpuTimeMs { get; set; }
		public double CpuMsPer100k => Verified ? CpuTimeMs / Received * 100_000.0 : double.NaN;
		public int Gen0 { get; set; }
		public int Gen1 { get; set; }
		public int Gen2 { get; set; }
		public long AllocatedBytes { get; set; }
		public double BytesPerMsg => MessageCount > 0 ? ((double)AllocatedBytes / MessageCount) : 0;
		public bool Verified { get; set; }

		public override string ToString()
		{
			return $"{Scenario,-25} | {MessagesPerSec,11:N0} msg/s | {MegabytesPerSec,8:N1} MB/s | CPU: {CpuMsPer100k,7:N1} ms/100k | GC: {Gen0}/{Gen1}/{Gen2} | Alloc: {BytesPerMsg,7:N0} B/msg | Verified: {Verified}";
		}

		public string ToMarkdownRow()
		{
			return $"| `{Scenario}` | {MessageCount}/{Received}/{Missing} | {MessagesPerSec:N0} | {MegabytesPerSec:N1} | {CpuMsPer100k:N1} | {Gen0} / {Gen1} / {Gen2} | {BytesPerMsg:N0} | {(Verified ? "PASS" : "FAIL")} |";
		}
	}
}
