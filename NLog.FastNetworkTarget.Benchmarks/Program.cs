using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Collections.Generic;

using NLog;
using NLog.Config;
using NLog.Common;
using NLog.Layouts;
using NLog.Targets;
using NLog.Targets.Wrappers;

namespace NLog.FastNetworkTarget.Benchmarks
{
	public sealed class Scenario
	{
		public string Name { get; set; }
		public Func<MockTcpReceiver, BenchmarkResult> Runner { get; set; }

		public Scenario(string name, Func<MockTcpReceiver, BenchmarkResult> runner)
		{
			Name = name;
			Runner = runner;
		}
	}

	public sealed class TargetContext : IDisposable
	{
		public NLog.Targets.FastNetworkTarget Target { get; set; }
		public LogFactory Factory { get; set; }

		public void Dispose()
		{
			Factory?.Dispose();
		}
	}

	public static class Program
	{
		private const int WARMUP_COUNT = 50_000;

		public static int Main(string[] args)
		{
			int iterations = 3;
			string mdPath = null;

			for (int i = 0; i < args.Length; i++)
			{
				if (args[i] == "--iterations" && i + 1 < args.Length)
					int.TryParse(args[++i], out iterations);
				else if (args[i] == "--markdown" && i + 1 < args.Length)
					mdPath = args[++i];
			}

			PrintEnvironmentInfo();

			var scenarios = new List<Scenario>
			{
				new Scenario("SingleThread_Direct", receiver => RunDirect(receiver, threads: 1, totalMessages: 50_000)),
				new Scenario("Concurrent8_Direct", receiver => RunDirect(receiver, threads: 8, totalMessages: 100_000)),
				new Scenario("Concurrent16_Direct", receiver => RunDirect(receiver, threads: 16, totalMessages: 100_000)),
				new Scenario("Concurrent32_Direct", receiver => RunDirect(receiver, threads: 32, totalMessages: 100_000)),
				new Scenario("Batch1000_Direct", receiver => RunBatchDirect(receiver, totalMessages: 100_000, batchSize: 1000)),
				new Scenario("HermaE2E_FullPipeline", receiver => RunHermaPipeline(receiver, threads: 16, totalMessages: 50_000)),
				new Scenario("LargePayload_32KB", receiver => RunLargePayload(receiver, totalMessages: 2_500, payloadSizeKb: 32))
			};

			// Warmup
			Console.WriteLine("Running warmup...");
			using (var warmupReceiver = new MockTcpReceiver())
			{
				RunDirect(warmupReceiver, threads: 1, totalMessages: WARMUP_COUNT);
			}
			GC.Collect();
			GC.WaitForPendingFinalizers();
			GC.Collect();
			Console.WriteLine("Warmup complete.\n");

			var medianResults = new List<BenchmarkResult>();

			Console.WriteLine("----------------------------------------------------------------------------------------------------------------------------------");
			Console.WriteLine(string.Format("{0,-25} | {1,18} | {2,13} | {3,19} | {4,12} | {5,14} | Verified",
				"Scenario", "Throughput", "Data Rate", "CPU Efficiency", "GC (0/1/2)", "Allocations"));
			Console.WriteLine("----------------------------------------------------------------------------------------------------------------------------------");

			foreach (var scenario in scenarios)
			{
				var runResults = new List<BenchmarkResult>();

				for (int iter = 1; iter <= iterations; iter++)
				{
					GC.Collect();
					GC.WaitForPendingFinalizers();
					GC.Collect();
					Thread.Sleep(50);

					using (var receiver = new MockTcpReceiver())
					{
						var result = scenario.Runner(receiver);
						result.Received = receiver.TotalLinesReceived;
						result.Verified &= result.Missing == 0;
						runResults.Add(result);
						Console.Write($"[{iter}: {result.MessagesPerSec:N0} msg/s] ");
					}
				}
				Console.WriteLine();

				// Pick median by MessagesPerSec
				var median = runResults.OrderBy(r => r.MessagesPerSec).ElementAt(runResults.Count / 2);
				median.Scenario = scenario.Name;
				foreach (var failed in runResults.Where(r => !r.Verified && r != median))
				{
					failed.Scenario = scenario.Name + " (failed iteration)";
					medianResults.Add(failed);
				}
				medianResults.Add(median);

				Console.WriteLine(median.ToString());
			}

			Console.WriteLine("----------------------------------------------------------------------------------------------------------------------------------");

			if (!string.IsNullOrEmpty(mdPath))
			{
				string title = Path.GetFileNameWithoutExtension(mdPath);
				WriteMarkdownReport(mdPath, medianResults, title);
				Console.WriteLine($"\nMarkdown report saved to: {mdPath}");
			}

			// Historical v1 starts foreground sender threads which self-join on close.
			Environment.Exit(medianResults.All(r => r.Verified) ? 0 : 1);
			return Environment.ExitCode;
		}

		private static void PrintEnvironmentInfo()
		{
			Console.WriteLine("==========================================================================================");
			Console.WriteLine(" NLog.FastNetworkTarget Performance Benchmark Harness");
			Console.WriteLine("==========================================================================================");
			Console.WriteLine($"Date:           {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
			Console.WriteLine($"OS:             {Environment.OSVersion} ({(IntPtr.Size == 8 ? "64-bit" : "32-bit")})");
			Console.WriteLine($"Runtime:        {GetRuntimeDescription()}");
			Console.WriteLine($"Processors:     {Environment.ProcessorCount} logical cores");
			Console.WriteLine($"NLog Version:   {typeof(LogEventInfo).Assembly.GetName().Version}");
			Console.WriteLine("==========================================================================================\n");
		}

		private static string GetRuntimeDescription()
		{
#if NET8_0_OR_GREATER
			return System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;
#else
			var monoRuntime = Type.GetType("Mono.Runtime");
			if (monoRuntime != null)
			{
				var displayName = monoRuntime.GetMethod("GetDisplayName", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
				return $"Mono {displayName?.Invoke(null, null)} (.NET Framework 4.6.2)";
			}
			return $".NET Framework {Environment.Version}";
#endif
		}

		private static JsonLayout CreateHermaJsonLayout()
		{
			var layout = new JsonLayout
			{
				IncludeAllProperties = true,
				ExcludeEmptyProperties = true,
				IncludeGdc = true,
				IncludeMdc = true,
				IncludeMdlc = true,
				RenderEmptyObject = false,
				EscapeForwardSlash = false
			};

			layout.Attributes.Add(new JsonAttribute("@timestamp", "${date:format=yyyy-MM-ddTHH\\:mm\\:ss.fffZ}"));
			layout.Attributes.Add(new JsonAttribute("hostname", "${hostname}"));
			layout.Attributes.Add(new JsonAttribute("threadid", "${threadid}"));
			layout.Attributes.Add(new JsonAttribute("threadname", "${threadname}"));
			layout.Attributes.Add(new JsonAttribute("level", "${level:upperCase=true}"));
			layout.Attributes.Add(new JsonAttribute("logger", "${logger}"));
			layout.Attributes.Add(new JsonAttribute("message", "${message}"));
			layout.Attributes.Add(new JsonAttribute("scope", "${when:when=length('${ndc}')>0:inner=${ndc}:else=${ndlc}}"));
			layout.Attributes.Add(new JsonAttribute("exception", "${exception:format=@}"));
			layout.Attributes.Add(new JsonAttribute("RunningTime", "${gdc:item=RunningTime:format=f}"));

			return layout;
		}

		private static TargetContext CreateTarget(MockTcpReceiver receiver, Layout layout = null)
		{
			var target = new NLog.Targets.FastNetworkTarget("LogStashInner")
			{
				Address = receiver.Address,
				NewLine = true,
				LineEnding = LineEndingMode.LF,
				MaxQueueSize = 126976,
				Layout = layout ?? CreateHermaJsonLayout()
			};

			var config = new LoggingConfiguration();
			config.AddTarget("LogStashInner", target);
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, target));
			var factory = new LogFactory(config);

			// Prime lazy TCP connection before timed measurement
			var primeMre = new ManualResetEvent(false);
			target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Info, "warmup", "prime"), ex => primeMre.Set()));
			primeMre.WaitOne(3000);
			receiver.WaitForMessages(1, TimeSpan.FromSeconds(3));
			receiver.Reset();

			return new TargetContext { Target = target, Factory = factory };
		}

		private static LogEventInfo CreateLogEvent(int index)
		{
			var logEvent = new LogEventInfo(LogLevel.Info, "Herma.Daemon.MessageProcessor", $"Processed Rebus message ID {index:D8} in 42.5ms; status=Success; recipient=user_{index}@evicertia.com");
			logEvent.Properties["MessageId"] = Guid.NewGuid().ToString("N");
			logEvent.Properties["TenantId"] = "Evicertia-EU-Prod";
			logEvent.Properties["BatchNumber"] = index / 1000;
			return logEvent;
		}

		private static BenchmarkResult RunDirect(MockTcpReceiver receiver, int threads, int totalMessages)
		{
			using (var ctx = CreateTarget(receiver))
			{
				var target = ctx.Target;
				var messagesPerThread = totalMessages / threads;

				GlobalDiagnosticsContext.Set("RunningTime", "0001.2s");
				MappedDiagnosticsContext.Set("CorrelationId", "corr-99281-xyz");

				var ready = new CountdownEvent(threads);
				var start = new ManualResetEventSlim();
				var process = Process.GetCurrentProcess();

				var workerThreads = new Thread[threads];
				for (int t = 0; t < threads; t++)
				{
					int threadIdx = t;
					workerThreads[t] = new Thread(() =>
					{
						var logEvents = new LogEventInfo[messagesPerThread];
						for (int i = 0; i < messagesPerThread; i++)
							logEvents[i] = CreateLogEvent(threadIdx * messagesPerThread + i);

						ready.Signal();
						start.Wait();

						for (int i = 0; i < messagesPerThread; i++)
						{
							target.WriteAsyncLogEvent(new AsyncLogEventInfo(logEvents[i], ex => { }));
						}
					})
					{
						IsBackground = true,
						Name = $"BenchmarkWorker-{t}"
					};
					workerThreads[t].Start();
				}

				ready.Wait();
				long startAlloc = GetAllocatedBytes();
				int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
				var startCpu = process.TotalProcessorTime;
				var sw = Stopwatch.StartNew();
				start.Set();

				for (int t = 0; t < threads; t++)
					workerThreads[t].Join();

				// Flush target
				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				flushMre.WaitOne(10_000);

				bool received = receiver.WaitForMessages(totalMessages, TimeSpan.FromSeconds(15));
				sw.Stop();
				var endCpu = process.TotalProcessorTime;
				long endAlloc = GetAllocatedBytes();

				return new BenchmarkResult
				{
					MessageCount = totalMessages,
					TotalBytes = receiver.TotalBytesReceived,
					ElapsedMs = sw.Elapsed.TotalMilliseconds,
					CpuTimeMs = (endCpu - startCpu).TotalMilliseconds,
					Gen0 = GC.CollectionCount(0) - g0,
					Gen1 = GC.CollectionCount(1) - g1,
					Gen2 = GC.CollectionCount(2) - g2,
					AllocatedBytes = Math.Max(0, endAlloc - startAlloc),
					Verified = received && receiver.TotalLinesReceived >= totalMessages
				};
			}
		}

		private static BenchmarkResult RunBatchDirect(MockTcpReceiver receiver, int totalMessages, int batchSize)
		{
			using (var ctx = CreateTarget(receiver))
			{
				var target = ctx.Target;
				int batchCount = totalMessages / batchSize;

				GlobalDiagnosticsContext.Set("RunningTime", "0001.2s");
				MappedDiagnosticsContext.Set("CorrelationId", "corr-batch-xyz");

				var batches = new AsyncLogEventInfo[batchCount][];
				for (int b = 0; b < batchCount; b++)
				{
					batches[b] = new AsyncLogEventInfo[batchSize];
					for (int i = 0; i < batchSize; i++)
					{
						var ev = CreateLogEvent(b * batchSize + i);
						batches[b][i] = new AsyncLogEventInfo(ev, ex => { });
					}
				}

				var process = Process.GetCurrentProcess();
				long startAlloc = GetAllocatedBytes();
				int g0 = GC.CollectionCount(0);
				int g1 = GC.CollectionCount(1);
				int g2 = GC.CollectionCount(2);
				var startCpu = process.TotalProcessorTime;
				var sw = Stopwatch.StartNew();

				for (int b = 0; b < batchCount; b++)
				{
					target.WriteAsyncLogEvents(batches[b]);
				}

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				flushMre.WaitOne(10_000);

				bool received = receiver.WaitForMessages(totalMessages, TimeSpan.FromSeconds(15));
				sw.Stop();
				var endCpu = process.TotalProcessorTime;
				long endAlloc = GetAllocatedBytes();

				return new BenchmarkResult
				{
					MessageCount = totalMessages,
					TotalBytes = receiver.TotalBytesReceived,
					ElapsedMs = sw.Elapsed.TotalMilliseconds,
					CpuTimeMs = (endCpu - startCpu).TotalMilliseconds,
					Gen0 = GC.CollectionCount(0) - g0,
					Gen1 = GC.CollectionCount(1) - g1,
					Gen2 = GC.CollectionCount(2) - g2,
					AllocatedBytes = Math.Max(0, endAlloc - startAlloc),
					Verified = received && receiver.TotalLinesReceived >= totalMessages
				};
			}
		}

		private static BenchmarkResult RunHermaPipeline(MockTcpReceiver receiver, int threads, int totalMessages)
		{
			// Configure NLog exactly as Herma does
			var config = new LoggingConfiguration();

			var innerTarget = new NLog.Targets.FastNetworkTarget("LogStashInner")
			{
				Address = receiver.Address,
				NewLine = true,
				LineEnding = LineEndingMode.LF,
				MaxQueueSize = 126976,
				Layout = CreateHermaJsonLayout()
			};

			var filterWrapper = new FilteringTargetWrapper("LogStashFilter", innerTarget, "length('http://test')>0");

			var asyncWrapper = new AsyncTargetWrapper("LogStash", filterWrapper)
			{
				BatchSize = 1000,
				TimeToSleepBetweenBatches = 0,
				OverflowAction = AsyncTargetWrapperOverflowAction.Discard,
				QueueLimit = 126976
			};

			config.AddTarget("LogStash", asyncWrapper);
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Info, asyncWrapper));

			var logFactory = new LogFactory(config);
			var logger = logFactory.GetLogger("Herma.Daemon.ServiceRunner");

			GlobalDiagnosticsContext.Set("RunningTime", "0042.0s");

			// Prime lazy TCP connection before timed measurement
			logger.Info("Warmup connection prime");
			logFactory.Flush(TimeSpan.FromSeconds(5));
			receiver.WaitForMessages(1, TimeSpan.FromSeconds(3));
			receiver.Reset();

			var messagesPerThread = totalMessages / threads;
			var barrier = new Barrier(threads + 1);
			var process = Process.GetCurrentProcess();

			long startAlloc = GetAllocatedBytes();
			int g0 = GC.CollectionCount(0);
			int g1 = GC.CollectionCount(1);
			int g2 = GC.CollectionCount(2);
			var startCpu = process.TotalProcessorTime;
			var sw = new Stopwatch();

			var workerThreads = new Thread[threads];
			for (int t = 0; t < threads; t++)
			{
				int threadIdx = t;
				workerThreads[t] = new Thread(() =>
				{
					barrier.SignalAndWait();

					for (int i = 0; i < messagesPerThread; i++)
					{
						logger.Info("Dispatched message {0} to recipient {1} with execution time {2}ms", threadIdx * messagesPerThread + i, "recipient@namirial.com", 12.3);
					}
				})
				{
					IsBackground = true,
					Name = $"HermaPipelineWorker-{t}"
				};
				workerThreads[t].Start();
			}

			sw.Start();
			barrier.SignalAndWait();

			for (int t = 0; t < threads; t++)
				workerThreads[t].Join();

			// Flush entire NLog factory
			logFactory.Flush(TimeSpan.FromSeconds(15));
			bool received = receiver.WaitForMessages(totalMessages, TimeSpan.FromSeconds(20));
			sw.Stop();

			var endCpu = process.TotalProcessorTime;
			long endAlloc = GetAllocatedBytes();

			logFactory.Dispose();

			return new BenchmarkResult
			{
				MessageCount = totalMessages,
				TotalBytes = receiver.TotalBytesReceived,
				ElapsedMs = sw.Elapsed.TotalMilliseconds,
				CpuTimeMs = (endCpu - startCpu).TotalMilliseconds,
				Gen0 = GC.CollectionCount(0) - g0,
				Gen1 = GC.CollectionCount(1) - g1,
				Gen2 = GC.CollectionCount(2) - g2,
				AllocatedBytes = Math.Max(0, endAlloc - startAlloc),
				Verified = received && receiver.TotalLinesReceived >= totalMessages
			};
		}

		private static BenchmarkResult RunLargePayload(MockTcpReceiver receiver, int totalMessages, int payloadSizeKb)
		{
			using (var ctx = CreateTarget(receiver, new SimpleLayout("${message}")))
			{
				var target = ctx.Target;
				var largeString = new string('X', payloadSizeKb * 1024 - 1);

				var process = Process.GetCurrentProcess();
				long startAlloc = GetAllocatedBytes();
				int g0 = GC.CollectionCount(0);
				int g1 = GC.CollectionCount(1);
				int g2 = GC.CollectionCount(2);
				var startCpu = process.TotalProcessorTime;
				var sw = Stopwatch.StartNew();

				for (int i = 0; i < totalMessages; i++)
				{
					var ev = new LogEventInfo(LogLevel.Error, "Herma.Daemon.LargeFault", largeString);
					target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => { }));
				}

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				flushMre.WaitOne(15_000);

				bool received = receiver.WaitForMessages(totalMessages, TimeSpan.FromSeconds(25));
				sw.Stop();
				var endCpu = process.TotalProcessorTime;
				long endAlloc = GetAllocatedBytes();

				return new BenchmarkResult
				{
					MessageCount = totalMessages,
					TotalBytes = receiver.TotalBytesReceived,
					ElapsedMs = sw.Elapsed.TotalMilliseconds,
					CpuTimeMs = (endCpu - startCpu).TotalMilliseconds,
					Gen0 = GC.CollectionCount(0) - g0,
					Gen1 = GC.CollectionCount(1) - g1,
					Gen2 = GC.CollectionCount(2) - g2,
					AllocatedBytes = Math.Max(0, endAlloc - startAlloc),
					Verified = received && receiver.TotalLinesReceived >= totalMessages
				};
			}
		}

#if !NET8_0_OR_GREATER
		private static readonly bool _appDomainMonitoringSupported = TryEnableAppDomainMonitoring();

		private static bool TryEnableAppDomainMonitoring()
		{
			try
			{
				AppDomain.MonitoringIsEnabled = true;
				return AppDomain.MonitoringIsEnabled;
			}
			catch
			{
				return false;
			}
		}
#endif

		private static long GetAllocatedBytes()
		{
#if NET8_0_OR_GREATER
			return GC.GetTotalAllocatedBytes(precise: true);
#else
			if (_appDomainMonitoringSupported)
			{
				long total = AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize;
				if (total > 0)
					return total;
			}
			return GC.GetTotalMemory(forceFullCollection: false);
#endif
		}

		private static void WriteMarkdownReport(string path, List<BenchmarkResult> results, string reportTitle = "v2 Performance Report")
		{
			var dir = Path.GetDirectoryName(path);
			if (!string.IsNullOrEmpty(dir))
				Directory.CreateDirectory(dir);

			var sb = new StringBuilder();
			sb.AppendLine($"# {reportTitle} ({GetRuntimeDescription()})");
			sb.AppendLine();
			sb.AppendLine($"- **Date**: `{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC`");
			sb.AppendLine($"- **OS**: `{Environment.OSVersion}` ({(IntPtr.Size == 8 ? "64-bit" : "32-bit")})");
			sb.AppendLine($"- **Runtime**: `{GetRuntimeDescription()}`");
			sb.AppendLine($"- **Processors**: `{Environment.ProcessorCount} logical cores`");
			sb.AppendLine($"- **NLog Version**: `{typeof(LogEventInfo).Assembly.GetName().Version}`");
			sb.AppendLine();
			sb.AppendLine("| Scenario | Attempted/received/missing | Delivered/s | MiB/s | CPU ms/100k delivered | GC (0/1/2) | B/msg* | Verified |");
			sb.AppendLine("| :--- | ---: | ---: | ---: | ---: | :---: | ---: | :---: |");

			foreach (var r in results)
				sb.AppendLine(r.ToMarkdownRow());

			sb.AppendLine("\n* Mono uses live heap delta when cumulative allocation accounting is unavailable; not comparable to .NET allocation counts.");
			File.WriteAllText(path, sb.ToString());
		}
	}
}
