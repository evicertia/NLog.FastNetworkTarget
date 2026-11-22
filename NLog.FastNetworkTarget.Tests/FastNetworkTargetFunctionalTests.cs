using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;

using NUnit.Framework;
using NLog;
using NLog.Config;
using NLog.Common;
using NLog.Layouts;
using NLog.Targets;

namespace NLog.FastNetworkTarget
{
	[TestFixture]
	public class FastNetworkTargetFunctionalTests
	{
		public sealed class TargetContext : IDisposable
		{
			public NLog.Targets.FastNetworkTarget Target { get; set; }
			public LogFactory Factory { get; set; }
			public void Dispose() => Factory?.Dispose();
		}

		private static TargetContext CreateTarget(string address, Action<NLog.Targets.FastNetworkTarget> configure = null)
		{
			var target = new NLog.Targets.FastNetworkTarget("TestNetwork")
			{
				Address = address,
				NewLine = true,
				LineEnding = LineEndingMode.LF,
				MaxQueueSize = 10000,
				Layout = new SimpleLayout("${message}")
			};

			configure?.Invoke(target);

			var config = new LoggingConfiguration();
			config.AddTarget("TestNetwork", target);
			config.LoggingRules.Add(new LoggingRule("*", LogLevel.Trace, target));
			var factory = new LogFactory(config);

			return new TargetContext { Target = target, Factory = factory };
		}

		[Test]
		public void SingleEvent_SendAndReceive()
		{
			using (var receiver = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget(receiver.Address))
			{
				var target = ctx.Target;
				var mre = new ManualResetEvent(false);
				Exception sendEx = null;

				var ev = new LogEventInfo(LogLevel.Info, "test", "Hello World 123");
				target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex =>
				{
					sendEx = ex;
					mre.Set();
				}));

				Assert.That(mre.WaitOne(3000), Is.True, "Continuation was not called within 3s");
				Assert.That(sendEx, Is.Null);

				Assert.That(receiver.WaitForMessages(1, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(receiver.ReceivedLines.TryDequeue(out var line), Is.True);
				Assert.That(line, Is.EqualTo("Hello World 123"));
			}
		}

		[Test]
		public void ConcurrentWriters_MaintainsPerThreadOrderAndIntegrity()
		{
			int threads = 8;
			int messagesPerThread = 2000;
			int totalMessages = threads * messagesPerThread;

			using (var receiver = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget(receiver.Address))
			{
				var target = ctx.Target;
				var barrier = new Barrier(threads + 1);
				var workerThreads = new Thread[threads];

				for (int t = 0; t < threads; t++)
				{
					int threadId = t;
					workerThreads[t] = new Thread(() =>
					{
						barrier.SignalAndWait();

						for (int seq = 0; seq < messagesPerThread; seq++)
						{
							var msg = $"T{threadId:D2}:{seq:D6}:PayloadData";
							var ev = new LogEventInfo(LogLevel.Info, "test", msg);
							target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => { }));
						}
					})
					{
						IsBackground = true
					};
					workerThreads[t].Start();
				}

				barrier.SignalAndWait();

				for (int t = 0; t < threads; t++)
					workerThreads[t].Join();

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				Assert.That(flushMre.WaitOne(5000), Is.True);

				Assert.That(receiver.WaitForMessages(totalMessages, TimeSpan.FromSeconds(10)), Is.True,
					$"Expected {totalMessages} messages, but got {receiver.TotalLinesReceived}");

				// Validate per-thread ordering and no corruption
				var lastSeqPerThread = new int[threads];
				for (int i = 0; i < threads; i++)
					lastSeqPerThread[i] = -1;

				int validatedCount = 0;
				while (receiver.ReceivedLines.TryDequeue(out var line))
				{
					validatedCount++;
					var parts = line.Split(':');
					Assert.That(parts.Length, Is.EqualTo(3), $"Corrupted line format: '{line}'");
					Assert.That(parts[0].StartsWith("T"), Is.True);

					int threadId = int.Parse(parts[0].Substring(1));
					int seq = int.Parse(parts[1]);

					Assert.That(seq, Is.EqualTo(lastSeqPerThread[threadId] + 1),
						$"Out of order sequence for thread {threadId}: expected {lastSeqPerThread[threadId] + 1}, got {seq}");

					lastSeqPerThread[threadId] = seq;
				}

				Assert.That(validatedCount, Is.EqualTo(totalMessages));
				for (int t = 0; t < threads; t++)
					Assert.That(lastSeqPerThread[t], Is.EqualTo(messagesPerThread - 1));
			}
		}

		[Test]
		public void BatchSend_PreservesFifoOrder()
		{
			using (var receiver = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget(receiver.Address))
			{
				var target = ctx.Target;
				int count = 500;
				var batch = new AsyncLogEventInfo[count];
				var continuationsFired = 0;

				for (int i = 0; i < count; i++)
				{
					var ev = new LogEventInfo(LogLevel.Info, "test", $"BatchItem_{i:D4}");
					batch[i] = new AsyncLogEventInfo(ev, ex =>
					{
						Interlocked.Increment(ref continuationsFired);
					});
				}

				target.WriteAsyncLogEvents(batch);

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				Assert.That(flushMre.WaitOne(5000), Is.True);

				Assert.That(receiver.WaitForMessages(count, TimeSpan.FromSeconds(5)), Is.True);

				for (int i = 0; i < count; i++)
				{
					Assert.That(receiver.ReceivedLines.TryDequeue(out var line), Is.True);
					Assert.That(line, Is.EqualTo($"BatchItem_{i:D4}"));
				}

				Assert.That(continuationsFired, Is.EqualTo(count));
			}
		}

		[Test]
		public void MaxMessageSize_Split_SplitsAcrossChunks()
		{
			using (var receiver = new TestTcpReceiver(storeLines: false))
			using (var ctx = CreateTarget(receiver.Address, t =>
			{
				t.MaxMessageSize = 50;
				t.OnOverflow = NetworkTargetOverflowAction.Split;
				t.NewLine = false;
			}))
			{
				var target = ctx.Target;
				var payload = new string('A', 120);
				var ev = new LogEventInfo(LogLevel.Info, "test", payload);
				var mre = new ManualResetEvent(false);
				Exception sendEx = null;

				target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex =>
				{
					sendEx = ex;
					mre.Set();
				}));

				Assert.That(mre.WaitOne(3000), Is.True);
				Assert.That(sendEx, Is.Null);

				Assert.That(receiver.WaitForBytes(120, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(receiver.TotalBytesReceived, Is.EqualTo(120));
			}
		}

		[Test]
		public void MaxMessageSize_Discard_DropsMessage()
		{
			using (var receiver = new TestTcpReceiver(storeLines: false))
			using (var ctx = CreateTarget(receiver.Address, t =>
			{
				t.MaxMessageSize = 50;
				t.OnOverflow = NetworkTargetOverflowAction.Discard;
				t.NewLine = false;
			}))
			{
				var target = ctx.Target;
				var payload = new string('B', 120);
				var ev = new LogEventInfo(LogLevel.Info, "test", payload);
				var mre = new ManualResetEvent(false);
				Exception sendEx = null;

				target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex =>
				{
					sendEx = ex;
					mre.Set();
				}));

				Assert.That(mre.WaitOne(3000), Is.True);
				Assert.That(sendEx, Is.Null);

				// Small wait; nothing should be sent
				Thread.Sleep(100);
				Assert.That(receiver.TotalBytesReceived, Is.EqualTo(0));
			}
		}

		[Test]
		public void MaxMessageSize_Error_FailsContinuationWithOverflowException()
		{
			using (var receiver = new TestTcpReceiver(storeLines: false))
			using (var ctx = CreateTarget(receiver.Address, t =>
			{
				t.MaxMessageSize = 50;
				t.OnOverflow = NetworkTargetOverflowAction.Error;
				t.NewLine = false;
			}))
			{
				var target = ctx.Target;
				var payload = new string('C', 120);
				var ev = new LogEventInfo(LogLevel.Info, "test", payload);
				var mre = new ManualResetEvent(false);
				Exception sendEx = null;

				target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex =>
				{
					sendEx = ex;
					mre.Set();
				}));

				Assert.That(mre.WaitOne(3000), Is.True);
				Assert.That(sendEx, Is.Not.Null);
				Assert.That(sendEx, Is.InstanceOf<OverflowException>());
			}
		}

		[Test]
		public void CloseTarget_DoesNotDeadlock()
		{
			using (var receiver = new TestTcpReceiver())
			{
				var ctx = CreateTarget(receiver.Address);

				var ev = new LogEventInfo(LogLevel.Info, "test", "Pre-close message");
				var mre = new ManualResetEvent(false);
				ctx.Target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => mre.Set()));
				mre.WaitOne(3000);

				var closeThread = new Thread(() => ctx.Dispose());
				closeThread.Start();

				bool closedCleanly = closeThread.Join(5000);
				Assert.That(closedCleanly, Is.True, "ctx.Dispose() deadlocked or timed out");
			}
		}

		[Test]
		public void LegacyNLogStatsFetcher_ReflectionCompatibility()
		{
			using (var receiver = new TestTcpReceiver())
			using (var ctx = CreateTarget(receiver.Address))
			{
				var target = ctx.Target;

				// Simulate Herma's NLogStatsFetcher reflection:
				// target.GetType().GetField("_openNetworkSenders", BindingFlags.NonPublic | BindingFlags.Instance)
				var sendersField = target.GetType().GetField("_openNetworkSenders", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
				Assert.That(sendersField, Is.Not.Null, "Expected _openNetworkSenders field to exist for backward compatibility");

				var senders = sendersField.GetValue(target) as System.Collections.ICollection;
				Assert.That(senders, Is.Not.Null);

				// Send an event so connection is opened
				var mre = new ManualResetEvent(false);
				target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Info, "test", "Hello"), ex => mre.Set()));
				mre.WaitOne(3000);

				var sender = System.Linq.Enumerable.Cast<object>(senders).FirstOrDefault();
				Assert.That(sender, Is.Not.Null, "Expected sender in _openNetworkSenders");

				// sender.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance)
				var itemsField = sender.GetType().GetField("_items", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
				Assert.That(itemsField, Is.Not.Null, "Expected _items field to exist on sender for backward compatibility");

				var itemsObj = itemsField.GetValue(sender) as System.Collections.ICollection;
				Assert.That(itemsObj, Is.Not.Null, "Expected _items to implement ICollection");

				int count = itemsObj.Count;
				Assert.That(count, Is.GreaterThanOrEqualTo(0));
			}
		}

		[Test]
		public void ObservabilityMetrics_TrackSentAndBatches()
		{
			using (var receiver = new TestTcpReceiver(storeLines: false))
			using (var ctx = CreateTarget(receiver.Address))
			{
				var target = ctx.Target;
				int count = 100;

				for (int i = 0; i < count; i++)
				{
					var ev = new LogEventInfo(LogLevel.Info, "test", $"Msg_{i}");
					target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => { }));
				}

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				flushMre.WaitOne(3000);

				Assert.That(receiver.WaitForMessages(count, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(target.SentMessages, Is.EqualTo(count));
				Assert.That(target.SentBytes, Is.GreaterThan(0));
				Assert.That(target.SentBatches, Is.GreaterThan(0));
			}
		}

		[Test]
		public void QueueOverflow_Discard_IncrementsDroppedMessages()
		{
			// Unreachable address
			using (var ctx = CreateTarget("tcp4://127.0.0.1:1", t =>
			{
				t.MaxQueueSize = 10;
				t.OnQueueOverflow = NetworkTargetQueueOverflowAction.Discard;
				t.ConnectionTimeout = 100;
			}))
			{
				var target = ctx.Target;

				// Overfill queue
				for (int i = 0; i < 50; i++)
				{
					var ev = new LogEventInfo(LogLevel.Info, "test", $"Msg_{i}");
					target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => { }));
				}

				// Producer was not blocked; dropped messages tracked
				Assert.That(target.DroppedMessages + target.SendFailures, Is.GreaterThan(0));
			}
		}
	}
}
