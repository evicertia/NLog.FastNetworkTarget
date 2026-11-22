using System;
using System.Net;
using System.Linq;
using System.Text;
using System.Net.Sockets;
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
				Assert.That(target.SentMessages, Is.EqualTo(count), "SentMessages must reflect logical log events, not slab count");
				Assert.That(target.SentBatches, Is.LessThan(count / 10), "Slab coalescing should pack 500 small items into far fewer batches");
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
			// Create a real TCP listener that never reads/accepts after backlog fills, or slow connect (blackhole port with tiny SendBufferSize)
			var stalledListener = new TcpListener(IPAddress.Loopback, 0);
			stalledListener.Start();
			Socket accepted = null;

			try
			{
				int port = ((IPEndPoint)stalledListener.LocalEndpoint).Port;
				using (var ctx = CreateTarget($"tcp4://127.0.0.1:{port}", t =>
				{
					t.MaxQueueSize = 16;
					t.SendBufferSize = 1024;
					t.SendTimeout = 5000;
					t.OnQueueOverflow = NetworkTargetQueueOverflowAction.Discard;
				}))
				{
					var target = ctx.Target;
					var acceptTask = stalledListener.AcceptSocketAsync();

					// Trigger lazy connection with initial write
					var bigPayload = new string('Q', 8192);
					target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Info, "test", bigPayload), ex => { }));

					Assert.That(acceptTask.Wait(3000), Is.True, "Expected lazy connection upon first write");
					accepted = acceptTask.Result;
					accepted.ReceiveBufferSize = 1024;

					// Pump enough 8KB messages to saturate kernel socket buffers + 16-slot ring buffer without reading
					for (int i = 0; i < 500; i++)
					{
						var ev = new LogEventInfo(LogLevel.Info, "test", bigPayload);
						target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => { }));
					}

					Assert.That(target.DroppedMessages, Is.GreaterThan(0), "Expected DroppedMessages > 0 when queue overflows in Discard mode");
				}
			}
			finally
			{
				try { accepted?.Close(); } catch { }
				try { stalledListener.Stop(); } catch { }
			}
		}

		[Test]
		public void Ipv6_UnspecifiedTcpScheme_ConnectsAndReceives()
		{
			if (!Socket.OSSupportsIPv6)
				Assert.Ignore("IPv6 not supported on this host.");

			using (var receiver = new TestTcpReceiver(storeLines: true, listenAddress: IPAddress.IPv6Loopback))
			using (var ctx = CreateTarget(receiver.UnspecifiedTcpAddress))
			{
				var target = ctx.Target;
				var mre = new ManualResetEvent(false);
				Exception sendEx = null;

				target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Info, "ipv6", "Hello IPv6"), ex =>
				{
					sendEx = ex;
					mre.Set();
				}));

				Assert.That(mre.WaitOne(3000), Is.True);
				Assert.That(sendEx, Is.Null);
				Assert.That(receiver.WaitForMessages(1, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(receiver.ReceivedLines.TryDequeue(out var line), Is.True);
				Assert.That(line, Is.EqualTo("Hello IPv6"));
			}
		}

		[Test]
		public void BatchWrite_DynamicAddress_RoutesToCorrectEndpoints()
		{
			using (var receiverA = new TestTcpReceiver(storeLines: true))
			using (var receiverB = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget("${event-properties:endpoint}"))
			{
				var target = ctx.Target;
				var batch = new AsyncLogEventInfo[20];

				for (int i = 0; i < batch.Length; i++)
				{
					bool isA = (i % 2) == 0;
					var ev = new LogEventInfo(LogLevel.Info, "router", isA ? $"A_{i}" : $"B_{i}");
					ev.Properties["endpoint"] = isA ? receiverA.Address : receiverB.Address;
					batch[i] = new AsyncLogEventInfo(ev, ex => { });
				}

				target.WriteAsyncLogEvents(batch);

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				Assert.That(flushMre.WaitOne(5000), Is.True);

				Assert.That(receiverA.WaitForMessages(10, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(receiverB.WaitForMessages(10, TimeSpan.FromSeconds(3)), Is.True);

				for (int i = 0; i < 20; i += 2)
				{
					Assert.That(receiverA.ReceivedLines.TryDequeue(out var lineA), Is.True);
					Assert.That(lineA, Is.EqualTo($"A_{i}"));
				}

				for (int i = 1; i < 20; i += 2)
				{
					Assert.That(receiverB.ReceivedLines.TryDequeue(out var lineB), Is.True);
					Assert.That(lineB, Is.EqualTo($"B_{i}"));
				}
			}
		}

		[Test]
		public void BatchWrite_RespectsMaxMessageSize_DiscardAndError()
		{
			using (var receiver = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget(receiver.Address, t =>
			{
				t.MaxMessageSize = 30;
				t.OnOverflow = NetworkTargetOverflowAction.Error;
			}))
			{
				var target = ctx.Target;
				var exceptions = new Exception[3];
				var countdown = new CountdownEvent(3);

				var ev0 = new LogEventInfo(LogLevel.Info, "test", "short-0");
				var ev1 = new LogEventInfo(LogLevel.Info, "test", new string('X', 80)); // Exceeds MaxMessageSize=30
				var ev2 = new LogEventInfo(LogLevel.Info, "test", "short-2");

				target.WriteAsyncLogEvents(new[]
				{
					new AsyncLogEventInfo(ev0, ex => { exceptions[0] = ex; countdown.Signal(); }),
					new AsyncLogEventInfo(ev1, ex => { exceptions[1] = ex; countdown.Signal(); }),
					new AsyncLogEventInfo(ev2, ex => { exceptions[2] = ex; countdown.Signal(); })
				});

				Assert.That(countdown.Wait(3000), Is.True);
				Assert.That(exceptions[0], Is.Null);
				Assert.That(exceptions[1], Is.InstanceOf<OverflowException>());
				Assert.That(exceptions[2], Is.Null);

				Assert.That(receiver.WaitForMessages(2, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(receiver.ReceivedLines.TryDequeue(out var l0) && l0 == "short-0", Is.True);
				Assert.That(receiver.ReceivedLines.TryDequeue(out var l2) && l2 == "short-2", Is.True);
			}
		}

		[Test]
		public void MaxMessageSize_SplitWithNewLine_TerminatesEveryChunkAndPreservesUtf8()
		{
			using (var receiver = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget(receiver.Address, t =>
			{
				t.MaxMessageSize = 25; // Small enough to force multiple chunks on 3-byte '€' chars + '\n'
				t.OnOverflow = NetworkTargetOverflowAction.Split;
				t.NewLine = true;
				t.LineEnding = LineEndingMode.LF;
			}))
			{
				var target = ctx.Target;
				var euroMessage = new string('€', 20); // 60 UTF-8 bytes
				var validNextJson = "{\"status\":\"ok\"}";

				var done1 = new ManualResetEvent(false);
				var done2 = new ManualResetEvent(false);

				target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Error, "test", euroMessage), ex => done1.Set()));
				target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Info, "test", validNextJson), ex => done2.Set()));

				Assert.That(done1.WaitOne(3000), Is.True);
				Assert.That(done2.WaitOne(3000), Is.True);

				var flushMre = new ManualResetEvent(false);
				target.Flush(ex => flushMre.Set());
				Assert.That(flushMre.WaitOne(3000), Is.True);

				// Wait until at least 4 lines (3+ split chunks + 1 valid JSON line) arrive
				Assert.That(receiver.WaitForMessages(4, TimeSpan.FromSeconds(3)), Is.True);

				var lines = new List<string>();
				while (receiver.ReceivedLines.TryDequeue(out var line))
					lines.Add(line);

				// Last line must be the intact JSON message (NOT concatenated onto a truncated chunk!)
				Assert.That(lines[lines.Count - 1], Is.EqualTo(validNextJson));

				// Reconstruct split chunks and verify zero UTF-8 replacement chars (\uFFFD) and exact content
				var reconstructed = string.Concat(lines.Take(lines.Count - 1));
				Assert.That(reconstructed.IndexOf('\uFFFD'), Is.EqualTo(-1), "UTF-8 multi-byte sequence was corrupted across split boundary");
				Assert.That(reconstructed, Is.EqualTo(euroMessage));
				Assert.That(target.SentMessages, Is.EqualTo(2), "SentMessages should count 2 logical events despite chunking");
			}
		}

		[Test]
		public void BatchWrite_SendFailure_PropagatesExceptionToContinuations()
		{
			using (var ctx = CreateTarget("tcp4://127.0.0.1:1", t =>
			{
				t.ConnectionTimeout = 150;
			}))
			{
				var target = ctx.Target;
				int count = 5;
				var exceptions = new Exception[count];
				var countdown = new CountdownEvent(count);

				var batch = new AsyncLogEventInfo[count];
				for (int i = 0; i < count; i++)
				{
					int idx = i;
					batch[i] = new AsyncLogEventInfo(
						new LogEventInfo(LogLevel.Info, "test", $"FailBatch_{i}"),
						ex =>
						{
							exceptions[idx] = ex;
							countdown.Signal();
						});
				}

				target.WriteAsyncLogEvents(batch);

				Assert.That(countdown.Wait(3000), Is.True, "All batch continuations must be invoked on send/connect failure");
				for (int i = 0; i < count; i++)
					Assert.That(exceptions[i], Is.Not.Null);
			}
		}

		[Test]
		public void ConnectionCacheSize_EvictsLeastRecentlyUsed_AndPreservesMetrics()
		{
			using (var r1 = new TestTcpReceiver(storeLines: true))
			using (var r2 = new TestTcpReceiver(storeLines: true))
			using (var r3 = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget("${event-properties:addr}", t =>
			{
				t.ConnectionCacheSize = 2;
			}))
			{
				var target = ctx.Target;

				void SendOne(string addr, string msg)
				{
					var ev = new LogEventInfo(LogLevel.Info, "lru", msg);
					ev.Properties["addr"] = addr;
					var mre = new ManualResetEvent(false);
					target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => mre.Set()));
					Assert.That(mre.WaitOne(3000), Is.True);
				}

				SendOne(r1.Address, "m1");
				Thread.Sleep(2);
				SendOne(r2.Address, "m2");
				Thread.Sleep(2);
				SendOne(r1.Address, "m3"); // Touch r1 so r2 is LRU
				Thread.Sleep(2);
				SendOne(r3.Address, "m4"); // Evicts r2

				Assert.That(target.ConnectionCount, Is.EqualTo(2));
				Assert.That(r1.WaitForMessages(2, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(r2.WaitForMessages(1, TimeSpan.FromSeconds(3)), Is.True);
				Assert.That(r3.WaitForMessages(1, TimeSpan.FromSeconds(3)), Is.True);

				// Wait briefly for evicted sender close callback to retire metrics
				Thread.Sleep(50);
				Assert.That(target.SentMessages, Is.EqualTo(4));
			}
		}

		[Test]
		public void KeepConnectionFalse_OpensAndClosesPerEvent()
		{
			using (var receiver = new TestTcpReceiver(storeLines: true))
			using (var ctx = CreateTarget(receiver.Address, t =>
			{
				t.KeepConnection = false;
				t.MaxConnections = 2;
				t.OnConnectionOverflow = NetworkTargetConnectionsOverflowAction.Block;
			}))
			{
				var target = ctx.Target;
				var countdown = new CountdownEvent(5);

				for (int i = 0; i < 5; i++)
				{
					var ev = new LogEventInfo(LogLevel.Info, "transient", $"Transient_{i}");
					target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => countdown.Signal()));
				}

				Assert.That(countdown.Wait(5000), Is.True);
				Assert.That(receiver.WaitForMessages(5, TimeSpan.FromSeconds(3)), Is.True);

				Thread.Sleep(50);
				Assert.That(target.ConnectionCount, Is.EqualTo(0));
				Assert.That(target.SentMessages, Is.EqualTo(5));
			}
		}

		[Test]
		public void UnrecognizedAddressScheme_ReportsArgumentException()
		{
			using (var ctx = CreateTarget("udp://127.0.0.1:9999"))
			{
				Exception captured = null;
				var mre = new ManualResetEvent(false);

				ctx.Target.WriteAsyncLogEvent(new AsyncLogEventInfo(new LogEventInfo(LogLevel.Info, "test", "bad"), ex =>
				{
					captured = ex;
					mre.Set();
				}));

				Assert.That(mre.WaitOne(2000), Is.True);
				Assert.That(captured, Is.InstanceOf<ArgumentException>());
			}
		}

		[Test]
		public void ConcurrentWriteAndClose_RaceStress_NoExceptionsOrLeaks()
		{
			using (var receiver = new TestTcpReceiver())
			{
				var ctx = CreateTarget(receiver.Address);
				var target = ctx.Target;
				int stop = 0;
				var unhandled = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
				var threads = new Thread[6];

				for (int t = 0; t < threads.Length; t++)
				{
					int idx = t;
					threads[t] = new Thread(() =>
					{
						try
						{
							int n = 0;
							while (Volatile.Read(ref stop) == 0)
							{
								var ev = new LogEventInfo(LogLevel.Info, "race", $"Race_{idx}_{n++}");
								if ((n & 3) == 0)
								{
									target.WriteAsyncLogEvents(new[]
									{
										new AsyncLogEventInfo(ev, ex => { }),
										new AsyncLogEventInfo(ev, ex => { })
									});
								}
								else
								{
									target.WriteAsyncLogEvent(new AsyncLogEventInfo(ev, ex => { }));
								}

								_ = target.PendingMessages;
								_ = target.SentMessages;
								_ = target.DroppedMessages;
								_ = target.ConnectionCount;
							}
						}
						catch (Exception ex)
						{
							unhandled.Enqueue(ex);
						}
					})
					{
						IsBackground = true
					};
					threads[t].Start();
				}

				Thread.Sleep(80);
				ctx.Dispose();
				Thread.Sleep(20);
				Volatile.Write(ref stop, 1);

				for (int t = 0; t < threads.Length; t++)
					threads[t].Join(3000);

				Assert.That(unhandled, Is.Empty);
				Assert.That(target.ConnectionCount, Is.EqualTo(0));
			}
		}
	}
}
