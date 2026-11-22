using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.Collections.Generic;

using NSubstitute;
using NUnit.Framework;

using NLog.Internal.NetworkSenders;

namespace NLog.FastNetworkTarget
{
	public class TcpNetworkSenderTests
	{
		#region Inner classes

		internal class MockSocket : ISocket
		{
			#region Fields & Properties

			private readonly MyTcpNetworkSender _sender;
			private readonly StringWriter _log;
			private bool _connected;
			private bool _faulted;

			public bool Connected => _connected;

			#endregion

			#region .ctors

			public MockSocket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType, MyTcpNetworkSender sender)
			{
				_sender = sender;
				_connected = true;
				_log = sender.Log;
				_log.WriteLine("Create socket {0} {1} {2}", addressFamily, socketType, protocolType);
			}

			#endregion

			public void Close()
			{
				lock (this)
					_log.WriteLine("Close");
			}

			public IAsyncResult BeginConnect(EndPoint remoteEP, AsyncCallback callback, object state)
			{
				_log.WriteLine("Connect async to {0}", remoteEP);

				lock (this)
				{
					if (_sender.ConnectFailure > 0)
					{
						_sender.ConnectFailure--;
						_connected = false;
						_faulted = true;
						_log.WriteLine("Failed");
					}
				}

				if (!_connected)
					throw new SocketException((int)SocketError.NotConnected);

				var asyncResult = Substitute.For<IAsyncResult>();
				asyncResult.AsyncWaitHandle.Returns(new ManualResetEvent(true));
				asyncResult.CompletedSynchronously.Returns(true);
				asyncResult.IsCompleted.Returns(false);
				asyncResult.AsyncState.Returns(state);

				return asyncResult;
			}

			public void EndConnect(IAsyncResult asyncResult)
			{
				lock (this)
					_log.WriteLine("Conection ended");
			}

			public int Send(byte[] buffer, int offset, int size, SocketFlags socketFlags)
			{
				_sender.SendStartedSignal?.Set();
				_sender.SendGate?.WaitOne(5000);

				int effectiveSize = _sender.MaxSendChunkSize > 0
					? Math.Min(size, _sender.MaxSendChunkSize)
					: size;

				lock (this)
				{
					_log.WriteLine("Send sync {0} {1} '{2}'", offset, effectiveSize, Encoding.UTF8.GetString(buffer, offset, effectiveSize));

					if (_sender.SendFailureIn > 0)
					{
						_sender.SendFailureIn--;

						if (_sender.SendFailureIn == 0)
						{
							_connected = false;
							_faulted = true;
						}
					}

					if (_faulted)
						_log.WriteLine("Failed");

					if (!_connected)
						throw new SocketException((int)SocketError.NotConnected);
				}

				return effectiveSize;
			}

			public void Dispose()
			{
				// Do nothing..
			}
		}

		internal class MockEndPoint : EndPoint
		{
			#region Fields

			private readonly Uri _uri;

			#endregion

			#region .ctors

			public MockEndPoint(Uri uri)
			{
				_uri = uri;
			}

			#endregion

			public override AddressFamily AddressFamily => (AddressFamily)10000;

			public override string ToString() => $"{{mock end point: {_uri}}}";
		}

		internal class MyTcpNetworkSender : TcpNetworkSender
		{
			#region Properties

			public int SendFailureIn { get; set; }
			public int ConnectFailure { get; set; }
			public int MaxSendChunkSize { get; set; }
			public ManualResetEvent SendGate { get; set; }
			public ManualResetEvent SendStartedSignal { get; set; }
			public StringWriter Log { get; set; }

			#endregion

			#region .ctors

			public MyTcpNetworkSender(string url, AddressFamily addressFamily)
				: base(url, addressFamily)
			{
				MaxQueueSize = 100;
				BatchSize = 1;
				Log = new StringWriter();
				ConnectionTimeout = TimeSpan.FromSeconds(1);
			}

			#endregion

			protected internal override ISocket CreateSocket(string host, AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType)
			{
				return new MockSocket(addressFamily, socketType, protocolType, this);
			}

			protected override EndPoint ParseEndpointAddress(Uri uri, AddressFamily addressFamily)
			{
				Log.WriteLine("Parse endpoint address {0} {1}", uri, addressFamily);
				return new MockEndPoint(uri);
			}
		}

		#endregion

		[Test]
		public void TcpHappyPathTest()
		{
			var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified);
			sender.Initialize();

			var buffer = Encoding.UTF8.GetBytes("quick brown fox jumps over the lazy dog");
			var allSent = new ManualResetEvent(false);
			var exceptions = new List<Exception>();

			for (var i = 1; i < 8; i *= 2)
			{
				sender.Send(
					buffer, 0, i, ex =>
					{
						lock (exceptions)
							exceptions.Add(ex);
					});
			}

			var mre = new ManualResetEvent(false);

			sender.FlushAsync(ex =>
			{
				lock (exceptions)
					exceptions.Add(ex);

				mre.Set();
			});

			mre.WaitOne();

			var actual = sender.Log.ToString();
			Assert.That(actual.IndexOf("Create socket Unspecified Stream Tcp") != -1);
			Assert.That(actual.IndexOf("Parse endpoint address tcp+thread://hostname:123/ Unspecified") != -1);
			Assert.That(actual.IndexOf("Connect async to {mock end point: tcp+thread://hostname:123/}") != -1);
			Assert.That(actual.IndexOf("Conection ended") != -1);
			Assert.That(actual.IndexOf("Send sync 0 1 'q'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 2 'qu'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 4 'quic'") != -1);

			for (var i = 1; i < 8; i *= 2)
			{
				sender.Send(
					buffer, 0, i, ex =>
					{
						lock (exceptions)
						{
							exceptions.Add(ex);
							if (exceptions.Count == 7)
								allSent.Set();
						}
					});
			}

			Assert.That(allSent.WaitOne(3000, false));

			actual = sender.Log.ToString();

			Assert.That(actual.IndexOf("Create socket Unspecified Stream Tcp") != -1);
			Assert.That(actual.IndexOf("Parse endpoint address tcp+thread://hostname:123/ Unspecified") != -1);
			Assert.That(actual.IndexOf("Connect async to {mock end point: tcp+thread://hostname:123/}") != -1);
			Assert.That(actual.IndexOf("Conection ended") != -1);
			Assert.That(actual.IndexOf("Send sync 0 1 'q'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 2 'qu'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 4 'quic'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 1 'q'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 2 'qu'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 4 'quic'") != -1);

			foreach (var ex in exceptions)
				Assert.That(ex, Is.Null);
		}

		[Test]
		public void TcpProxyTest()
		{
			var sender = new TcpNetworkSender("tcp+thread://foo:1234", AddressFamily.Unspecified);
			var socket = sender.CreateSocket("foo", AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

			Assert.That(socket is SocketProxy);
		}

		[Test]
		public void TcpConnectFailureTest()
		{
			var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified)
			{
				ConnectFailure = 1
			};

			sender.Initialize();

			var buffer = Encoding.UTF8.GetBytes("quick brown fox jumps over the lazy dog");
			var allSent = new ManualResetEvent(false);
			var exceptions = new List<Exception>();

			for (var i = 1; i < 8; i++)
			{
				sender.Send(
					buffer, 0, i, ex =>
					{
						lock (exceptions)
						{
							exceptions.Add(ex);
							if (exceptions.Count == 7)
								allSent.Set();
						}
					});
			}

			Assert.That(allSent.WaitOne(3000, false));

			var mre = new ManualResetEvent(false);
			sender.FlushAsync(ex => mre.Set());
			mre.WaitOne(3000, false);

			var actual = sender.Log.ToString();
			Assert.That(actual.IndexOf("Create socket Unspecified Stream Tcp") != -1);
			Assert.That(actual.IndexOf("Parse endpoint address tcp+thread://hostname:123/ Unspecified") != -1);
			Assert.That(actual.IndexOf("Connect async to {mock end point: tcp+thread://hostname:123/}") != -1);
			Assert.That(actual.IndexOf("Failed") != -1);

			foreach (var ex in exceptions)
				Assert.That(ex, Is.Not.Null);
		}

		[Test]
		public void TcpSendFailureTest()
		{
			var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified)
			{
				SendFailureIn = 3, //< Will cause failure on 3rd send
			};

			sender.Initialize();

			var buffer = Encoding.UTF8.GetBytes("quick brown fox jumps over the lazy dog");
			var writeFinished = new ManualResetEvent(false);
			var exceptions = new Exception[9];
			var remaining = exceptions.Length;

			for (var i = 1; i < 10; i++)
			{
				var pos = i - 1;

				sender.Send(
					buffer, 0, i, ex =>
					{
						lock (exceptions)
						{
							exceptions[pos] = ex;
							if (--remaining == 0)
								writeFinished.Set();
						}
					});
			}

			writeFinished.WaitOne();

			var actual = sender.Log.ToString();
			Assert.That(actual.IndexOf("Create socket Unspecified Stream Tcp") != -1);
			Assert.That(actual.IndexOf("Parse endpoint address tcp+thread://hostname:123/ Unspecified") != -1);
			Assert.That(actual.IndexOf("Connect async to {mock end point: tcp+thread://hostname:123/}") != -1);
			Assert.That(actual.IndexOf("Conection ended") != -1);
			Assert.That(actual.IndexOf("Send sync 0 1 'q'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 2 'qu'") != -1);
			Assert.That(actual.IndexOf("Send sync 0 3 'qui'") != -1);
			Assert.That(actual.IndexOf("Failed") != -1);

			for (var i = 0; i < exceptions.Length; ++i)
			{
				if (i < 2)
					Assert.That(exceptions[i], Is.Null);
				else
					Assert.That(exceptions[i], Is.Not.Null);
			}

			sender.Close(ex => { });
		}

		[Test]
		public void TcpPartialSend_InvokesContinuationOnceOnly()
		{
			var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified)
			{
				MaxSendChunkSize = 3
			};
			sender.Initialize();

			var buffer = Encoding.UTF8.GetBytes("0123456789ABCDEF"); // 16 bytes -> 6 Send() calls (3+3+3+3+3+1)
			int continuationCount = 0;
			Exception capturedEx = null;
			var done = new ManualResetEvent(false);

			sender.Send(buffer, 0, buffer.Length, ex =>
			{
				capturedEx = ex;
				Interlocked.Increment(ref continuationCount);
				done.Set();
			});

			Assert.That(done.WaitOne(3000), Is.True);
			Thread.Sleep(50);

			Assert.That(capturedEx, Is.Null);
			Assert.That(continuationCount, Is.EqualTo(1), "Continuation must be invoked exactly once after full transmission");
			Assert.That(sender.SentMessages, Is.EqualTo(1));
			Assert.That(sender.SentBytes, Is.EqualTo(16));

			sender.Close(ex => { });
		}

		[Test]
		public void TcpQueueOverflow_Discard_DirectlyIncrementsDroppedMessages()
		{
			using (var sendGate = new ManualResetEvent(false))
			using (var sendStarted = new ManualResetEvent(false))
			{
				var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified)
				{
					MaxQueueSize = 16,
					OnQueueOverflow = NLog.Targets.NetworkTargetQueueOverflowAction.Discard,
					SendGate = sendGate,
					SendStartedSignal = sendStarted
				};
				sender.Initialize();

				var payload = Encoding.UTF8.GetBytes("overflow-test");

				// First message enters Send() and blocks on sendGate
				sender.Send(payload, 0, payload.Length, ex => { });
				Assert.That(sendStarted.WaitOne(3000), Is.True);

				// Enqueue 40 more items into 16-slot ring -> deterministic overflow discards
				int completedContinuations = 0;
				for (int i = 0; i < 40; i++)
				{
					sender.Send(payload, 0, payload.Length, ex => Interlocked.Increment(ref completedContinuations));
				}

				Assert.That(sender.DroppedMessages, Is.GreaterThanOrEqualTo(24));
				Assert.That(sender.SendFailures, Is.EqualTo(0));

				sendGate.Set();
				var flushed = new ManualResetEvent(false);
				sender.FlushAsync(ex => flushed.Set());
				Assert.That(flushed.WaitOne(3000), Is.True);

				sender.Close(ex => { });
			}
		}

		[Test]
		public void TcpQueueOverflow_Block_IncrementsBlockedEnqueuesAndDeliversAll()
		{
			using (var sendGate = new ManualResetEvent(false))
			using (var sendStarted = new ManualResetEvent(false))
			{
				var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified)
				{
					MaxQueueSize = 16,
					OnQueueOverflow = NLog.Targets.NetworkTargetQueueOverflowAction.Block,
					SendGate = sendGate,
					SendStartedSignal = sendStarted
				};
				sender.Initialize();

				var payload = Encoding.UTF8.GetBytes("block-test");

				// First item enters Send() and holds consumer
				sender.Send(payload, 0, payload.Length, ex => { });
				Assert.That(sendStarted.WaitOne(3000), Is.True);

				int totalToEnqueue = 24;
				var producerDone = new ManualResetEvent(false);
				var producer = new Thread(() =>
				{
					for (int i = 0; i < totalToEnqueue; i++)
						sender.Send(payload, 0, payload.Length, ex => { });
					producerDone.Set();
				})
				{
					IsBackground = true
				};
				producer.Start();

				// Wait until producer blocks on full ring
				var sw = System.Diagnostics.Stopwatch.StartNew();
				while (sender.BlockedEnqueues == 0 && sw.ElapsedMilliseconds < 2000)
					Thread.Sleep(5);

				Assert.That(sender.BlockedEnqueues, Is.GreaterThan(0));

				// Release gate so all items drain
				sendGate.Set();
				Assert.That(producerDone.WaitOne(3000), Is.True);

				var flushed = new ManualResetEvent(false);
				sender.FlushAsync(ex => flushed.Set());
				Assert.That(flushed.WaitOne(3000), Is.True);

				Assert.That(sender.DroppedMessages, Is.EqualTo(0));
				Assert.That(sender.SentMessages, Is.EqualTo(1 + totalToEnqueue));

				sender.Close(ex => { });
			}
		}

		[Test]
		public void TcpSendAfterClose_InvokesContinuationWithError()
		{
			var sender = new MyTcpNetworkSender("tcp+thread://hostname:123", AddressFamily.Unspecified);
			sender.Initialize();

			var closed = new ManualResetEvent(false);
			sender.Close(ex => closed.Set());
			Assert.That(closed.WaitOne(3000), Is.True);

			Exception afterCloseEx = null;
			var contCalled = new ManualResetEvent(false);
			sender.Send(new byte[] { 1, 2, 3 }, 0, 3, ex =>
			{
				afterCloseEx = ex;
				contCalled.Set();
			});

			Assert.That(contCalled.WaitOne(1000), Is.True);
			Assert.That(afterCloseEx, Is.Not.Null);
		}
	}
}