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
				lock (this)
				{
					_log.WriteLine("Send sync {0} {1} '{2}'", offset, size, Encoding.UTF8.GetString(buffer, offset, size));

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

				return size;
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
			public StringWriter Log { get; set; }

			#endregion

			#region .ctors

			public MyTcpNetworkSender(string url, AddressFamily addressFamily)
				: base(url, addressFamily)
			{
				MaxQueueSize = 100;
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
		}
	}
}