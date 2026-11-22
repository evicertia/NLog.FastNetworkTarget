using System;
using System.IO;
using System.Net;
using System.Text;
using System.Net.Sockets;
using System.Threading;
using System.Collections.Concurrent;

namespace NLog.FastNetworkTarget
{
	public sealed class TestTcpReceiver : IDisposable
	{
		private readonly TcpListener _listener;
		private readonly Thread _acceptThread;
		private readonly IPAddress _listenAddress;
		private volatile bool _running;
		private Socket _clientSocket;

		private long _totalBytesReceived;
		private long _totalLinesReceived;
		private readonly ConcurrentQueue<string> _receivedLines = new ConcurrentQueue<string>();
		private readonly bool _storeLines;

		public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

		public string Address => _listenAddress.AddressFamily == AddressFamily.InterNetworkV6
			? $"tcp6://[{_listenAddress}]:{Port}"
			: $"tcp4://{_listenAddress}:{Port}";

		public string UnspecifiedTcpAddress => _listenAddress.AddressFamily == AddressFamily.InterNetworkV6
			? $"tcp://[{_listenAddress}]:{Port}"
			: $"tcp://{_listenAddress}:{Port}";

		public long TotalBytesReceived => Interlocked.Read(ref _totalBytesReceived);
		public long TotalLinesReceived => Interlocked.Read(ref _totalLinesReceived);
		public ConcurrentQueue<string> ReceivedLines => _receivedLines;

		public TestTcpReceiver(bool storeLines = false, IPAddress listenAddress = null)
		{
			_storeLines = storeLines;
			_listenAddress = listenAddress ?? IPAddress.Loopback;
			_listener = new TcpListener(_listenAddress, 0);
			_listener.Server.NoDelay = true;
			_listener.Start();

			_running = true;
			_acceptThread = new Thread(AcceptLoop)
			{
				IsBackground = true,
				Name = "TestTcpReceiver-Accept"
			};
			_acceptThread.Start();
		}

		private void AcceptLoop()
		{
			while (_running)
			{
				try
				{
					var socket = _listener.AcceptSocket();
					socket.NoDelay = true;
					socket.ReceiveBufferSize = 256 * 1024;
					_clientSocket = socket;

					var worker = new Thread(() => ReceiveLoop(socket))
					{
						IsBackground = true,
						Name = "TestTcpReceiver-Worker"
					};
					worker.Start();
				}
				catch when (!_running)
				{
					break;
				}
				catch
				{
					if (!_running) break;
				}
			}
		}

		private void ReceiveLoop(Socket socket)
		{
			var buffer = new byte[64 * 1024];
			var lineBytes = _storeLines ? new MemoryStream(1024) : null;

			try
			{
				while (_running && socket.Connected)
				{
					int read = socket.Receive(buffer, 0, buffer.Length, SocketFlags.None);
					if (read <= 0)
						break;

					Interlocked.Add(ref _totalBytesReceived, read);

					long lines = 0;
					for (int i = 0; i < read; i++)
					{
						byte b = buffer[i];
						if (b == (byte)'\n')
						{
							lines++;
							if (_storeLines)
							{
								int len = (int)lineBytes.Length;
								if (len > 0 && lineBytes.GetBuffer()[len - 1] == (byte)'\r')
									len--;

								var decoded = Encoding.UTF8.GetString(lineBytes.GetBuffer(), 0, len);
								_receivedLines.Enqueue(decoded);
								lineBytes.SetLength(0);
							}
						}
						else if (_storeLines)
						{
							lineBytes.WriteByte(b);
						}
					}

					if (lines > 0)
						Interlocked.Add(ref _totalLinesReceived, lines);
				}
			}
			catch
			{
				// Connection reset or closed
			}
			finally
			{
				lineBytes?.Dispose();
				try { socket.Close(); } catch { }
				try { socket.Dispose(); } catch { }
			}
		}

		public bool WaitForMessages(long expectedCount, TimeSpan timeout)
		{
			var sw = System.Diagnostics.Stopwatch.StartNew();
			while (sw.Elapsed < timeout)
			{
				if (TotalLinesReceived >= expectedCount)
					return true;
				Thread.Sleep(5);
			}
			return TotalLinesReceived >= expectedCount;
		}

		public bool WaitForBytes(long expectedBytes, TimeSpan timeout)
		{
			var sw = System.Diagnostics.Stopwatch.StartNew();
			while (sw.Elapsed < timeout)
			{
				if (TotalBytesReceived >= expectedBytes)
					return true;
				Thread.Sleep(5);
			}
			return TotalBytesReceived >= expectedBytes;
		}

		public void Dispose()
		{
			_running = false;
			try { _clientSocket?.Close(); } catch { }
			try { _clientSocket?.Dispose(); } catch { }
			try { _listener.Stop(); } catch { }
		}
	}
}
