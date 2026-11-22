using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace NLog.FastNetworkTarget.Benchmarks
{
	public sealed class MockTcpReceiver : IDisposable
	{
		private readonly TcpListener _listener;
		private readonly Thread _acceptThread;
		private volatile bool _running;
		private Socket _clientSocket;

		private long _totalBytesReceived;
		private long _totalLinesReceived;

		public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
		public string Address => $"tcp4://127.0.0.1:{Port}";

		public long TotalBytesReceived => Interlocked.Read(ref _totalBytesReceived);
		public long TotalLinesReceived => Interlocked.Read(ref _totalLinesReceived);

		public MockTcpReceiver()
		{
			_listener = new TcpListener(IPAddress.Loopback, 0);
			_listener.Server.NoDelay = true;
			_listener.Start();

			_running = true;
			_acceptThread = new Thread(AcceptLoop)
			{
				IsBackground = true,
				Name = "MockTcpReceiver-Accept"
			};
			_acceptThread.Start();
		}

		public void Reset()
		{
			Interlocked.Exchange(ref _totalBytesReceived, 0);
			Interlocked.Exchange(ref _totalLinesReceived, 0);
		}

		private void AcceptLoop()
		{
			while (_running)
			{
				try
				{
					var socket = _listener.AcceptSocket();
					socket.NoDelay = true;
					socket.ReceiveBufferSize = 512 * 1024;
					_clientSocket = socket;

					var receiveThread = new Thread(() => ReceiveLoop(socket))
					{
						IsBackground = true,
						Name = "MockTcpReceiver-Worker"
					};
					receiveThread.Start();
				}
				catch when (!_running)
				{
					break;
				}
				catch (Exception)
				{
					if (!_running) break;
				}
			}
		}

		private void ReceiveLoop(Socket socket)
		{
			var buffer = new byte[128 * 1024];

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
						if (buffer[i] == (byte)'\n')
							lines++;
					}

					if (lines > 0)
						Interlocked.Add(ref _totalLinesReceived, lines);
				}
			}
			catch
			{
				// Socket closed or aborted
			}
			finally
			{
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
