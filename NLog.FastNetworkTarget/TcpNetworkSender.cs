using System;
using System.Threading;
using System.Diagnostics;
using System.Net.Sockets;
using System.Collections;
using System.Security.Authentication;
using System.Runtime.CompilerServices;

using NLog.Common;
using NLog.Targets;

namespace NLog.Internal.NetworkSenders
{
	internal class TcpNetworkSender : NetworkSender
	{
		#region Constants & Signals

		private const int SIGNAL_DATA = 0;
		private const int CLOSE_SIGNAL = -31337;
		private const int FLUSH_SIGNAL = -31338;

		private const int STATE_RUNNING = 0;
		private const int STATE_SLEEPING = 1;

		#endregion

		#region Ring Buffer Slot Structure

		private struct Slot
		{
			public int Sequence;
			public byte[] Buffer;
			public int Offset;
			public int Length;
			public AsyncContinuation Continuation;
			public int Signal;
		}

		#endregion

		#region QueueShim for Herma.Facilities NLogStatsFetcher reflection backward compatibility

		private readonly QueueShim _items;

		private sealed class QueueShim : ICollection
		{
			private readonly TcpNetworkSender _owner;

			public QueueShim(TcpNetworkSender owner)
			{
				_owner = owner;
			}

			public int Count => (int)_owner.PendingMessages;
			public object SyncRoot => this;
			public bool IsSynchronized => false;

			public void CopyTo(Array array, int index) { }
			public IEnumerator GetEnumerator()
			{
				return System.Linq.Enumerable.Empty<object>().GetEnumerator();
			}
		}

		#endregion

		#region Fields & Properties

		private readonly object _lock = new object();
		private readonly AutoResetEvent _dataAvailable = new AutoResetEvent(false);
		private readonly AutoResetEvent _spaceAvailable = new AutoResetEvent(false);

		private volatile bool _isClosed;
		private volatile Thread _thread;
		private ISocket _socket;
		private Uri _uri;
		private bool? _enableKeepAliveSuccessful;

		// Bounded MPSC Ring Buffer
		private Slot[] _ring;
		private int _capacity;
		private int _mask;
		private int _enqueuePos;
		private int _dequeuePos;
		private int _consumerState;

		// Observability Metrics
		private long _sentMessages;
		private long _sentBytes;
		private long _sentBatches;
		private long _droppedMessages;
		private long _blockedEnqueues;
		private long _sendFailures;
		private long _reconnectCount;

		// Batching & Socket Options
		internal AddressFamily AddressFamily { get; set; }
		internal SslProtocols SslProtocols { get; set; }
		internal TimeSpan KeepAliveTime { get; set; }
		internal TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(1);
		internal int MaxQueueSize { get; set; } = 8192;

		public bool NoDelay { get; set; } = true;
		public int SendTimeout { get; set; } = 30000;
		public int SendBufferSize { get; set; } = 256 * 1024;
		public int BatchSize { get; set; } = 512;
		public int MaxBatchBytes { get; set; } = 256 * 1024;
		public NetworkTargetQueueOverflowAction OnQueueOverflow { get; set; } = NetworkTargetQueueOverflowAction.Discard;

		// Public Metrics
		public long PendingMessages => Math.Max(0, Volatile.Read(ref _enqueuePos) - Volatile.Read(ref _dequeuePos));
		public long SentMessages => Volatile.Read(ref _sentMessages);
		public long SentBytes => Volatile.Read(ref _sentBytes);
		public long SentBatches => Volatile.Read(ref _sentBatches);
		public long DroppedMessages => Volatile.Read(ref _droppedMessages);
		public long BlockedEnqueues => Volatile.Read(ref _blockedEnqueues);
		public long SendFailures => Volatile.Read(ref _sendFailures);
		public long ReconnectCount => Volatile.Read(ref _reconnectCount);

		#endregion

		#region .ctors

		public TcpNetworkSender(string url, AddressFamily addressFamily)
			: base(url)
		{
			AddressFamily = addressFamily;
			_items = new QueueShim(this);
		}

		#endregion

		#region NetworkSender lifecycle

		protected override void DoInitialize()
		{
			if (_thread == null)
			{
				lock (_lock)
				{
					if (_thread == null)
					{
						_uri = new Uri(Address);

						// Compute power of 2 ring capacity >= MaxQueueSize (minimum 16)
						int cap = 16;
						while (cap < MaxQueueSize)
							cap <<= 1;

						_capacity = cap;
						_mask = cap - 1;
						_ring = new Slot[_capacity];

						for (int i = 0; i < _capacity; i++)
							_ring[i].Sequence = i;

						_enqueuePos = 0;
						_dequeuePos = 0;

						_thread = new Thread(Main)
						{
							Name = $"{nameof(TcpNetworkSender)} - {Address}",
							IsBackground = true
						};

						_thread.Start();
					}
				}
			}

			base.DoInitialize();
		}

		protected override void DoSend(byte[] bytes, int offset, int length, AsyncContinuation asyncContinuation)
		{
			if (_isClosed)
			{
				ByteBufferPool.Return(bytes);
				asyncContinuation?.Invoke(new InvalidOperationException("NetworkSender is closed."));
				return;
			}

			EnqueueItem(bytes, offset, length, asyncContinuation, SIGNAL_DATA);
		}

		protected override void DoFlush(AsyncContinuation continuation)
		{
			if (_isClosed)
			{
				continuation?.Invoke(null);
				return;
			}

			EnqueueItem(null, 0, 0, continuation, FLUSH_SIGNAL);
		}

		protected override void DoClose(AsyncContinuation continuation)
		{
			if (_isClosed)
			{
				continuation?.Invoke(null);
				return;
			}

			_isClosed = true;
			EnqueueItem(null, 0, 0, continuation, CLOSE_SIGNAL);
		}

		#endregion

		#region Enqueue Logic

		private void EnqueueItem(byte[] bytes, int offset, int length, AsyncContinuation continuation, int signal)
		{
			while (!_isClosed || signal == CLOSE_SIGNAL)
			{
				int head = Volatile.Read(ref _enqueuePos);
				int tail = Volatile.Read(ref _dequeuePos);

				if (head - tail >= _capacity)
				{
					if (signal == CLOSE_SIGNAL)
					{
						// Close signal must never be dropped; yield briefly
						Thread.Sleep(1);
						continue;
					}

					if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Discard)
					{
						Interlocked.Increment(ref _droppedMessages);
						if (bytes != null)
							ByteBufferPool.Return(bytes);

						continuation?.Invoke(null);
						return;
					}
					else // Block
					{
						Interlocked.Increment(ref _blockedEnqueues);
						_spaceAvailable.WaitOne(50);
						continue;
					}
				}

				if (Interlocked.CompareExchange(ref _enqueuePos, head + 1, head) == head)
				{
					int index = head & _mask;
					var spinner = new SpinWait();

					while (Volatile.Read(ref _ring[index].Sequence) != head)
						spinner.SpinOnce();

					_ring[index].Buffer = bytes;
					_ring[index].Offset = offset;
					_ring[index].Length = length;
					_ring[index].Continuation = continuation;
					_ring[index].Signal = signal;

					Volatile.Write(ref _ring[index].Sequence, head + 1);

					if (Volatile.Read(ref _consumerState) == STATE_SLEEPING)
						_dataAvailable.Set();

					return;
				}
			}

			// Closed
			if (bytes != null)
				ByteBufferPool.Return(bytes);

			continuation?.Invoke(null);
		}

		#endregion

		#region Socket & Thread Methods

		private static readonly int[] DelayIntervals = new[] { 50, 100, 250, 500, 1000 };
		private static int GetDelay(uint step) => DelayIntervals[Math.Min(DelayIntervals.Length - 1, step)];

		private static bool TrySetSocketOption(Socket socket, SocketOptionLevel level, SocketOptionName name, object value)
		{
			try
			{
				socket.SetSocketOption(level, name, value);
				return true;
			}
			catch (Exception ex)
			{
				InternalLogger.Warn(ex, "Failed to configure socket option with level: {0}, name: {1}, value: {2}.", level, name, value);
				return false;
			}
		}

		private static bool TryEnableKeepAlive(Socket socket, int keepAliveTimeSeconds)
		{
			if (TrySetSocketOption(socket, SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true))
			{
				var TcpKeepAliveTime = (SocketOptionName)0x3;
				var TcpKeepAliveInterval = (SocketOptionName)0x11;

				if (PlatformDetector.CurrentOS == RuntimeOS.Linux)
				{
					TcpKeepAliveTime = (SocketOptionName)0x4;
					TcpKeepAliveInterval = (SocketOptionName)0x5;
				}
				else if (PlatformDetector.CurrentOS == RuntimeOS.MacOSX)
				{
					TcpKeepAliveTime = (SocketOptionName)0x10;
					TcpKeepAliveInterval = (SocketOptionName)0x101;
				}

				if (TrySetSocketOption(socket, SocketOptionLevel.Tcp, TcpKeepAliveTime, keepAliveTimeSeconds))
				{
					TrySetSocketOption(socket, SocketOptionLevel.Tcp, TcpKeepAliveInterval, 1);
					return true;
				}
			}

			return false;
		}

		protected internal virtual ISocket CreateSocket(string host, AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType)
		{
			var socketAf = addressFamily == AddressFamily.Unspecified ? AddressFamily.InterNetwork : addressFamily;
			var result = new SocketProxy(socketAf, socketType, protocolType);
			var socket = result.UnderlyingSocket;

			if (NoDelay)
			{
				try { socket.NoDelay = true; } catch { }
			}

			if (SendTimeout > 0)
			{
				try { socket.SendTimeout = SendTimeout; } catch { }
			}

			if (SendBufferSize > 0)
			{
				try { socket.SendBufferSize = SendBufferSize; } catch { }
			}

			if (KeepAliveTime.TotalSeconds >= 1.0 && _enableKeepAliveSuccessful != false)
				_enableKeepAliveSuccessful = TryEnableKeepAlive(socket, (int)KeepAliveTime.TotalSeconds);

			if (SslProtocols != SslProtocols.None)
				throw new NotImplementedException("SSL socket connection not implemented.");

			return result;
		}

		private void Connect()
		{
			_socket = CreateSocket(_uri.Host, AddressFamily, SocketType.Stream, ProtocolType.Tcp);

			var endpoint = ParseEndpointAddress(_uri, AddressFamily);
			var asyncResult = _socket.BeginConnect(endpoint, null, null);
			var signalReceived = asyncResult.AsyncWaitHandle.WaitOne(ConnectionTimeout, true);

			if (!signalReceived)
			{
				try { CloseSocket(); } catch { }
				throw new SocketException((int)SocketError.TimedOut);
			}

			_socket.EndConnect(asyncResult);
		}

		private void CloseSocket()
		{
			var sock = _socket;
			_socket = null;

			if (sock != null)
			{
				try
				{
					if (sock.Connected)
						sock.Close();
				}
				catch { }

				try { sock.Dispose(); } catch { }
			}
		}

		private void SendFully(byte[] buffer, int offset, int count)
		{
			int left = count;
			while (left > 0)
			{
				int sent = _socket.Send(buffer, offset + (count - left), left, SocketFlags.None);
				if (sent <= 0)
					throw new SocketException((int)SocketError.ConnectionReset);

				left -= sent;
			}
		}

		private void Main()
		{
			uint step = 0;
			var sw = new Stopwatch();

			// Persistent send coalescing buffer
			int maxBatchBytes = Math.Max(64 * 1024, MaxBatchBytes);
			int maxBatchCount = Math.Max(1, BatchSize);

#if NET8_0_OR_GREATER
			byte[] sendBuffer = GC.AllocateUninitializedArray<byte>(maxBatchBytes, pinned: true);
#else
			byte[] sendBuffer = new byte[maxBatchBytes];
#endif
			AsyncContinuation[] batchContinuations = new AsyncContinuation[maxBatchCount];

			while (true)
			{
				try
				{
					if (_socket == null || !_socket.Connected)
					{
						CloseSocket();
						Connect();
						Interlocked.Increment(ref _reconnectCount);
					}

					step = 0;

					while (true)
					{
						int tail = _dequeuePos;
						int head = Volatile.Read(ref _enqueuePos);

						if (tail == head)
						{
							// Ring buffer empty - wait for data
							Volatile.Write(ref _consumerState, STATE_SLEEPING);

							// Double check
							if (Volatile.Read(ref _enqueuePos) == tail)
							{
								_dataAvailable.WaitOne(50);
							}

							Volatile.Write(ref _consumerState, STATE_RUNNING);
							continue;
						}

						int available = head - tail;
						int batchLimit = Math.Min(available, maxBatchCount);
						int batchBytes = 0;
						int batchCount = 0;
						bool encounteredClose = false;
						AsyncContinuation closeContinuation = null;
						bool encounteredFlush = false;
						AsyncContinuation flushContinuation = null;

						for (int i = 0; i < batchLimit; i++)
						{
							int pos = tail + i;
							int index = pos & _mask;

							if (Volatile.Read(ref _ring[index].Sequence) != pos + 1)
								break; // Item not yet fully published by producer

							int signal = _ring[index].Signal;
							if (signal == CLOSE_SIGNAL)
							{
								encounteredClose = true;
								closeContinuation = _ring[index].Continuation;
								batchLimit = i + 1;
								break;
							}

							if (signal == FLUSH_SIGNAL)
							{
								encounteredFlush = true;
								flushContinuation = _ring[index].Continuation;
								batchLimit = i + 1;
								break;
							}

							int itemLen = _ring[index].Length;
							if (batchBytes > 0 && batchBytes + itemLen > maxBatchBytes)
							{
								batchLimit = i;
								break;
							}

							batchBytes += itemLen;
							batchCount++;
						}

						if (batchCount == 0 && !encounteredClose && !encounteredFlush)
						{
							Thread.Yield();
							continue;
						}

						// Process and send the batch
						if (batchCount > 0)
						{
							if (batchCount == 1 && _ring[tail & _mask].Buffer != null && _ring[tail & _mask].Length > 0)
							{
								// Single item or large slab: send directly (zero-copy)
								int idx = tail & _mask;
								var buf = _ring[idx].Buffer;
								var off = _ring[idx].Offset;
								var len = _ring[idx].Length;
								var cont = _ring[idx].Continuation;

								Volatile.Write(ref _ring[idx].Sequence, tail + _capacity);
								_dequeuePos = tail + 1;

								if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
									_spaceAvailable.Set();

								try
								{
									SendFully(buf, off, len);
									Interlocked.Increment(ref _sentMessages);
									Interlocked.Add(ref _sentBytes, len);
									Interlocked.Increment(ref _sentBatches);

									cont?.Invoke(null);
								}
								catch (Exception sendEx)
								{
									cont?.Invoke(sendEx);
									throw;
								}
								finally
								{
									ByteBufferPool.Return(buf);
								}
							}
							else
							{
								// Multiple items: coalesce into sendBuffer
								int writeOffset = 0;
								for (int i = 0; i < batchCount; i++)
								{
									int pos = tail + i;
									int idx = pos & _mask;
									var buf = _ring[idx].Buffer;
									var off = _ring[idx].Offset;
									var len = _ring[idx].Length;

									batchContinuations[i] = _ring[idx].Continuation;

									if (buf != null && len > 0)
									{
										Buffer.BlockCopy(buf, off, sendBuffer, writeOffset, len);
										writeOffset += len;
										ByteBufferPool.Return(buf);
									}

									Volatile.Write(ref _ring[idx].Sequence, pos + _capacity);
								}

								_dequeuePos = tail + batchCount;

								if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
									_spaceAvailable.Set();

								try
								{
									SendFully(sendBuffer, 0, writeOffset);

									Interlocked.Add(ref _sentMessages, batchCount);
									Interlocked.Add(ref _sentBytes, writeOffset);
									Interlocked.Increment(ref _sentBatches);

									for (int i = 0; i < batchCount; i++)
										batchContinuations[i]?.Invoke(null);
								}
								catch (Exception sendEx)
								{
									for (int i = 0; i < batchCount; i++)
										batchContinuations[i]?.Invoke(sendEx);
									throw;
								}
							}
						}

						if (encounteredFlush)
						{
							int idx = (tail + batchLimit - 1) & _mask;
							Volatile.Write(ref _ring[idx].Sequence, (tail + batchLimit - 1) + _capacity);
							_dequeuePos = tail + batchLimit;

							if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
								_spaceAvailable.Set();

							flushContinuation?.Invoke(null);
						}

						if (encounteredClose)
						{
							int idx = (tail + batchLimit - 1) & _mask;
							Volatile.Write(ref _ring[idx].Sequence, (tail + batchLimit - 1) + _capacity);
							_dequeuePos = tail + batchLimit;

							CloseSocket();
							closeContinuation?.Invoke(null);
							return; // Thread exit
						}
					}
				}
				catch (Exception ex)
				{
					Interlocked.Increment(ref _sendFailures);
					InternalLogger.Error(ex, "Unexpected error while sending log data to {0}", _uri);

					CloseSocket();

					// Drain and fail pending queue items
					int tail = _dequeuePos;
					int head = Volatile.Read(ref _enqueuePos);
					while (tail < head)
					{
						int idx = tail & _mask;
						if (Volatile.Read(ref _ring[idx].Sequence) == tail + 1)
						{
							var buf = _ring[idx].Buffer;
							var cont = _ring[idx].Continuation;
							int signal = _ring[idx].Signal;

							if (buf != null)
								ByteBufferPool.Return(buf);

							Volatile.Write(ref _ring[idx].Sequence, tail + _capacity);
							_dequeuePos = ++tail;

							if (signal == CLOSE_SIGNAL)
							{
								cont?.Invoke(null);
								return;
							}

							cont?.Invoke(ex);
						}
						else
						{
							break;
						}
					}

					var delay = GetDelay(step++);
					Thread.Sleep(delay);
				}
			}
		}

		#endregion
	}
}
