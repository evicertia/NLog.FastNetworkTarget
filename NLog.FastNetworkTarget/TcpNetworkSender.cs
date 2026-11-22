using System;
using System.Threading;
using System.Diagnostics;
using System.Net.Sockets;
using System.Collections;
using System.Security.Authentication;

using NLog.Common;
using NLog.Targets;

namespace NLog.Internal.NetworkSenders
{
	internal class TcpNetworkSender : NetworkSender
	{
		#region Constants & Flags

		private const byte FLAG_DATA = 0;
		private const byte FLAG_POOLED = 1;
		private const byte FLAG_FLUSH = 2;
		private const byte FLAG_CLOSE = 4;

		private const int STATE_RUNNING = 0;
		private const int STATE_SLEEPING = 1;

		private const int MAX_RING_CAPACITY = 1 << 20; // 1,048,576 slots max

		#endregion

		#region Ring Buffer Slot Structure

		private struct Slot
		{
			public int Sequence;
			public byte[] Buffer;
			public int Offset;
			public int Length;
			public int EventCount;
			public object Continuation;
			public byte Flags;
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
		private readonly ManualResetEventSlim _closedCompleted = new ManualResetEventSlim(false);

		private volatile bool _isClosed;
		private int _closeSignaled;
		private volatile Thread _thread;
		private ISocket _socket;
		private Uri _uri;
		private AddressFamily _resolvedAddressFamily;
		private bool? _enableKeepAliveSuccessful;

		// Bounded MPSC Ring Buffer (padded to avoid false sharing between producers and single consumer)
		private Slot[] _ring;
		private int _capacity;
		private int _mask;

#pragma warning disable CS0169
		private long _pad0_0, _pad0_1, _pad0_2, _pad0_3, _pad0_4, _pad0_5, _pad0_6, _pad0_7;
		private int _enqueuePos;
		private long _pad1_0, _pad1_1, _pad1_2, _pad1_3, _pad1_4, _pad1_5, _pad1_6, _pad1_7;
		private int _dequeuePos;
		private long _pad2_0, _pad2_1, _pad2_2, _pad2_3, _pad2_4, _pad2_5, _pad2_6, _pad2_7;
		private int _consumerState;
		private long _pad3_0, _pad3_1, _pad3_2, _pad3_3, _pad3_4, _pad3_5, _pad3_6, _pad3_7;
#pragma warning restore CS0169

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
			_resolvedAddressFamily = addressFamily;
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

						int clampedQueueSize = MaxQueueSize <= 0 ? 8192 : Math.Min(MaxQueueSize, MAX_RING_CAPACITY);
						int cap = 16;
						while (cap < clampedQueueSize)
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

		internal void SendPooled(byte[] pooledBuffer, int offset, int length, int eventCount, object continuation)
		{
			if (_isClosed)
			{
				ByteBufferPool.Return(pooledBuffer);
				InvokeContinuation(continuation, new InvalidOperationException("NetworkSender is closed."));
				return;
			}

			EnqueueItem(pooledBuffer, offset, length, eventCount, continuation, FLAG_POOLED);
		}

		protected override void DoSend(byte[] bytes, int offset, int length, AsyncContinuation asyncContinuation)
		{
			if (_isClosed)
			{
				asyncContinuation?.Invoke(new InvalidOperationException("NetworkSender is closed."));
				return;
			}

			EnqueueItem(bytes, offset, length, 1, asyncContinuation, FLAG_DATA);
		}

		protected override void DoFlush(AsyncContinuation continuation)
		{
			if (_isClosed)
			{
				continuation?.Invoke(null);
				return;
			}

			EnqueueItem(null, 0, 0, 0, continuation, FLAG_FLUSH);
		}

		protected override void DoClose(AsyncContinuation continuation)
		{
			if (_thread == null)
			{
				_isClosed = true;
				_closedCompleted.Set();
				continuation?.Invoke(null);
				return;
			}

			if (Interlocked.Exchange(ref _closeSignaled, 1) != 0)
			{
				continuation?.Invoke(null);
				return;
			}

			_isClosed = true;
			EnqueueItem(null, 0, 0, 0, continuation, FLAG_CLOSE);
			TrySetEvent(_dataAvailable);
			TrySetEvent(_spaceAvailable);
		}

		internal bool WaitForClose(int millisecondsTimeout)
		{
			if (_thread == null)
				return true;

			return _closedCompleted.Wait(millisecondsTimeout);
		}

		#endregion

		#region Helpers

		private static void InvokeContinuation(object continuation, Exception ex)
		{
			if (continuation == null)
				return;

			if (continuation is AsyncContinuation single)
			{
				single(ex);
				return;
			}

			if (continuation is AsyncContinuation[] array)
			{
				for (int i = 0; i < array.Length; i++)
					array[i]?.Invoke(ex);
			}
		}

		private static void ReturnIfPooled(byte[] buffer, byte flags)
		{
			if (buffer != null && (flags & FLAG_POOLED) != 0)
				ByteBufferPool.Return(buffer);
		}

		private static void TrySetEvent(AutoResetEvent evt)
		{
			try { evt.Set(); }
			catch (ObjectDisposedException) { }
		}

		private static void TryWaitEvent(AutoResetEvent evt, int timeoutMs)
		{
			try { evt.WaitOne(timeoutMs); }
			catch (ObjectDisposedException) { }
		}

		#endregion

		#region Enqueue Logic

		private void EnqueueItem(byte[] bytes, int offset, int length, int eventCount, object continuation, byte flags)
		{
			bool isClose = (flags & FLAG_CLOSE) != 0;

			while (!_isClosed || isClose)
			{
				int head = Volatile.Read(ref _enqueuePos);
				int tail = Volatile.Read(ref _dequeuePos);

				if (head - tail >= _capacity)
				{
					if (isClose)
					{
						Thread.Sleep(1);
						continue;
					}

					if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Discard)
					{
						if (eventCount > 0)
							Interlocked.Add(ref _droppedMessages, eventCount);

						ReturnIfPooled(bytes, flags);
						InvokeContinuation(continuation, null);
						return;
					}

					// Block until space becomes available or sender closes
					Interlocked.Increment(ref _blockedEnqueues);
					TryWaitEvent(_spaceAvailable, 50);
					continue;
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
					_ring[index].EventCount = eventCount;
					_ring[index].Continuation = continuation;
					_ring[index].Flags = flags;

					Volatile.Write(ref _ring[index].Sequence, head + 1);

					if (Volatile.Read(ref _consumerState) == STATE_SLEEPING &&
						Interlocked.CompareExchange(ref _consumerState, STATE_RUNNING, STATE_SLEEPING) == STATE_SLEEPING)
					{
						TrySetEvent(_dataAvailable);
					}

					return;
				}
			}

			// Sender closed concurrently
			ReturnIfPooled(bytes, flags);
			InvokeContinuation(continuation, new InvalidOperationException("NetworkSender is closed."));
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
#if NET8_0_OR_GREATER
				if (TrySetSocketOption(socket, SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, keepAliveTimeSeconds))
				{
					TrySetSocketOption(socket, SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
					return true;
				}
#else
				var tcpKeepAliveTime = (SocketOptionName)0x3;
				var tcpKeepAliveInterval = (SocketOptionName)0x11;

				if (PlatformDetector.CurrentOS == RuntimeOS.Linux)
				{
					tcpKeepAliveTime = (SocketOptionName)0x4;
					tcpKeepAliveInterval = (SocketOptionName)0x5;
				}
				else if (PlatformDetector.CurrentOS == RuntimeOS.MacOSX)
				{
					tcpKeepAliveTime = (SocketOptionName)0x10;
					tcpKeepAliveInterval = (SocketOptionName)0x101;
				}

				if (TrySetSocketOption(socket, SocketOptionLevel.Tcp, tcpKeepAliveTime, keepAliveTimeSeconds))
				{
					TrySetSocketOption(socket, SocketOptionLevel.Tcp, tcpKeepAliveInterval, 1);
					return true;
				}
#endif
			}

			return false;
		}

		protected internal virtual ISocket CreateSocket(string host, AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType)
		{
			var socketAf = addressFamily == AddressFamily.Unspecified
				? (_resolvedAddressFamily != AddressFamily.Unspecified ? _resolvedAddressFamily : AddressFamily.InterNetwork)
				: addressFamily;

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
			var endpoint = ParseEndpointAddress(_uri, AddressFamily);
			_resolvedAddressFamily = (AddressFamily == AddressFamily.Unspecified &&
				(endpoint.AddressFamily == AddressFamily.InterNetwork || endpoint.AddressFamily == AddressFamily.InterNetworkV6))
				? endpoint.AddressFamily
				: AddressFamily;

			_socket = CreateSocket(_uri.Host, AddressFamily, SocketType.Stream, ProtocolType.Tcp);

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

		private static readonly bool _isMultiProcessor = Environment.ProcessorCount > 1;

		private void WaitForNextPublishedSlot(int tail)
		{
			while (true)
			{
				if (Volatile.Read(ref _enqueuePos) != tail)
				{
					if (Volatile.Read(ref _ring[tail & _mask].Sequence) == tail + 1)
						return;

					Thread.Yield();
					continue;
				}

				if (_isMultiProcessor)
				{
					for (int s = 0; s < 3 && Volatile.Read(ref _enqueuePos) == tail; s++)
						Thread.SpinWait(4);

					if (Volatile.Read(ref _enqueuePos) != tail)
						continue;
				}

				Interlocked.Exchange(ref _consumerState, STATE_SLEEPING);

				if (Volatile.Read(ref _enqueuePos) == tail)
					TryWaitEvent(_dataAvailable, 50);

				Volatile.Write(ref _consumerState, STATE_RUNNING);
			}
		}

		private void Main()
		{
			uint step = 0;

			int maxBatchBytes = Math.Max(64 * 1024, MaxBatchBytes);
			int maxBatchCount = Math.Max(1, BatchSize);

#if NET8_0_OR_GREATER
			byte[] sendBuffer = GC.AllocateUninitializedArray<byte>(maxBatchBytes, pinned: true);
#else
			byte[] sendBuffer = new byte[maxBatchBytes];
#endif
			object[] batchContinuations = new object[maxBatchCount];

			try
			{
				while (true)
				{
					try
					{
						// Wait lazily for work before attempting socket connection
						int initialTail = _dequeuePos;
						WaitForNextPublishedSlot(initialTail);

						int initialIdx = initialTail & _mask;
						byte initialFlags = _ring[initialIdx].Flags;

						if ((initialFlags & FLAG_CLOSE) != 0)
						{
							var closeCont = _ring[initialIdx].Continuation;
							_ring[initialIdx].Buffer = null;
							_ring[initialIdx].Continuation = null;
							Volatile.Write(ref _ring[initialIdx].Sequence, initialTail + _capacity);
							_dequeuePos = initialTail + 1;

							CloseSocket();
							InvokeContinuation(closeCont, null);
							return;
						}

						if ((initialFlags & FLAG_FLUSH) != 0 && (_socket == null || !_socket.Connected))
						{
							var flushCont = _ring[initialIdx].Continuation;
							_ring[initialIdx].Buffer = null;
							_ring[initialIdx].Continuation = null;
							Volatile.Write(ref _ring[initialIdx].Sequence, initialTail + _capacity);
							_dequeuePos = initialTail + 1;

							if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
								TrySetEvent(_spaceAvailable);

							InvokeContinuation(flushCont, null);
							continue;
						}

						if (_socket == null || !_socket.Connected)
						{
							CloseSocket();
							Connect();
							Volatile.Write(ref _reconnectCount, _reconnectCount + 1);
						}

						step = 0;

						while (true)
						{
							int tail = _dequeuePos;
							WaitForNextPublishedSlot(tail);

							int head = Volatile.Read(ref _enqueuePos);
							int available = head - tail;
							int batchLimit = Math.Min(available, maxBatchCount);
							int batchBytes = 0;
							int batchCount = 0;
							bool encounteredClose = false;
							object closeContinuation = null;
							bool encounteredFlush = false;
							object flushContinuation = null;

							for (int i = 0; i < batchLimit; i++)
							{
								int pos = tail + i;
								int index = pos & _mask;

								if (Volatile.Read(ref _ring[index].Sequence) != pos + 1)
									break;

								byte flags = _ring[index].Flags;
								if ((flags & FLAG_CLOSE) != 0)
								{
									encounteredClose = true;
									closeContinuation = _ring[index].Continuation;
									batchLimit = i + 1;
									break;
								}

								if ((flags & FLAG_FLUSH) != 0)
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

							if (batchCount > 0)
							{
								int firstIdx = tail & _mask;
								if (batchCount == 1)
								{
									// Single item or 64KB slab: send directly (zero-copy)
									var buf = _ring[firstIdx].Buffer;
									var off = _ring[firstIdx].Offset;
									var len = _ring[firstIdx].Length;
									var evCount = _ring[firstIdx].EventCount;
									var cont = _ring[firstIdx].Continuation;
									var flags = _ring[firstIdx].Flags;

									_ring[firstIdx].Buffer = null;
									_ring[firstIdx].Continuation = null;
									Volatile.Write(ref _ring[firstIdx].Sequence, tail + _capacity);
									_dequeuePos = tail + 1;

									if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
										TrySetEvent(_spaceAvailable);

									try
									{
										if (buf != null && len > 0)
											SendFully(buf, off, len);

										if (evCount > 0)
											Volatile.Write(ref _sentMessages, _sentMessages + evCount);
										if (len > 0)
										{
											Volatile.Write(ref _sentBytes, _sentBytes + len);
											Volatile.Write(ref _sentBatches, _sentBatches + 1);
										}

										InvokeContinuation(cont, null);
									}
									catch (Exception sendEx)
									{
										InvokeContinuation(cont, sendEx);
										throw;
									}
									finally
									{
										ReturnIfPooled(buf, flags);
									}
								}
								else
								{
									// Multiple items: coalesce into sendBuffer and return pooled buffers before syscall
									int writeOffset = 0;
									long batchEvents = 0;

									for (int i = 0; i < batchCount; i++)
									{
										int pos = tail + i;
										int idx = pos & _mask;
										var buf = _ring[idx].Buffer;
										var off = _ring[idx].Offset;
										var len = _ring[idx].Length;
										var flags = _ring[idx].Flags;

										batchEvents += _ring[idx].EventCount;
										batchContinuations[i] = _ring[idx].Continuation;

										if (buf != null && len > 0)
										{
											Buffer.BlockCopy(buf, off, sendBuffer, writeOffset, len);
											writeOffset += len;
										}

										ReturnIfPooled(buf, flags);

										_ring[idx].Buffer = null;
										_ring[idx].Continuation = null;
										Volatile.Write(ref _ring[idx].Sequence, pos + _capacity);
									}

									_dequeuePos = tail + batchCount;

									if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
										TrySetEvent(_spaceAvailable);

									try
									{
										if (writeOffset > 0)
											SendFully(sendBuffer, 0, writeOffset);

										if (batchEvents > 0)
											Volatile.Write(ref _sentMessages, _sentMessages + batchEvents);
										if (writeOffset > 0)
										{
											Volatile.Write(ref _sentBytes, _sentBytes + writeOffset);
											Volatile.Write(ref _sentBatches, _sentBatches + 1);
										}

										for (int i = 0; i < batchCount; i++)
											InvokeContinuation(batchContinuations[i], null);
									}
									catch (Exception sendEx)
									{
										for (int i = 0; i < batchCount; i++)
											InvokeContinuation(batchContinuations[i], sendEx);
										throw;
									}
									finally
									{
										Array.Clear(batchContinuations, 0, batchCount);
									}
								}
							}

							if (encounteredFlush)
							{
								int flushPos = tail + batchLimit - 1;
								int idx = flushPos & _mask;
								_ring[idx].Buffer = null;
								_ring[idx].Continuation = null;
								Volatile.Write(ref _ring[idx].Sequence, flushPos + _capacity);
								_dequeuePos = tail + batchLimit;

								if (OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
									TrySetEvent(_spaceAvailable);

								InvokeContinuation(flushContinuation, null);
							}

							if (encounteredClose)
							{
								int closePos = tail + batchLimit - 1;
								int idx = closePos & _mask;
								_ring[idx].Buffer = null;
								_ring[idx].Continuation = null;
								Volatile.Write(ref _ring[idx].Sequence, closePos + _capacity);
								_dequeuePos = tail + batchLimit;

								CloseSocket();
								InvokeContinuation(closeContinuation, null);
								return;
							}
						}
					}
					catch (Exception ex)
					{
						Interlocked.Increment(ref _sendFailures);
						InternalLogger.Error(ex, "Unexpected error while sending log data to {0}", _uri);

						CloseSocket();

						// Wraparound-safe drain and fail of pending/arriving queue items during backoff window
						var delay = GetDelay(step++);
						long deadline = Stopwatch.GetTimestamp() + (long)(delay * (Stopwatch.Frequency / 1000.0));

						while (true)
						{
							int tail = _dequeuePos;
							int head = Volatile.Read(ref _enqueuePos);
							bool drainedAny = false;

							while (head - tail > 0)
							{
								int idx = tail & _mask;
								if (Volatile.Read(ref _ring[idx].Sequence) != tail + 1)
									break;

								var buf = _ring[idx].Buffer;
								var cont = _ring[idx].Continuation;
								byte flags = _ring[idx].Flags;

								_ring[idx].Buffer = null;
								_ring[idx].Continuation = null;
								ReturnIfPooled(buf, flags);

								Volatile.Write(ref _ring[idx].Sequence, tail + _capacity);
								_dequeuePos = ++tail;
								drainedAny = true;

								if ((flags & FLAG_CLOSE) != 0)
								{
									InvokeContinuation(cont, null);
									return;
								}

								if ((flags & FLAG_FLUSH) != 0)
									InvokeContinuation(cont, null);
								else
									InvokeContinuation(cont, ex);

								head = Volatile.Read(ref _enqueuePos);
							}

							if (drainedAny && OnQueueOverflow == NetworkTargetQueueOverflowAction.Block)
								TrySetEvent(_spaceAvailable);

							if (_isClosed && Volatile.Read(ref _enqueuePos) == _dequeuePos)
								break;

							long remainingTicks = deadline - Stopwatch.GetTimestamp();
							if (remainingTicks <= 0)
								break;

							int waitMs = Math.Max(1, (int)(remainingTicks * 1000 / Stopwatch.Frequency));
							Interlocked.Exchange(ref _consumerState, STATE_SLEEPING);
							if (Volatile.Read(ref _enqueuePos) == _dequeuePos && !_isClosed)
								TryWaitEvent(_dataAvailable, waitMs);
							Volatile.Write(ref _consumerState, STATE_RUNNING);
						}
					}
				}
			}
			finally
			{
				CloseSocket();
				_closedCompleted.Set();
				try { _dataAvailable.Dispose(); } catch { }
				try { _spaceAvailable.Dispose(); } catch { }
			}
		}

		#endregion
	}
}
