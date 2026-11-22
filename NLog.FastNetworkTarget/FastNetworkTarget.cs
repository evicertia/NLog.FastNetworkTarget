using System;
using System.Text;
using System.Threading;
using System.Reflection;
using System.Net.Sockets;
using System.ComponentModel;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Security.Authentication;

using NLog.Common;
using NLog.Layouts;
using NLog.Internal;
using NLog.Internal.NetworkSenders;

namespace NLog.Targets
{
	[Target("FastNetwork")]
	public class FastNetworkTarget : TargetWithLayout
	{
		#region Delegates for Internal NLog Methods

		private delegate bool TryGetCachedLayoutValueDelegate(LogEventInfo logEvent, Layout layout, out object text);
		private delegate void RenderAppendBuilderDelegate(Layout layout, LogEventInfo logEvent, StringBuilder target, bool cacheLayoutResult);

		private static readonly TryGetCachedLayoutValueDelegate _tryGetCachedLayoutValueFn;
		private static readonly RenderAppendBuilderDelegate _renderAppendBuilderFn;
		private static readonly MethodInfo _tryGetCachedLayoutValueMethod;
		private static readonly MethodInfo _renderAppendBuilderMethod;
		private static readonly AsyncContinuation _completedContinuation;

		static FastNetworkTarget()
		{
			try
			{
				_tryGetCachedLayoutValueMethod = typeof(LogEventInfo).GetMethod("TryGetCachedLayoutValue", BindingFlags.NonPublic | BindingFlags.Instance);
				if (_tryGetCachedLayoutValueMethod != null)
					_tryGetCachedLayoutValueFn = (TryGetCachedLayoutValueDelegate)Delegate.CreateDelegate(typeof(TryGetCachedLayoutValueDelegate), _tryGetCachedLayoutValueMethod);
			}
			catch (Exception ex)
			{
				InternalLogger.Warn(ex, "FastNetworkTarget: Could not bind TryGetCachedLayoutValue delegate.");
			}

			try
			{
				_renderAppendBuilderMethod = typeof(Layout).GetMethod("RenderAppendBuilder", BindingFlags.NonPublic | BindingFlags.Instance);
				if (_renderAppendBuilderMethod != null)
					_renderAppendBuilderFn = (RenderAppendBuilderDelegate)Delegate.CreateDelegate(typeof(RenderAppendBuilderDelegate), _renderAppendBuilderMethod);
			}
			catch (Exception ex)
			{
				InternalLogger.Warn(ex, "FastNetworkTarget: Could not bind RenderAppendBuilder delegate.");
			}

			try
			{
				var singleCallType = typeof(AsyncHelpers).Assembly.GetType("NLog.Internal.SingleCallContinuation");
				var completedField = singleCallType?.GetField("Completed", BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Public);
				_completedContinuation = completedField?.GetValue(null) as AsyncContinuation;
			}
			catch
			{
				// Ignore
			}
		}

		#endregion

		#region Fields & Properties

		// Retained for backward compatibility with Herma.Facilities NLogStatsFetcher reflection
		private readonly IDictionary<string, LinkedListNode<NetworkSender>> _currentSenderCache = new Dictionary<string, LinkedListNode<NetworkSender>>();
		private readonly LinkedList<NetworkSender> _openNetworkSenders = new LinkedList<NetworkSender>();

		private readonly ConcurrentDictionary<string, TcpNetworkSender> _senderMap = new ConcurrentDictionary<string, TcpNetworkSender>(StringComparer.OrdinalIgnoreCase);
		private TcpNetworkSender _fixedSender;
		private byte[] _newLineBytes;

		[ThreadStatic]
		private static StringBuilder t_threadBuilder;

		[ThreadStatic]
		private static char[] t_threadCharBuffer;

		/// <summary>
		/// Gets or sets the network address.
		/// </summary>
		public Layout Address { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether to keep connection open whenever possible.
		/// </summary>
		[DefaultValue(true)]
		public bool KeepConnection { get; set; } = true;

		/// <summary>
		/// Gets or sets a value indicating whether to append newline at the end of log message.
		/// </summary>
		[DefaultValue(false)]
		public bool NewLine { get; set; }

		/// <summary>
		/// Gets or sets the end of line value if a newline is appended at the end of log message <see cref="NewLine"/>.
		/// </summary>
		[DefaultValue("CRLF")]
		public LineEndingMode LineEnding { get; set; } = LineEndingMode.CRLF;

		/// <summary>
		/// Gets or sets the maximum message size in bytes.
		/// </summary>
		[DefaultValue(65000)]
		public int MaxMessageSize { get; set; } = 65000;

		/// <summary>
		/// Gets or sets the size of the connection cache (number of connections which are kept alive).
		/// </summary>
		[DefaultValue(5)]
		public int ConnectionCacheSize { get; set; } = 5;

		/// <summary>
		/// Gets or sets the maximum current connections. 0 = no maximum.
		/// </summary>
		public int MaxConnections { get; set; }

		/// <summary>
		/// Gets or sets the action that should be taken if there will be more connections than <see cref="MaxConnections"/>.
		/// </summary>
		public NetworkTargetConnectionsOverflowAction OnConnectionOverflow { get; set; }

		/// <summary>
		/// Gets or sets the maximum queue size.
		/// </summary>
		[DefaultValue(8192)]
		public int MaxQueueSize { get; set; } = 8192;

		/// <summary>
		/// Gets or sets the action that should be taken if the message is larger than maxMessageSize.
		/// </summary>
		[DefaultValue(NetworkTargetOverflowAction.Split)]
		public NetworkTargetOverflowAction OnOverflow { get; set; } = NetworkTargetOverflowAction.Split;

		/// <summary>
		/// Gets or sets the encoding to be used.
		/// </summary>
		[DefaultValue("utf-8")]
		public Encoding Encoding { get; set; } = Encoding.UTF8;

		/// <summary>
		/// Get or set the SSL/TLS protocols.
		/// </summary>
		public SslProtocols SslProtocols { get; set; } = SslProtocols.None;

		/// <summary>
		/// The number of milliseconds a connection will remain idle before the first keep-alive probe is sent.
		/// </summary>
		public int KeepAliveTime { get; set; }

		/// <summary>
		/// Get or set the timeout in milliseconds for socket connection.
		/// </summary>
		[DefaultValue(1000)]
		public int ConnectionTimeout { get; set; } = 1000;

		/// <summary>
		/// Gets or sets the action to take when the internal sender queue is full.
		/// Defaults to <see cref="NetworkTargetQueueOverflowAction.Discard"/> to avoid blocking producer threads.
		/// </summary>
		[DefaultValue(NetworkTargetQueueOverflowAction.Discard)]
		public NetworkTargetQueueOverflowAction OnQueueOverflow { get; set; } = NetworkTargetQueueOverflowAction.Discard;

		/// <summary>
		/// Gets or sets the maximum number of items to coalesce per socket write.
		/// </summary>
		[DefaultValue(512)]
		public int BatchSize { get; set; } = 512;

		/// <summary>
		/// Gets or sets the maximum coalesced batch size in bytes.
		/// </summary>
		[DefaultValue(256 * 1024)]
		public int MaxBatchBytes { get; set; } = 256 * 1024;

		/// <summary>
		/// Gets or sets whether TCP_NODELAY is enabled.
		/// </summary>
		[DefaultValue(true)]
		public bool NoDelay { get; set; } = true;

		/// <summary>
		/// Gets or sets the socket send timeout in milliseconds.
		/// </summary>
		[DefaultValue(30000)]
		public int SendTimeout { get; set; } = 30000;

		/// <summary>
		/// Gets or sets the socket send buffer size in bytes.
		/// </summary>
		[DefaultValue(256 * 1024)]
		public int SendBufferSize { get; set; } = 256 * 1024;

		#endregion

		#region Observability Metrics

		public long PendingMessages
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.PendingMessages;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.PendingMessages;
				return total;
			}
		}

		public int ConnectionCount => _fixedSender != null ? 1 : _senderMap.Count;

		public long SentMessages
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.SentMessages;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.SentMessages;
				return total;
			}
		}

		public long SentBytes
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.SentBytes;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.SentBytes;
				return total;
			}
		}

		public long SentBatches
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.SentBatches;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.SentBatches;
				return total;
			}
		}

		public long DroppedMessages
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.DroppedMessages;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.DroppedMessages;
				return total;
			}
		}

		public long BlockedEnqueues
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.BlockedEnqueues;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.BlockedEnqueues;
				return total;
			}
		}

		public long SendFailures
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.SendFailures;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.SendFailures;
				return total;
			}
		}

		public long ReconnectCount
		{
			get
			{
				if (_fixedSender != null)
					return _fixedSender.ReconnectCount;

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.ReconnectCount;
				return total;
			}
		}

		#endregion

		#region .ctors

		public FastNetworkTarget()
		{
			OptimizeBufferReuse = true;
		}

		public FastNetworkTarget(string name)
			: this()
		{
			Name = name;
		}

		#endregion

		#region Initialization & Teardown

		protected override void InitializeTarget()
		{
			base.InitializeTarget();

			_newLineBytes = NewLine && LineEnding != null
				? Encoding.GetBytes(LineEnding.NewLineCharacters)
				: Array.Empty<byte>();

			// Fast-path: detect static/fixed addresses at initialization time
			if (Address is SimpleLayout simple && simple.IsFixedText)
			{
				var addr = simple.FixedText;
				if (!string.IsNullOrWhiteSpace(addr))
					_fixedSender = GetOrCreateSender(addr);
			}
		}

		protected override void CloseTarget()
		{
			base.CloseTarget();

			_fixedSender?.Close(ex => { });
			_fixedSender = null;

			foreach (var pair in _senderMap)
				pair.Value.Close(ex => { });

			_senderMap.Clear();

			lock (_openNetworkSenders)
			{
				_openNetworkSenders.Clear();
			}

			lock (_currentSenderCache)
			{
				_currentSenderCache.Clear();
			}
		}

		protected override void FlushAsync(AsyncContinuation asyncContinuation)
		{
			var senders = new List<TcpNetworkSender>();
			if (_fixedSender != null)
			{
				senders.Add(_fixedSender);
			}
			else
			{
				senders.AddRange(_senderMap.Values);
			}

			if (senders.Count == 0)
			{
				asyncContinuation(null);
				return;
			}

			int remaining = senders.Count;
			void Continuation(Exception ex)
			{
				if (Interlocked.Decrement(ref remaining) == 0)
					asyncContinuation(null);
			}

			foreach (var sender in senders)
				sender.FlushAsync(Continuation);
		}

		#endregion

		#region Sender Management

		private static AddressFamily DetectAddressFamily(string url)
		{
			if (url.StartsWith("tcp4://", StringComparison.OrdinalIgnoreCase))
				return AddressFamily.InterNetwork;

			if (url.StartsWith("tcp6://", StringComparison.OrdinalIgnoreCase))
				return AddressFamily.InterNetworkV6;

			return AddressFamily.Unspecified;
		}

		private TcpNetworkSender CreateTcpSender(string url)
		{
			var af = DetectAddressFamily(url);
			var sender = new TcpNetworkSender(url, af)
			{
				ConnectionTimeout = TimeSpan.FromMilliseconds(ConnectionTimeout),
				KeepAliveTime = TimeSpan.FromMilliseconds(KeepAliveTime),
				SslProtocols = SslProtocols,
				MaxQueueSize = MaxQueueSize,
				NoDelay = NoDelay,
				SendTimeout = SendTimeout,
				SendBufferSize = SendBufferSize,
				BatchSize = BatchSize,
				MaxBatchBytes = MaxBatchBytes,
				OnQueueOverflow = OnQueueOverflow
			};

			sender.Initialize();

			// Populate legacy reflection structures for NLogStatsFetcher backward compatibility
			lock (_openNetworkSenders)
			{
				var node = _openNetworkSenders.AddLast(sender);
				lock (_currentSenderCache)
				{
					_currentSenderCache[url] = node;
				}
			}

			return sender;
		}

		private TcpNetworkSender GetOrCreateSender(string address)
		{
			if (_fixedSender != null)
				return _fixedSender;

			if (_senderMap.TryGetValue(address, out var sender))
				return sender;

			lock (_currentSenderCache)
			{
				return _senderMap.GetOrAdd(address, CreateTcpSender);
			}
		}

		#endregion

		#region Layout Rendering & Encoding Helpers

		private static bool TryGetCachedLayoutValue(LogEventInfo logEvent, Layout layout, out object text)
		{
			if (_tryGetCachedLayoutValueFn != null)
				return _tryGetCachedLayoutValueFn(logEvent, layout, out text);

			if (_tryGetCachedLayoutValueMethod != null)
			{
				var args = new[] { layout, (object)null };
				var result = (bool)_tryGetCachedLayoutValueMethod.Invoke(logEvent, args);
				text = args[1];
				return result;
			}

			text = null;
			return false;
		}

		private static void RenderAppendBuilder(Layout layout, LogEventInfo logEvent, StringBuilder target, bool cacheLayoutResult = false)
		{
			if (_renderAppendBuilderFn != null)
			{
				_renderAppendBuilderFn(layout, logEvent, target, cacheLayoutResult);
				return;
			}

			if (_renderAppendBuilderMethod != null)
			{
				_renderAppendBuilderMethod.Invoke(layout, new object[] { logEvent, target, cacheLayoutResult });
				return;
			}

			target.Append(layout.Render(logEvent));
		}

		private int EncodeLogEvent(LogEventInfo logEvent, byte[] destBuffer, int destOffset)
		{
			int maxAvailable = destBuffer.Length - destOffset - _newLineBytes.Length;
			if (maxAvailable <= 0)
				return -1;

			if (TryGetCachedLayoutValue(logEvent, Layout, out var cachedText) && cachedText != null)
			{
				string str = cachedText.ToString();
				int byteCount = Encoding.GetByteCount(str);
				if (byteCount > maxAvailable)
					return -1;

				int written = Encoding.GetBytes(str, 0, str.Length, destBuffer, destOffset);
				if (_newLineBytes.Length > 0)
				{
					Buffer.BlockCopy(_newLineBytes, 0, destBuffer, destOffset + written, _newLineBytes.Length);
					written += _newLineBytes.Length;
				}
				return written;
			}

			var sb = t_threadBuilder ?? (t_threadBuilder = new StringBuilder(1024));
			sb.Clear();

			RenderAppendBuilder(Layout, logEvent, sb, false);

			int charLen = sb.Length;
			var charBuf = t_threadCharBuffer;
			if (charBuf == null || charBuf.Length < charLen)
				t_threadCharBuffer = charBuf = new char[Math.Max(charLen, 2048)];

			sb.CopyTo(0, charBuf, 0, charLen);

			int reqBytes = Encoding.GetByteCount(charBuf, 0, charLen);
			if (reqBytes > maxAvailable)
				return -1;

			int enc = Encoding.GetBytes(charBuf, 0, charLen, destBuffer, destOffset);
			if (_newLineBytes.Length > 0)
			{
				Buffer.BlockCopy(_newLineBytes, 0, destBuffer, destOffset + enc, _newLineBytes.Length);
				enc += _newLineBytes.Length;
			}
			return enc;
		}

		private byte[] RenderToRentedBuffer(LogEventInfo logEvent, out int totalLength)
		{
			if (TryGetCachedLayoutValue(logEvent, Layout, out var cachedText) && cachedText != null)
			{
				string str = cachedText.ToString();
				int byteCount = Encoding.GetByteCount(str);
				int total = byteCount + _newLineBytes.Length;

				byte[] buf = ByteBufferPool.Rent(total);
				int enc = Encoding.GetBytes(str, 0, str.Length, buf, 0);

				if (_newLineBytes.Length > 0)
					Buffer.BlockCopy(_newLineBytes, 0, buf, enc, _newLineBytes.Length);

				totalLength = total;
				return buf;
			}

			var sb = t_threadBuilder ?? (t_threadBuilder = new StringBuilder(1024));
			sb.Clear();

			RenderAppendBuilder(Layout, logEvent, sb, false);

			int charLen = sb.Length;
			var charBuf = t_threadCharBuffer;
			if (charBuf == null || charBuf.Length < charLen)
				t_threadCharBuffer = charBuf = new char[Math.Max(charLen, 2048)];

			sb.CopyTo(0, charBuf, 0, charLen);

			int reqBytes = Encoding.GetByteCount(charBuf, 0, charLen);
			int fullTotal = reqBytes + _newLineBytes.Length;

			byte[] rented = ByteBufferPool.Rent(fullTotal);
			int written = Encoding.GetBytes(charBuf, 0, charLen, rented, 0);

			if (_newLineBytes.Length > 0)
				Buffer.BlockCopy(_newLineBytes, 0, rented, written, _newLineBytes.Length);

			totalLength = fullTotal;
			return rented;
		}

		#endregion

		#region High-Performance ThreadSafe Overrides (Lock-Free Hot Path)

		protected override void WriteAsyncThreadSafe(AsyncLogEventInfo logEvent)
		{
			if (!IsInitialized)
			{
				logEvent.Continuation(null);
				return;
			}

			Write(logEvent);
		}

		protected override void WriteAsyncThreadSafe(IList<AsyncLogEventInfo> logEvents)
		{
			if (!IsInitialized)
			{
				for (int i = 0; i < logEvents.Count; i++)
					logEvents[i].Continuation(null);
				return;
			}

			Write(logEvents);
		}

		#endregion

		#region Write Implementation

		protected override void Write(AsyncLogEventInfo logEvent)
		{
			TcpNetworkSender sender;
			if (_fixedSender != null)
			{
				sender = _fixedSender;
			}
			else
			{
				var address = RenderLogEvent(Address, logEvent.LogEvent);
				if (string.IsNullOrWhiteSpace(address))
				{
					logEvent.Continuation(null);
					return;
				}
				sender = GetOrCreateSender(address);
			}

			byte[] buf = RenderToRentedBuffer(logEvent.LogEvent, out int len);

			// Check MaxMessageSize overflow
			if (len > MaxMessageSize)
			{
				if (OnOverflow == NetworkTargetOverflowAction.Discard)
				{
					ByteBufferPool.Return(buf);
					logEvent.Continuation(null);
					return;
				}

				if (OnOverflow == NetworkTargetOverflowAction.Error)
				{
					ByteBufferPool.Return(buf);
					logEvent.Continuation(new OverflowException($"Attempted to send a message larger than MaxMessageSize ({MaxMessageSize}). Actual size was: {len}."));
					return;
				}

				// Split: send in MaxMessageSize chunks
				int pos = 0;
				int toSend = len;
				while (toSend > 0)
				{
					int chunkSize = Math.Min(toSend, MaxMessageSize);
					byte[] chunkBuf = ByteBufferPool.Rent(chunkSize);
					Buffer.BlockCopy(buf, pos, chunkBuf, 0, chunkSize);

					pos += chunkSize;
					toSend -= chunkSize;

					var cont = toSend == 0 ? logEvent.Continuation : null;
					sender.Send(chunkBuf, 0, chunkSize, cont);
				}

				ByteBufferPool.Return(buf);
				return;
			}

			sender.Send(buf, 0, len, logEvent.Continuation);
		}

		protected override void Write(IList<AsyncLogEventInfo> logEvents)
		{
			int count = logEvents.Count;
			if (count == 0)
				return;

			if (count == 1)
			{
				Write(logEvents[0]);
				return;
			}

			// Producer-side Slab Coalescing into 64KB buffers
			TcpNetworkSender sender = _fixedSender;
			byte[] slab = null;
			int slabOffset = 0;

			for (int i = 0; i < count; i++)
			{
				var asyncEvent = logEvents[i];

				if (sender == null)
				{
					var address = RenderLogEvent(Address, asyncEvent.LogEvent);
					if (string.IsNullOrWhiteSpace(address))
					{
						asyncEvent.Continuation(null);
						continue;
					}
					sender = GetOrCreateSender(address);
				}

				if (slab == null)
				{
					slab = ByteBufferPool.Rent(ByteBufferPool.Size64K);
					slabOffset = 0;
				}

				int written = EncodeLogEvent(asyncEvent.LogEvent, slab, slabOffset);
				if (written > 0)
				{
					slabOffset += written;
					asyncEvent.Continuation(null);
				}
				else
				{
					// Slab full: dispatch current slab
					if (slabOffset > 0)
					{
						sender.Send(slab, 0, slabOffset, null);
						slab = ByteBufferPool.Rent(ByteBufferPool.Size64K);
						slabOffset = 0;
					}

					// Try again in fresh slab
					written = EncodeLogEvent(asyncEvent.LogEvent, slab, slabOffset);
					if (written > 0)
					{
						slabOffset += written;
						asyncEvent.Continuation(null);
					}
					else
					{
						// Message alone exceeds 64KB or MaxMessageSize: fall back to single Write
						Write(asyncEvent);
					}
				}
			}

			if (slab != null && slabOffset > 0 && sender != null)
			{
				sender.Send(slab, 0, slabOffset, null);
			}
			else if (slab != null && slabOffset == 0)
			{
				ByteBufferPool.Return(slab);
			}
		}

		#endregion
	}
}
