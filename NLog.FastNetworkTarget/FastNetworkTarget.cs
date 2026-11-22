using System;
using System.Text;
using System.Threading;
using System.Reflection;
using System.Diagnostics;
using System.Net.Sockets;
using System.ComponentModel;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Security.Authentication;

using NLog.Common;
using NLog.Layouts;
using NLog.Internal;
using NLog.Internal.NetworkSenders;
using NLog.LayoutRenderers.Wrappers;

namespace NLog.Targets
{
	[Target("FastNetwork")]
	public class FastNetworkTarget : TargetWithLayout
	{
		#region Constants & Inner Types

		private const int TLS_CHAR_BUFFER_SIZE = 4096;
		private const int TLS_MAX_BUILDER_CAPACITY = 40960; // Keep <= 80KB (below 85KB LOH threshold), trim giant outliers
		private const int CLOSE_DRAIN_TIMEOUT_MS = 2000;

		private enum EncodeStatus
		{
			Ok = 0,
			SlabFull = 1,
			FallbackSingle = 2
		}

		#endregion

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
				// Fallback gracefully if NLog internals differ
			}
		}

		#endregion

		#region Fields & Properties

		private readonly object _senderGate = new object();

		// Retained and synchronized for backward compatibility with Herma.Facilities NLogStatsFetcher reflection
		private readonly IDictionary<string, LinkedListNode<NetworkSender>> _currentSenderCache = new Dictionary<string, LinkedListNode<NetworkSender>>(StringComparer.OrdinalIgnoreCase);
		private readonly LinkedList<NetworkSender> _openNetworkSenders = new LinkedList<NetworkSender>();

		private readonly ConcurrentDictionary<string, TcpNetworkSender> _senderMap = new ConcurrentDictionary<string, TcpNetworkSender>(StringComparer.OrdinalIgnoreCase);
		private TcpNetworkSender _fixedSender;
		private TcpNetworkSender _mruSender;

		private volatile bool _isOpen;
		private bool _isStaticAddress;
		private bool _isUtf8Encoding = true;
		private byte[] _newLineBytes = Array.Empty<byte>();

		// Cumulative metrics preserved from evicted/closed senders
		private long _retiredSentMessages;
		private long _retiredSentBytes;
		private long _retiredSentBatches;
		private long _retiredDroppedMessages;
		private long _retiredBlockedEnqueues;
		private long _retiredSendFailures;
		private long _retiredReconnectCount;

		[ThreadStatic]
		private static StringBuilder t_threadBuilder;

		[ThreadStatic]
		private static Encoder t_threadEncoder;

		[ThreadStatic]
		private static Encoding t_threadEncoderEncoding;

#if !NET8_0_OR_GREATER
		[ThreadStatic]
		private static char[] t_threadCharBuffer;
#endif

		/// <summary>
		/// Gets or sets the network address.
		/// </summary>
		/// <remarks>
		/// Supported schemes:
		/// <ul>
		/// <li>tcp://host:port - TCP (auto select IPv4/IPv6)</li>
		/// <li>tcp4://host:port - force TCP/IPv4</li>
		/// <li>tcp6://host:port - force TCP/IPv6</li>
		/// </ul>
		/// </remarks>
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
		/// Gets or sets the maximum message size in bytes. Defaults to 262144 (256 KB).
		/// </summary>
		[DefaultValue(262144)]
		public int MaxMessageSize { get; set; } = 262144;

		/// <summary>
		/// Gets or sets the size of the connection cache (number of connections which are kept alive).
		/// </summary>
		[DefaultValue(5)]
		public int ConnectionCacheSize { get; set; } = 5;

		/// <summary>
		/// Gets or sets the maximum current connections when <see cref="KeepConnection"/> is false. 0 = no maximum.
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
		/// Gets or sets the action that should be taken if the message is larger than <see cref="MaxMessageSize"/>.
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
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return fixedSender.PendingMessages;

				if (!KeepConnection)
				{
					lock (_openNetworkSenders)
					{
						long pending = 0;
						foreach (var s in _openNetworkSenders)
						{
							if (s is TcpNetworkSender tcp)
								pending += tcp.PendingMessages;
						}
						return pending;
					}
				}

				long total = 0;
				foreach (var pair in _senderMap)
					total += pair.Value.PendingMessages;
				return total;
			}
		}

		public int ConnectionCount
		{
			get
			{
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return 1;

				if (!KeepConnection)
				{
					lock (_openNetworkSenders)
						return _openNetworkSenders.Count;
				}

				return _senderMap.Count;
			}
		}

		public long SentMessages
		{
			get
			{
				long total = Volatile.Read(ref _retiredSentMessages);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.SentMessages;

				foreach (var pair in _senderMap)
					total += pair.Value.SentMessages;
				return total;
			}
		}

		public long SentBytes
		{
			get
			{
				long total = Volatile.Read(ref _retiredSentBytes);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.SentBytes;

				foreach (var pair in _senderMap)
					total += pair.Value.SentBytes;
				return total;
			}
		}

		public long SentBatches
		{
			get
			{
				long total = Volatile.Read(ref _retiredSentBatches);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.SentBatches;

				foreach (var pair in _senderMap)
					total += pair.Value.SentBatches;
				return total;
			}
		}

		public long DroppedMessages
		{
			get
			{
				long total = Volatile.Read(ref _retiredDroppedMessages);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.DroppedMessages;

				foreach (var pair in _senderMap)
					total += pair.Value.DroppedMessages;
				return total;
			}
		}

		public long BlockedEnqueues
		{
			get
			{
				long total = Volatile.Read(ref _retiredBlockedEnqueues);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.BlockedEnqueues;

				foreach (var pair in _senderMap)
					total += pair.Value.BlockedEnqueues;
				return total;
			}
		}

		public long SendFailures
		{
			get
			{
				long total = Volatile.Read(ref _retiredSendFailures);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.SendFailures;

				foreach (var pair in _senderMap)
					total += pair.Value.SendFailures;
				return total;
			}
		}

		public long ReconnectCount
		{
			get
			{
				long total = Volatile.Read(ref _retiredReconnectCount);
				var fixedSender = Volatile.Read(ref _fixedSender);
				if (fixedSender != null)
					return total + fixedSender.ReconnectCount;

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

		private static bool IsStaticAddressLayout(Layout addressLayout)
		{
			if (addressLayout is SimpleLayout simple)
			{
				if (simple.IsFixedText)
					return true;

				if (simple.Renderers != null &&
					simple.Renderers.Count == 1 &&
					simple.Renderers[0] is CachedLayoutRendererWrapper cached &&
					cached.Cached &&
					cached.ClearCache == CachedLayoutRendererWrapper.ClearCacheOption.None &&
					cached.CacheKey == null &&
					cached.CachedSeconds == 0)
				{
					return true;
				}
			}

			return false;
		}

		private static bool IsSupportedTcpScheme(string url)
		{
			return url.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase) ||
			       url.StartsWith("tcp4://", StringComparison.OrdinalIgnoreCase) ||
			       url.StartsWith("tcp6://", StringComparison.OrdinalIgnoreCase);
		}

		protected override void InitializeTarget()
		{
			base.InitializeTarget();

			var enc = Encoding ?? Encoding.UTF8;
			_isUtf8Encoding = enc is UTF8Encoding;
			_newLineBytes = NewLine && LineEnding != null
				? enc.GetBytes(LineEnding.NewLineCharacters)
				: Array.Empty<byte>();
			_isStaticAddress = IsStaticAddressLayout(Address);

			lock (_senderGate)
			{
				_isOpen = true;
			}

			if (_isStaticAddress && KeepConnection && Address != null)
			{
				var addr = RenderLogEvent(Address, LogEventInfo.CreateNullEvent());
				if (!string.IsNullOrWhiteSpace(addr) && IsSupportedTcpScheme(addr))
				{
					var sender = GetOrCreateCachedSender(addr);
					Volatile.Write(ref _fixedSender, sender);
				}
			}
		}

		protected override void CloseTarget()
		{
			base.CloseTarget();

			List<TcpNetworkSender> toClose;
			lock (_senderGate)
			{
				_isOpen = false;
				Volatile.Write(ref _fixedSender, null);
				Volatile.Write(ref _mruSender, null);
				_senderMap.Clear();

				lock (_openNetworkSenders)
				{
					toClose = new List<TcpNetworkSender>(_openNetworkSenders.Count);
					foreach (var s in _openNetworkSenders)
					{
						if (s is TcpNetworkSender tcp)
							toClose.Add(tcp);
					}

					_openNetworkSenders.Clear();
					Monitor.PulseAll(_openNetworkSenders);
				}

				lock (_currentSenderCache)
				{
					_currentSenderCache.Clear();
				}
			}

			if (toClose.Count == 0)
				return;

			foreach (var sender in toClose)
				sender.Close(ex => { });

			long deadline = Stopwatch.GetTimestamp() + (long)(CLOSE_DRAIN_TIMEOUT_MS * (Stopwatch.Frequency / 1000.0));
			foreach (var sender in toClose)
			{
				long remTicks = deadline - Stopwatch.GetTimestamp();
				int waitMs = remTicks > 0 ? Math.Max(1, (int)(remTicks * 1000 / Stopwatch.Frequency)) : 0;
				if (waitMs > 0)
					sender.WaitForClose(waitMs);

				RetireSenderMetrics(sender);
			}
		}

		protected override void FlushAsync(AsyncContinuation asyncContinuation)
		{
			List<NetworkSender> senders;
			lock (_openNetworkSenders)
			{
				if (_openNetworkSenders.Count == 0)
				{
					asyncContinuation(null);
					return;
				}

				senders = new List<NetworkSender>(_openNetworkSenders);
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

		private void RetireSenderMetrics(TcpNetworkSender sender)
		{
			if (sender == null)
				return;

			Interlocked.Add(ref _retiredSentMessages, sender.SentMessages);
			Interlocked.Add(ref _retiredSentBytes, sender.SentBytes);
			Interlocked.Add(ref _retiredSentBatches, sender.SentBatches);
			Interlocked.Add(ref _retiredDroppedMessages, sender.DroppedMessages);
			Interlocked.Add(ref _retiredBlockedEnqueues, sender.BlockedEnqueues);
			Interlocked.Add(ref _retiredSendFailures, sender.SendFailures);
			Interlocked.Add(ref _retiredReconnectCount, sender.ReconnectCount);
		}

		private static AddressFamily ValidateAndDetectAddressFamily(string url)
		{
			if (url.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
				return AddressFamily.Unspecified;

			if (url.StartsWith("tcp4://", StringComparison.OrdinalIgnoreCase))
				return AddressFamily.InterNetwork;

			if (url.StartsWith("tcp6://", StringComparison.OrdinalIgnoreCase))
				return AddressFamily.InterNetworkV6;

			throw new ArgumentException("Unrecognized network address.", nameof(url));
		}

		private TcpNetworkSender CreateInitializedSender(string url)
		{
			var af = ValidateAndDetectAddressFamily(url);
			var sender = new TcpNetworkSender(url, af)
			{
				ConnectionTimeout = TimeSpan.FromMilliseconds(ConnectionTimeout),
				KeepAliveTime = TimeSpan.FromMilliseconds(KeepAliveTime),
				SslProtocols = SslProtocols,
				MaxQueueSize = KeepConnection ? MaxQueueSize : 2,
				NoDelay = NoDelay,
				SendTimeout = SendTimeout,
				SendBufferSize = SendBufferSize,
				BatchSize = BatchSize,
				MaxBatchBytes = MaxBatchBytes,
				OnQueueOverflow = OnQueueOverflow
			};

			sender.Initialize();
			return sender;
		}

		private static bool TryRemove<T>(LinkedList<T> list, LinkedListNode<T> node)
		{
			if (node == null || list != node.List)
				return false;

			list.Remove(node);
			return true;
		}

		private TcpNetworkSender GetOrCreateCachedSender(string address)
		{
			var mru = Volatile.Read(ref _mruSender);
			if (mru != null && string.Equals(mru.Address, address, StringComparison.OrdinalIgnoreCase))
				return mru;

			if (_senderMap.TryGetValue(address, out var existing))
			{
				Volatile.Write(ref _mruSender, existing);
				return existing;
			}

			TcpNetworkSender evicted = null;
			TcpNetworkSender created;

			lock (_senderGate)
			{
				if (!_isOpen)
					return null;

				if (_senderMap.TryGetValue(address, out existing))
				{
					Volatile.Write(ref _mruSender, existing);
					return existing;
				}

				if (ConnectionCacheSize > 0 && _senderMap.Count >= ConnectionCacheSize)
				{
					LinkedListNode<NetworkSender> lruNode = null;
					long minTicks = long.MaxValue;

					lock (_openNetworkSenders)
					{
						for (var node = _openNetworkSenders.First; node != null; node = node.Next)
						{
							if (node.Value.LastSendTicks < minTicks)
							{
								minTicks = node.Value.LastSendTicks;
								lruNode = node;
							}
						}

						if (lruNode != null)
						{
							TryRemove(_openNetworkSenders, lruNode);
							evicted = lruNode.Value as TcpNetworkSender;
						}
					}

					if (evicted != null)
					{
						_senderMap.TryRemove(evicted.Address, out _);
						lock (_currentSenderCache)
						{
							if (_currentSenderCache.TryGetValue(evicted.Address, out var cachedNode) && ReferenceEquals(cachedNode, lruNode))
								_currentSenderCache.Remove(evicted.Address);
						}

						if (ReferenceEquals(Volatile.Read(ref _mruSender), evicted))
							Volatile.Write(ref _mruSender, null);
					}
				}

				try
				{
					created = CreateInitializedSender(address);
				}
				catch (Exception ex)
				{
					InternalLogger.Error(ex, "NetworkTarget(Name={0}): Failed to create sender to address: '{1}'", Name, address);
					throw;
				}

				lock (_openNetworkSenders)
				{
					var node = _openNetworkSenders.AddLast(created);
					lock (_currentSenderCache)
					{
						_currentSenderCache[address] = node;
					}
				}

				_senderMap[address] = created;
				Volatile.Write(ref _mruSender, created);
			}

			if (evicted != null)
			{
				evicted.Close(ex =>
				{
					RetireSenderMetrics(evicted);
				});
			}

			return created;
		}

		private TcpNetworkSender ResolveCachedSender(LogEventInfo logEvent)
		{
			var fixedSender = Volatile.Read(ref _fixedSender);
			if (fixedSender != null)
				return fixedSender;

			var address = RenderLogEvent(Address, logEvent);
			if (string.IsNullOrWhiteSpace(address))
				return null;

			var sender = GetOrCreateCachedSender(address);
			if (sender != null)
			{
				if (_isStaticAddress && KeepConnection)
					Interlocked.CompareExchange(ref _fixedSender, sender, null);
				else
					sender.TouchLastSendTime();
			}

			return sender;
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

		private Encoder GetThreadEncoder(Encoding encoding)
		{
			var encoder = t_threadEncoder;
			if (encoder == null || !ReferenceEquals(t_threadEncoderEncoding, encoding))
			{
				t_threadEncoderEncoding = encoding;
				t_threadEncoder = encoder = encoding.GetEncoder();
			}
			else
			{
				encoder.Reset();
			}

			return encoder;
		}

		private static void ReleaseThreadBuilder(StringBuilder sb)
		{
			bool trim = sb.Capacity > TLS_MAX_BUILDER_CAPACITY;
			sb.Clear();
			if (trim)
				sb.Capacity = TLS_CHAR_BUFFER_SIZE;
		}

#if NET8_0_OR_GREATER
		private int GetBuilderByteCount(StringBuilder sb, Encoding encoding)
		{
			var chunks = sb.GetChunks();
			if (!chunks.MoveNext())
				return 0;

			var first = chunks.Current.Span;
			if (!chunks.MoveNext())
				return first.IsEmpty ? 0 : encoding.GetByteCount(first);

			// Counting slices does not advance Encoder state across surrogate pairs.
			return encoding.GetByteCount(sb.ToString());
		}

		private int EncodeBuilderToSpan(StringBuilder sb, Encoding encoding, Span<byte> destination)
		{
			var chunks = sb.GetChunks();
			if (!chunks.MoveNext())
				return 0;

			var first = chunks.Current.Span;
			if (!chunks.MoveNext())
				return first.IsEmpty ? 0 : encoding.GetBytes(first, destination);

			var encoder = GetThreadEncoder(encoding);
			int written = first.IsEmpty ? 0 : encoder.GetBytes(first, destination, flush: false);
			bool hasMore = true;
			while (hasMore)
			{
				var current = chunks.Current.Span;
				hasMore = chunks.MoveNext();
				if (!current.IsEmpty)
					written += encoder.GetBytes(current, destination.Slice(written), flush: !hasMore);
			}
			return written;
		}
#else
		private int GetBuilderByteCount(StringBuilder sb, Encoding encoding, char[] charBuf)
		{
			int charLen = sb.Length;
			if (charLen == 0)
				return 0;

			if (charLen <= charBuf.Length)
			{
				sb.CopyTo(0, charBuf, 0, charLen);
				return encoding.GetByteCount(charBuf, 0, charLen);
			}

			// UTF-8 is stateless at scalar boundaries. Leave a trailing high surrogate
			// for the next slice, including when a strict fallback is configured.
			if (encoding is UTF8Encoding)
			{
				int total = 0;
				for (int pos = 0; pos < charLen;)
				{
					int n = Math.Min(charLen - pos, charBuf.Length);
					sb.CopyTo(pos, charBuf, 0, n);
					if (pos + n < charLen && char.IsHighSurrogate(charBuf[n - 1])) n--;
					total = checked(total + encoding.GetByteCount(charBuf, 0, n));
					pos += n;
				}
				return total;
			}
			return encoding.GetByteCount(sb.ToString());
		}

		private int EncodeBuilderToArray(StringBuilder sb, Encoding encoding, char[] charBuf, int preCopiedLen, byte[] dest, int destOffset)
		{
			int charLen = sb.Length;
			if (charLen == 0)
				return 0;

			if (charLen <= charBuf.Length)
			{
				if (preCopiedLen != charLen)
					sb.CopyTo(0, charBuf, 0, charLen);

				return encoding.GetBytes(charBuf, 0, charLen, dest, destOffset);
			}

			var encoder = GetThreadEncoder(encoding);
			int written = 0;
			int pos = 0;
			while (pos < charLen)
			{
				int slice = Math.Min(charLen - pos, charBuf.Length);
				sb.CopyTo(pos, charBuf, 0, slice);
				pos += slice;
				written += encoder.GetBytes(charBuf, 0, slice, dest, destOffset + written, pos >= charLen);
			}
			return written;
		}
#endif

		private EncodeStatus TryEncodeIntoSlab(LogEventInfo logEvent, byte[] slab, int slabOffset, ref string rendered, out int bytesWritten)
		{
			bytesWritten = 0;
			var encoding = Encoding;
			int maxAllowed = MaxMessageSize > 0 ? Math.Min(MaxMessageSize, slab.Length) : slab.Length;
			int remainingInSlab = slab.Length - slabOffset;

			if (rendered != null || (TryGetCachedLayoutValue(logEvent, Layout, out var cachedText) && (rendered = cachedText?.ToString()) != null))
			{
				string str = rendered;
				int maxPossibleTotal = encoding.GetMaxByteCount(str.Length) + _newLineBytes.Length;

				if (maxPossibleTotal <= maxAllowed && maxPossibleTotal <= remainingInSlab)
				{
					// Single-pass encode (no GetByteCount scan needed)
					int writtenPayload = 0;
					if (str.Length > 0)
					{
#if NET8_0_OR_GREATER
						writtenPayload = encoding.GetBytes(str.AsSpan(), slab.AsSpan(slabOffset));
#else
						writtenPayload = encoding.GetBytes(str, 0, str.Length, slab, slabOffset);
#endif
					}

					if (_newLineBytes.Length > 0)
						Buffer.BlockCopy(_newLineBytes, 0, slab, slabOffset + writtenPayload, _newLineBytes.Length);

					bytesWritten = writtenPayload + _newLineBytes.Length;
					return EncodeStatus.Ok;
				}

#if NET8_0_OR_GREATER
				int payloadBytes = encoding.GetByteCount(str.AsSpan());
#else
				int payloadBytes = encoding.GetByteCount(str);
#endif
				int totalBytes = payloadBytes + _newLineBytes.Length;

				if (totalBytes > maxAllowed)
					return EncodeStatus.FallbackSingle;

				if (totalBytes > remainingInSlab)
					return EncodeStatus.SlabFull;

				if (payloadBytes > 0)
				{
#if NET8_0_OR_GREATER
					encoding.GetBytes(str.AsSpan(), slab.AsSpan(slabOffset, payloadBytes));
#else
					encoding.GetBytes(str, 0, str.Length, slab, slabOffset);
#endif
				}

				if (_newLineBytes.Length > 0)
					Buffer.BlockCopy(_newLineBytes, 0, slab, slabOffset + payloadBytes, _newLineBytes.Length);

				bytesWritten = totalBytes;
				return EncodeStatus.Ok;
			}

			var sb = t_threadBuilder ?? (t_threadBuilder = new StringBuilder(1024));
			sb.Clear();

			try
			{
				RenderAppendBuilder(Layout, logEvent, sb, false);

				int maxBuilderTotal = encoding.GetMaxByteCount(sb.Length) + _newLineBytes.Length;
				if (maxBuilderTotal <= maxAllowed && maxBuilderTotal <= remainingInSlab)
				{
					int writtenPayload = 0;
					if (sb.Length > 0)
					{
#if NET8_0_OR_GREATER
						writtenPayload = EncodeBuilderToSpan(sb, encoding, slab.AsSpan(slabOffset));
#else
						var fastCharBuf = t_threadCharBuffer ?? (t_threadCharBuffer = new char[TLS_CHAR_BUFFER_SIZE]);
						writtenPayload = EncodeBuilderToArray(sb, encoding, fastCharBuf, 0, slab, slabOffset);
#endif
					}

					if (_newLineBytes.Length > 0)
						Buffer.BlockCopy(_newLineBytes, 0, slab, slabOffset + writtenPayload, _newLineBytes.Length);

					bytesWritten = writtenPayload + _newLineBytes.Length;
					return EncodeStatus.Ok;
				}

#if NET8_0_OR_GREATER
				int reqBytes = GetBuilderByteCount(sb, encoding);
				int fullTotal = reqBytes + _newLineBytes.Length;

				if (fullTotal > maxAllowed || fullTotal > remainingInSlab)
				{
					rendered = sb.ToString();
					return fullTotal > maxAllowed ? EncodeStatus.FallbackSingle : EncodeStatus.SlabFull;
				}

				if (reqBytes > 0)
					EncodeBuilderToSpan(sb, encoding, slab.AsSpan(slabOffset, reqBytes));
#else
				var charBuf = t_threadCharBuffer ?? (t_threadCharBuffer = new char[TLS_CHAR_BUFFER_SIZE]);
				int reqBytes = GetBuilderByteCount(sb, encoding, charBuf);
				int fullTotal = reqBytes + _newLineBytes.Length;

				if (fullTotal > maxAllowed || fullTotal > remainingInSlab)
				{
					rendered = sb.ToString();
					return fullTotal > maxAllowed ? EncodeStatus.FallbackSingle : EncodeStatus.SlabFull;
				}

				if (reqBytes > 0)
					EncodeBuilderToArray(sb, encoding, charBuf, sb.Length <= charBuf.Length ? sb.Length : 0, slab, slabOffset);
#endif

				if (_newLineBytes.Length > 0)
					Buffer.BlockCopy(_newLineBytes, 0, slab, slabOffset + reqBytes, _newLineBytes.Length);

				bytesWritten = fullTotal;
				return EncodeStatus.Ok;
			}
			finally
			{
				ReleaseThreadBuilder(sb);
			}
		}

		private byte[] RenderToRentedBuffer(LogEventInfo logEvent, out int totalLength, string rendered)
		{
			var encoding = Encoding;

			if (rendered != null || (TryGetCachedLayoutValue(logEvent, Layout, out var cachedText) && (rendered = cachedText?.ToString()) != null))
			{
				string str = rendered;
				int maxPossibleTotal = encoding.GetMaxByteCount(str.Length) + _newLineBytes.Length;

				if (maxPossibleTotal <= ByteBufferPool.Size1K)
				{
					byte[] fastBuf = ByteBufferPool.Rent(Math.Max(1, maxPossibleTotal));
					int written = 0;
					if (str.Length > 0)
					{
#if NET8_0_OR_GREATER
						written = encoding.GetBytes(str.AsSpan(), fastBuf.AsSpan());
#else
						written = encoding.GetBytes(str, 0, str.Length, fastBuf, 0);
#endif
					}

					if (_newLineBytes.Length > 0)
						Buffer.BlockCopy(_newLineBytes, 0, fastBuf, written, _newLineBytes.Length);

					totalLength = written + _newLineBytes.Length;
					return fastBuf;
				}

#if NET8_0_OR_GREATER
				int byteCount = encoding.GetByteCount(str.AsSpan());
				int total = byteCount + _newLineBytes.Length;
				byte[] buf = ByteBufferPool.Rent(Math.Max(1, total));

				if (byteCount > 0)
					encoding.GetBytes(str.AsSpan(), buf.AsSpan(0, byteCount));
#else
				int byteCount = encoding.GetByteCount(str);
				int total = byteCount + _newLineBytes.Length;
				byte[] buf = ByteBufferPool.Rent(Math.Max(1, total));

				if (byteCount > 0)
					encoding.GetBytes(str, 0, str.Length, buf, 0);
#endif

				if (_newLineBytes.Length > 0)
					Buffer.BlockCopy(_newLineBytes, 0, buf, byteCount, _newLineBytes.Length);

				totalLength = total;
				return buf;
			}

			var sb = t_threadBuilder ?? (t_threadBuilder = new StringBuilder(1024));
			sb.Clear();

			try
			{
				RenderAppendBuilder(Layout, logEvent, sb, false);

				int maxBuilderTotal = encoding.GetMaxByteCount(sb.Length) + _newLineBytes.Length;
				if (maxBuilderTotal <= ByteBufferPool.Size1K)
				{
					byte[] fastBuf = ByteBufferPool.Rent(Math.Max(1, maxBuilderTotal));
					int written = 0;
					if (sb.Length > 0)
					{
#if NET8_0_OR_GREATER
						written = EncodeBuilderToSpan(sb, encoding, fastBuf.AsSpan());
#else
						var fastCharBuf = t_threadCharBuffer ?? (t_threadCharBuffer = new char[TLS_CHAR_BUFFER_SIZE]);
						written = EncodeBuilderToArray(sb, encoding, fastCharBuf, 0, fastBuf, 0);
#endif
					}

					if (_newLineBytes.Length > 0)
						Buffer.BlockCopy(_newLineBytes, 0, fastBuf, written, _newLineBytes.Length);

					totalLength = written + _newLineBytes.Length;
					return fastBuf;
				}

#if NET8_0_OR_GREATER
				int reqBytes = GetBuilderByteCount(sb, encoding);
				int fullTotal = reqBytes + _newLineBytes.Length;
				byte[] rented = ByteBufferPool.Rent(Math.Max(1, fullTotal));

				if (reqBytes > 0)
					EncodeBuilderToSpan(sb, encoding, rented.AsSpan(0, reqBytes));
#else
				var charBuf = t_threadCharBuffer ?? (t_threadCharBuffer = new char[TLS_CHAR_BUFFER_SIZE]);
				int reqBytes = GetBuilderByteCount(sb, encoding, charBuf);
				int fullTotal = reqBytes + _newLineBytes.Length;
				byte[] rented = ByteBufferPool.Rent(Math.Max(1, fullTotal));

				if (reqBytes > 0)
					EncodeBuilderToArray(sb, encoding, charBuf, sb.Length <= charBuf.Length ? sb.Length : 0, rented, 0);
#endif

				if (_newLineBytes.Length > 0)
					Buffer.BlockCopy(_newLineBytes, 0, rented, reqBytes, _newLineBytes.Length);

				totalLength = fullTotal;
				return rented;
			}
			finally
			{
				ReleaseThreadBuilder(sb);
			}
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

		private bool IsNoOpContinuation(AsyncContinuation continuation)
		{
			return continuation == null || ReferenceEquals(continuation, _completedContinuation);
		}

		public bool NewLinePerSplitChunk { get; set; }

		private int AdjustUtf8ChunkBoundary(byte[] buffer, int pos, int proposedSlice, int totalLimit)
		{
			if (!_isUtf8Encoding || pos + proposedSlice >= totalLimit)
				return proposedSlice;

			// Step back up to 3 bytes if split point falls on a UTF-8 continuation byte (10xxxxxx)
			int minSlice = Math.Max(0, proposedSlice - 3);
			int slice = proposedSlice;
			while (slice > minSlice && (buffer[pos + slice] & 0xC0) == 0x80)
				slice--;

			if (slice == 0) throw new OverflowException("MaxMessageSize cannot fit one UTF-8 character plus line ending.");
			return slice;
		}

		private void SendSplitChunks(TcpNetworkSender sender, byte[] fullBuf, int fullLen, AsyncContinuation finalContinuation)
		{
			object completion = IsNoOpContinuation(finalContinuation) ? null : finalContinuation;
			if (!NewLinePerSplitChunk || _newLineBytes.Length == 0)
			{
				sender.SendPooled(fullBuf, 0, fullLen, 1, completion, MaxMessageSize);
				return;
			}
			try
			{
				int limit = MaxMessageSize - _newLineBytes.Length;
				if (limit <= 0) throw new OverflowException("MaxMessageSize must exceed the line ending length.");
				int end = fullLen - _newLineBytes.Length;
				int chunks = 0;
				for (int pos = 0; pos < end; chunks++)
					pos += AdjustUtf8ChunkBoundary(fullBuf, pos, Math.Min(limit, end - pos), end);
				var framed = ByteBufferPool.Rent(checked(end + chunks * _newLineBytes.Length));
				int written = 0;
				for (int pos = 0; pos < end;)
				{
					int n = AdjustUtf8ChunkBoundary(fullBuf, pos, Math.Min(limit, end - pos), end);
					Buffer.BlockCopy(fullBuf, pos, framed, written, n);
					Buffer.BlockCopy(_newLineBytes, 0, framed, written + n, _newLineBytes.Length);
					pos += n;
					written += n + _newLineBytes.Length;
				}
				sender.SendPooled(framed, 0, written, 1, completion, MaxMessageSize);
			}
			finally
			{
				ByteBufferPool.Return(fullBuf);
			}
		}

		private void WriteSingleToSender(TcpNetworkSender sender, AsyncLogEventInfo logEvent, string rendered = null)
		{
			if (sender == null)
			{
				logEvent.Continuation(null);
				return;
			}

			byte[] buf = RenderToRentedBuffer(logEvent.LogEvent, out int len, rendered);

			if (len == 0)
			{
				ByteBufferPool.Return(buf);
				logEvent.Continuation(null);
				return;
			}

			if (MaxMessageSize > 0 && len > MaxMessageSize)
			{
				if (OnOverflow == NetworkTargetOverflowAction.Discard)
				{
					ByteBufferPool.Return(buf);
					Interlocked.Increment(ref _retiredDroppedMessages);
					logEvent.Continuation(null);
					return;
				}

				if (OnOverflow == NetworkTargetOverflowAction.Error)
				{
					ByteBufferPool.Return(buf);
					logEvent.Continuation(new OverflowException($"Attempted to send a message larger than MaxMessageSize ({MaxMessageSize}). Actual size was: {len}. Adjust OnOverflow and MaxMessageSize parameters accordingly."));
					return;
				}

				SendSplitChunks(sender, buf, len, logEvent.Continuation);
				return;
			}

			object cont = IsNoOpContinuation(logEvent.Continuation) ? null : logEvent.Continuation;
			sender.SendPooled(buf, 0, len, 1, cont);
		}

		private void WriteTransientConnection(AsyncLogEventInfo logEvent)
		{
			var address = RenderLogEvent(Address, logEvent.LogEvent);
			if (string.IsNullOrWhiteSpace(address))
			{
				logEvent.Continuation(null);
				return;
			}

			TcpNetworkSender sender;
			LinkedListNode<NetworkSender> linkedListNode;

			lock (_openNetworkSenders)
			{
				if (!_isOpen)
				{
					logEvent.Continuation(null);
					return;
				}

				if (MaxConnections > 0 && _openNetworkSenders.Count >= MaxConnections)
				{
					switch (OnConnectionOverflow)
					{
						case NetworkTargetConnectionsOverflowAction.DiscardMessage:
							Interlocked.Increment(ref _retiredDroppedMessages);
							logEvent.Continuation(null);
							return;

						case NetworkTargetConnectionsOverflowAction.AllowNewConnnection:
							break;

						case NetworkTargetConnectionsOverflowAction.Block:
							while (_isOpen && _openNetworkSenders.Count >= MaxConnections)
								Monitor.Wait(_openNetworkSenders);

							if (!_isOpen)
							{
								logEvent.Continuation(null);
								return;
							}
							break;
					}
				}

				try
				{
					sender = CreateInitializedSender(address);
				}
				catch (Exception ex)
				{
					InternalLogger.Error(ex, "NetworkTarget(Name={0}): Failed to create sender to address: '{1}'", Name, address);
					throw;
				}

				linkedListNode = _openNetworkSenders.AddLast(sender);
			}

			AsyncContinuation wrappedContinuation = ex =>
			{
				lock (_openNetworkSenders)
				{
					TryRemove(_openNetworkSenders, linkedListNode);
					if (OnConnectionOverflow == NetworkTargetConnectionsOverflowAction.Block)
						Monitor.PulseAll(_openNetworkSenders);
				}

				sender.Close(ex2 => RetireSenderMetrics(sender));
				logEvent.Continuation(ex);
			};

			try { WriteSingleToSender(sender, logEvent.LogEvent.WithContinuation(wrappedContinuation)); }
			catch (Exception ex) { wrappedContinuation(ex); }
		}

		protected override void Write(AsyncLogEventInfo logEvent)
		{
			if (!KeepConnection)
			{
				WriteTransientConnection(logEvent);
				return;
			}

			if (!_isStaticAddress)
			{
				// Keep a dynamic sender alive until this event is admitted; eviction uses this gate.
				lock (_senderGate) WriteSingleToSender(ResolveCachedSender(logEvent.LogEvent), logEvent);
			}
			else WriteSingleToSender(ResolveCachedSender(logEvent.LogEvent), logEvent);
		}

		protected override void Write(IList<AsyncLogEventInfo> logEvents)
		{
			int count = logEvents.Count;
			if (count == 0)
				return;

			if (!KeepConnection || !_isStaticAddress || count == 1)
			{
				for (int i = 0; i < count; i++)
					Write(logEvents[i]);
				return;
			}

			// Producer-side Slab Coalescing into 64KB pooled buffers
			var fixedSender = Volatile.Read(ref _fixedSender);
			TcpNetworkSender currentSender = fixedSender;
			byte[] slab = null;
			int slabOffset = 0;
			int slabEvents = 0;
			AsyncContinuation singleCont = null;
			List<AsyncContinuation> multiConts = null;

			void FlushCurrentSlab()
			{
				if (slabOffset > 0 && currentSender != null)
				{
					object contPayload = multiConts != null
						? (object)multiConts.ToArray()
						: singleCont;

					multiConts?.Clear();
					singleCont = null;

					currentSender.SendPooled(slab, 0, slabOffset, slabEvents, contPayload);
					slab = null;
					slabOffset = 0;
					slabEvents = 0;
				}
				else if (slab != null)
				{
					ByteBufferPool.Return(slab);
					slab = null;
					slabOffset = 0;
					slabEvents = 0;
					multiConts?.Clear();
					singleCont = null;
				}
			}

			void RecordSlabContinuation(AsyncContinuation cont)
			{
				if (IsNoOpContinuation(cont))
					return;

				if (multiConts != null)
				{
					multiConts.Add(cont);
				}
				else if (singleCont == null)
				{
					singleCont = cont;
				}
				else
				{
					multiConts = new List<AsyncContinuation>(Math.Min(count, 64))
					{
						singleCont,
						cont
					};
					singleCont = null;
				}
			}

			try
			{
				for (int i = 0; i < count; i++)
				{
					var asyncEvent = logEvents[i];

					TcpNetworkSender eventSender = fixedSender ?? ResolveCachedSender(asyncEvent.LogEvent);
					if (eventSender == null)
					{
						asyncEvent.Continuation(null);
						continue;
					}

					if (_isStaticAddress && fixedSender == null)
						fixedSender = eventSender;

					if (currentSender != null && !ReferenceEquals(eventSender, currentSender))
						FlushCurrentSlab();

					currentSender = eventSender;

					if (slab == null)
					{
						slab = ByteBufferPool.Rent(ByteBufferPool.Size64K);
						slabOffset = 0;
						slabEvents = 0;
					}

					string rendered = null;
					var status = TryEncodeIntoSlab(asyncEvent.LogEvent, slab, slabOffset, ref rendered, out int written);
					if (status == EncodeStatus.SlabFull)
					{
						FlushCurrentSlab();
						slab = ByteBufferPool.Rent(ByteBufferPool.Size64K);
						status = TryEncodeIntoSlab(asyncEvent.LogEvent, slab, 0, ref rendered, out written);
					}

					if (status == EncodeStatus.Ok)
					{
						if (written > 0)
						{
							slabOffset += written;
							slabEvents++;
							RecordSlabContinuation(asyncEvent.Continuation);
						}
						else
						{
							asyncEvent.Continuation(null);
						}
					}
					else // FallbackSingle (> 64KB or > MaxMessageSize)
					{
						FlushCurrentSlab();
						WriteSingleToSender(currentSender, asyncEvent, rendered);
					}
				}

				FlushCurrentSlab();
			}
			catch
			{
				if (slab != null)
				{
					ByteBufferPool.Return(slab);
					slab = null;
				}
				throw;
			}
		}

		#endregion
	}
}
