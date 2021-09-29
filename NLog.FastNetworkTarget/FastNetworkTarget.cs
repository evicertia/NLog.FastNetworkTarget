using System;
using System.Text;
using System.Threading;
using System.Reflection;
using System.Net.Sockets;
using System.ComponentModel;
using System.Collections.Generic;
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
		#region Fields & Properties

		private static readonly MethodInfo _tryGetCachedLayoutValue = typeof(LogEventInfo).GetMethod("TryGetCachedLayoutValue", BindingFlags.NonPublic | BindingFlags.Instance);
		private static readonly MethodInfo _renderAppendBuilder = typeof(Layout).GetMethod("RenderAppendBuilder", BindingFlags.NonPublic | BindingFlags.Instance);

		private readonly IDictionary<string, LinkedListNode<NetworkSender>> _currentSenderCache = new Dictionary<string, LinkedListNode<NetworkSender>>();
		private readonly ReusableBufferCreator _reusableEncodingBuffer = new ReusableBufferCreator(16 * 1024);
		private readonly LinkedList<NetworkSender> _openNetworkSenders = new LinkedList<NetworkSender>();
		private readonly ReusableBuilderCreator _reusableLayoutBuilder = new ReusableBuilderCreator();

		/// <summary>
		/// Gets or sets the network address.
		/// </summary>
		/// <remarks>
		/// The network address can be:
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
		/// Gets or sets the action that should be taken if the will be more connections than <see cref="MaxConnections"/>.
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
		/// Get or set the SSL/TLS protocols. Not implemented.
		/// </summary>
		public SslProtocols SslProtocols { get; set; } = SslProtocols.None;

		/// <summary>
		/// The number of milliseconds a connection will remain idle before the first keep-alive probe is sent
		/// </summary>
		public int KeepAliveTime { get; set; }

		/// <summary>
		/// Get or set the timeout in milliseconds for socket connection.
		/// </summary>
		[DefaultValue(1000)]
		public int ConnectionTimeout { get; set; } = 1000;

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

		#region Private methods

		private static bool TryGetCachedLayoutValue(LogEventInfo logEvent, Layout layout, out object text)
		{
			var args = new[] { layout, (object)null };
			var result = (bool)_tryGetCachedLayoutValue.Invoke(logEvent, args);

			text = args[1];

			return result;
		}

		private static void RenderAppendBuilder(Layout layout, LogEventInfo logEvent, StringBuilder target, bool cacheLayoutResult = false)
		{
			_renderAppendBuilder.Invoke(layout, new object[] { logEvent, target, cacheLayoutResult });
		}

		private static bool TryRemove<T>(LinkedList<T> list, LinkedListNode<T> node)
		{
			if (node is null || list != node.List)
				return false;

			list.Remove(node);
			return true;
		}

		private static NetworkSender Create(string url, int maxQueueSize, SslProtocols sslProtocols, TimeSpan keepAliveTime, TimeSpan connectionTimeout)
		{
			if (url.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
			{
				return new TcpNetworkSender(url, AddressFamily.Unspecified)
				{
					ConnectionTimeout = connectionTimeout,
					KeepAliveTime = keepAliveTime,
					SslProtocols = sslProtocols,
					MaxQueueSize = maxQueueSize,
				};
			}

			if (url.StartsWith("tcp4://", StringComparison.OrdinalIgnoreCase))
			{
				return new TcpNetworkSender(url, AddressFamily.InterNetwork)
				{
					ConnectionTimeout = connectionTimeout,
					KeepAliveTime = keepAliveTime,
					SslProtocols = sslProtocols,
					MaxQueueSize = maxQueueSize,
				};
			}

			if (url.StartsWith("tcp6://", StringComparison.OrdinalIgnoreCase))
			{
				return new TcpNetworkSender(url, AddressFamily.InterNetworkV6)
				{
					ConnectionTimeout = connectionTimeout,
					KeepAliveTime = keepAliveTime,
					SslProtocols = sslProtocols,
					MaxQueueSize = maxQueueSize,
				};
			}

			throw new ArgumentException("Unrecognized network address.", nameof(url));
		}

		private NetworkSender CreateNetworkSender(string address)
		{
			var sender = Create(address, MaxQueueSize, SslProtocols, TimeSpan.FromMilliseconds(KeepAliveTime), TimeSpan.FromMilliseconds(ConnectionTimeout));
			sender.Initialize();

			return sender;
		}

		private LinkedListNode<NetworkSender> GetCachedNetworkSender(string address)
		{
			lock (_currentSenderCache)
			{
				// Already have address
				if (_currentSenderCache.TryGetValue(address, out var senderNode))
					return senderNode;

				if (_currentSenderCache.Count >= ConnectionCacheSize)
				{
					// Make room in the cache by closing the least recently used connection
					LinkedListNode<NetworkSender> leastRecentlyUsed = null;
					var minAccessTime = int.MaxValue;

					foreach (var pair in _currentSenderCache)
					{
						var networkSender = pair.Value.Value;
						if (networkSender.LastSendTime < minAccessTime)
						{
							minAccessTime = networkSender.LastSendTime;
							leastRecentlyUsed = pair.Value;
						}
					}

					if (leastRecentlyUsed != null)
						ReleaseCachedConnection(leastRecentlyUsed);
				}

				var sender = CreateNetworkSender(address);
				lock (_openNetworkSenders)
					senderNode = _openNetworkSenders.AddLast(sender);

				_currentSenderCache.Add(address, senderNode);

				return senderNode;
			}
		}

		private void ReleaseCachedConnection(LinkedListNode<NetworkSender> senderNode)
		{
			lock (_currentSenderCache)
			{
				var networkSender = senderNode.Value;
				lock (_openNetworkSenders)
				{
					if (TryRemove(_openNetworkSenders, senderNode))
					{
						// Only remove it once.
						networkSender.Close(ex => { });
					}
				}

				// Make sure the current sender for this address is the one we want to remove.
				if (_currentSenderCache.TryGetValue(networkSender.Address, out var sender2) && ReferenceEquals(senderNode, sender2))
					_currentSenderCache.Remove(networkSender.Address);
			}
		}

		private void ChunkedSend(NetworkSender sender, byte[] buffer, AsyncContinuation continuation)
		{
			var toSend = buffer.Length;
			if (toSend <= MaxMessageSize)
			{
				// Chunking is not needed, no need to perform delegate capture
				InternalLogger.Trace("{0}: Sending chunk, position: {1}, length: {2}", nameof(FastNetworkTarget), 0, toSend);
				if (toSend <= 0)
				{
					continuation(null);
					return;
				}

				sender.Send(buffer, 0, toSend, continuation);
			}
			else
			{
				var pos = 0;

				void SendNextChunk(Exception ex)
				{
					if (ex != null)
					{
						continuation(ex);
						return;
					}

					InternalLogger.Trace("{0}: Sending chunk, position: {1}, length: {2}", nameof(FastNetworkTarget), pos, toSend);
					if (toSend <= 0)
					{
						continuation(null);
						return;
					}

					var chunksize = toSend;
					if (chunksize > MaxMessageSize)
					{
						if (OnOverflow == NetworkTargetOverflowAction.Discard)
						{
							InternalLogger.Trace("{0}: Discard because chunksize > this.MaxMessageSize", nameof(FastNetworkTarget));
							continuation(null);
							return;
						}

						if (OnOverflow == NetworkTargetOverflowAction.Error)
						{
							continuation(new OverflowException($"Attempted to send a message larger than MaxMessageSize ({MaxMessageSize}). Actual size was: {buffer.Length}. Adjust OnOverflow and MaxMessageSize parameters accordingly."));
							return;
						}

						chunksize = MaxMessageSize;
					}

					var pos0 = pos;
					toSend -= chunksize;
					pos += chunksize;

					sender.Send(buffer, pos0, chunksize, SendNextChunk);
				}

				SendNextChunk(null);
			}
		}

		private byte[] GetBytesFromStringBuilder(char[] charBuffer, StringBuilder stringBuilder)
		{
			if (stringBuilder.Length <= charBuffer.Length)
			{
				stringBuilder.CopyTo(0, charBuffer, 0, stringBuilder.Length);
				return Encoding.GetBytes(charBuffer, 0, stringBuilder.Length);
			}

			return Encoding.GetBytes(stringBuilder.ToString());
		}

		private byte[] GetBytesFromString(char[] charBuffer, string layoutMessage)
		{
			if (layoutMessage.Length <= charBuffer.Length)
			{
				layoutMessage.CopyTo(0, charBuffer, 0, layoutMessage.Length);
				return Encoding.GetBytes(charBuffer, 0, layoutMessage.Length);
			}

			return Encoding.GetBytes(layoutMessage);
		}

		private byte[] GetBytesToWrite(LogEventInfo logEvent)
		{
			if (OptimizeBufferReuse)
			{
				using (var localBuffer = _reusableEncodingBuffer.Allocate())
				{
					if (!NewLine && TryGetCachedLayoutValue(logEvent, Layout, out var text))
					{
						return GetBytesFromString(localBuffer.Result, text?.ToString() ?? string.Empty);
					}
					else
					{
						using (var localBuilder = _reusableLayoutBuilder.Allocate())
						{
							RenderAppendBuilder(Layout, logEvent, localBuilder.Result, false);

							if (NewLine)
								localBuilder.Result.Append(LineEnding.NewLineCharacters);

							return GetBytesFromStringBuilder(localBuffer.Result, localBuilder.Result);
						}
					}
				}
			}
			else
			{
				var rendered = Layout.Render(logEvent);
				InternalLogger.Trace("NetworkTarget(Name={0}): Sending: {1}", Name, rendered);
				if (NewLine)
				{
					rendered += LineEnding.NewLineCharacters;
				}
				return Encoding.GetBytes(rendered);
			}
		}

		#endregion

		protected override void FlushAsync(AsyncContinuation asyncContinuation)
		{
			int remainingCount;

			void Continuation(Exception ex)
			{
				// Ignore exception
				if (Interlocked.Decrement(ref remainingCount) == 0)
					asyncContinuation(null);
			}

			lock (_openNetworkSenders)
			{
				remainingCount = _openNetworkSenders.Count;
				if (remainingCount == 0)
				{
					// Nothing to flush
					asyncContinuation(null);
				}
				else
				{
					// Otherwise call FlushAsync() on all senders and invoke continuation at the very end
					foreach (var openSender in _openNetworkSenders)
						openSender.FlushAsync(Continuation);
				}
			}
		}

		protected override void CloseTarget()
		{
			base.CloseTarget();

			lock (_openNetworkSenders)
			{
				foreach (var openSender in _openNetworkSenders)
					openSender.Close(ex => { });

				_openNetworkSenders.Clear();
			}
		}

		protected override void Write(AsyncLogEventInfo logEvent)
		{
			var address = RenderLogEvent(Address, logEvent.LogEvent);
			var bytes = GetBytesToWrite(logEvent.LogEvent);

			if (KeepConnection)
			{
				LinkedListNode<NetworkSender> senderNode;

				try
				{
					senderNode = GetCachedNetworkSender(address);
				}
				catch (Exception ex)
				{
					InternalLogger.Error(ex, "NetworkTarget(Name={0}): Failed to create sender to address: '{1}'", Name, address);
					throw;
				}

				ChunkedSend(
					senderNode.Value,
					bytes,
					ex =>
					{
						if (ex != null)
							InternalLogger.Error(ex, "NetworkTarget(Name={0}): Error when sending.", Name);

						logEvent.Continuation(ex);
					});
			}
			else
			{
				NetworkSender sender;
				LinkedListNode<NetworkSender> linkedListNode;

				lock (_openNetworkSenders)
				{
					var tooManyConnections = _openNetworkSenders.Count >= MaxConnections;

					if (tooManyConnections && MaxConnections > 0)
					{
						switch (OnConnectionOverflow)
						{
							case NetworkTargetConnectionsOverflowAction.DiscardMessage:
								InternalLogger.Warn("NetworkTarget(Name={0}): Discarding message otherwise to many connections.", Name);
								logEvent.Continuation(null);
								return;

							case NetworkTargetConnectionsOverflowAction.AllowNewConnnection:
								InternalLogger.Debug("NetworkTarget(Name={0}): Too may connections, but this is allowed", Name);
								break;

							case NetworkTargetConnectionsOverflowAction.Block:
								while (_openNetworkSenders.Count >= MaxConnections)
								{
									InternalLogger.Debug("NetworkTarget(Name={0}): Blocking networktarget otherwhise too many connections.", Name);
									Monitor.Wait(_openNetworkSenders);
									InternalLogger.Trace("NetworkTarget(Name={0}): Entered critical section.", Name);
								}

								InternalLogger.Trace("NetworkTarget(Name={0}): Limit ok.", Name);
								break;
						}
					}

					try
					{
						sender = CreateNetworkSender(address);
					}
					catch (Exception ex)
					{
						InternalLogger.Error(ex, "NetworkTarget(Name={0}): Failed to create sender to address: '{1}'", Name, address);
						throw;
					}

					linkedListNode = _openNetworkSenders.AddLast(sender);
				}

				ChunkedSend(
					sender,
					bytes,
					ex =>
					{
						lock (_openNetworkSenders)
						{
							TryRemove(_openNetworkSenders, linkedListNode);

							if (OnConnectionOverflow == NetworkTargetConnectionsOverflowAction.Block)
								Monitor.PulseAll(_openNetworkSenders);
						}

						if (ex != null)
							InternalLogger.Error(ex, "NetworkTarget(Name={0}): Error when sending.", Name);

						sender.Close(ex2 => { });
						logEvent.Continuation(ex);
					});
			}
		}
	}
}