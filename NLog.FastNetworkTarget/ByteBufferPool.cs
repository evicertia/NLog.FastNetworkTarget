using System;
using System.Threading;

namespace NLog.Internal
{
	/// <summary>
	/// High-performance, lock-free byte buffer pool designed to eliminate steady-state
	/// allocations and prevent Large Object Heap (LOH >= 85KB) GC pressure under deep queues.
	/// Callers MUST only pass buffers obtained from <see cref="Rent"/> to <see cref="Return"/>.
	/// </summary>
	internal static class ByteBufferPool
	{
		public const int Size1K = 1024;
		public const int Size4K = 4096;
		public const int Size16K = 16384;
		public const int Size32K = 32768;
		public const int Size64K = 65536;
		public const int Size128K = 131072;
		public const int Size256K = 262144;

		private sealed class Bucket
		{
			private readonly int _bufferSize;
			private readonly bool _useThreadLocal;
			private readonly byte[][] _slots;
			private readonly int _mask;
			private int _rentCursor;
			private int _returnCursor;

			[ThreadStatic]
			private static byte[] t_fastSlot1K;
			[ThreadStatic]
			private static byte[] t_fastSlot4K;
			[ThreadStatic]
			private static byte[] t_fastSlot16K;
			[ThreadStatic]
			private static byte[] t_fastSlot32K;
			[ThreadStatic]
			private static byte[] t_fastSlot64K;

			public Bucket(int bufferSize, int poolCapacity, bool useThreadLocal)
			{
				_bufferSize = bufferSize;
				_useThreadLocal = useThreadLocal;
				_slots = new byte[poolCapacity][];
				_mask = poolCapacity - 1;
			}

			public byte[] Rent()
			{
				byte[] buf;
				if (_useThreadLocal)
				{
					buf = GetFastSlot();
					if (buf != null)
					{
						SetFastSlot(null);
						return buf;
					}
				}

				int start = Interlocked.Increment(ref _rentCursor);
				int probes = Math.Min(_slots.Length, 16);
				for (int i = 0; i < probes; i++)
				{
					int idx = (start + i) & _mask;
					buf = _slots[idx];
					if (buf != null && Interlocked.CompareExchange(ref _slots[idx], null, buf) == buf)
						return buf;
				}

				return new byte[_bufferSize];
			}

			public void Return(byte[] buffer)
			{
				if (buffer == null || buffer.Length != _bufferSize)
					return;

				if (_useThreadLocal && GetFastSlot() == null)
				{
					SetFastSlot(buffer);
					return;
				}

				int start = Interlocked.Increment(ref _returnCursor);
				int probes = Math.Min(_slots.Length, 16);
				for (int i = 0; i < probes; i++)
				{
					int idx = (start + i) & _mask;
					if (_slots[idx] == null && Interlocked.CompareExchange(ref _slots[idx], buffer, null) == null)
						return;
				}
			}

			private byte[] GetFastSlot()
			{
				switch (_bufferSize)
				{
					case Size1K: return t_fastSlot1K;
					case Size4K: return t_fastSlot4K;
					case Size16K: return t_fastSlot16K;
					case Size32K: return t_fastSlot32K;
					case Size64K: return t_fastSlot64K;
					default: return null;
				}
			}

			private void SetFastSlot(byte[] buf)
			{
				switch (_bufferSize)
				{
					case Size1K: t_fastSlot1K = buf; break;
					case Size4K: t_fastSlot4K = buf; break;
					case Size16K: t_fastSlot16K = buf; break;
					case Size32K: t_fastSlot32K = buf; break;
					case Size64K: t_fastSlot64K = buf; break;
				}
			}
		}

		private static readonly Bucket _bucket1K = new Bucket(Size1K, 1024, useThreadLocal: true);
		private static readonly Bucket _bucket4K = new Bucket(Size4K, 256, useThreadLocal: true);
		private static readonly Bucket _bucket16K = new Bucket(Size16K, 64, useThreadLocal: true);
		private static readonly Bucket _bucket32K = new Bucket(Size32K, 64, useThreadLocal: true);
		private static readonly Bucket _bucket64K = new Bucket(Size64K, 64, useThreadLocal: true);
#if !NET8_0_OR_GREATER
		private static readonly Bucket _bucket128K = new Bucket(Size128K, 16, useThreadLocal: false);
		private static readonly Bucket _bucket256K = new Bucket(Size256K, 16, useThreadLocal: false);
#endif

		public static byte[] Rent(int minSize)
		{
			if (minSize <= Size1K) return _bucket1K.Rent();
			if (minSize <= Size4K) return _bucket4K.Rent();
			if (minSize <= Size16K) return _bucket16K.Rent();
			if (minSize <= Size32K) return _bucket32K.Rent();
			if (minSize <= Size64K) return _bucket64K.Rent();

#if NET8_0_OR_GREATER
			return System.Buffers.ArrayPool<byte>.Shared.Rent(minSize);
#else
			if (minSize <= Size128K) return _bucket128K.Rent();
			if (minSize <= Size256K) return _bucket256K.Rent();

			return new byte[minSize];
#endif
		}

		public static void Return(byte[] buffer)
		{
			if (buffer == null) return;

			switch (buffer.Length)
			{
				case Size1K: _bucket1K.Return(buffer); break;
				case Size4K: _bucket4K.Return(buffer); break;
				case Size16K: _bucket16K.Return(buffer); break;
				case Size32K: _bucket32K.Return(buffer); break;
				case Size64K: _bucket64K.Return(buffer); break;
				default:
#if NET8_0_OR_GREATER
					if (buffer.Length > Size64K)
						System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
#else
					if (buffer.Length == Size128K) _bucket128K.Return(buffer);
					else if (buffer.Length == Size256K) _bucket256K.Return(buffer);
#endif
					break;
			}
		}
	}
}
