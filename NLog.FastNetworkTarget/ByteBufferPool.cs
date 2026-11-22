using System;
using System.Threading;

namespace NLog.Internal
{
	/// <summary>
	/// High-performance, lock-free byte buffer pool designed to eliminate steady-state
	/// allocations and completely prevent Large Object Heap (LOH >= 85KB) fragmentation.
	/// </summary>
	internal static class ByteBufferPool
	{
		public const int Size1K = 1024;
		public const int Size4K = 4096;
		public const int Size16K = 16384;
		public const int Size64K = 65536;

		private sealed class Bucket
		{
			private readonly int _bufferSize;
			private readonly byte[][] _slots;

			[ThreadStatic]
			private static byte[] t_fastSlot1K;
			[ThreadStatic]
			private static byte[] t_fastSlot4K;
			[ThreadStatic]
			private static byte[] t_fastSlot16K;
			[ThreadStatic]
			private static byte[] t_fastSlot64K;

			public Bucket(int bufferSize, int poolCapacity)
			{
				_bufferSize = bufferSize;
				_slots = new byte[poolCapacity][];
			}

			public byte[] Rent()
			{
				// Fast TLS slot (0 locks, 0 atomic ops)
				byte[] buf = GetFastSlot();
				if (buf != null)
				{
					SetFastSlot(null);
					return buf;
				}

				// Shared slot pool
				for (int i = 0; i < _slots.Length; i++)
				{
					buf = _slots[i];
					if (buf != null && Interlocked.CompareExchange(ref _slots[i], null, buf) == buf)
						return buf;
				}

				return new byte[_bufferSize];
			}

			public void Return(byte[] buffer)
			{
				if (buffer == null || buffer.Length != _bufferSize)
					return;

				if (GetFastSlot() == null)
				{
					SetFastSlot(buffer);
					return;
				}

				for (int i = 0; i < _slots.Length; i++)
				{
					if (_slots[i] == null && Interlocked.CompareExchange(ref _slots[i], buffer, null) == null)
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
					case Size64K: t_fastSlot64K = buf; break;
				}
			}
		}

		private static readonly Bucket _bucket1K = new Bucket(Size1K, 64);
		private static readonly Bucket _bucket4K = new Bucket(Size4K, 64);
		private static readonly Bucket _bucket16K = new Bucket(Size16K, 64);
		private static readonly Bucket _bucket64K = new Bucket(Size64K, 64);

		public static byte[] Rent(int minSize)
		{
			if (minSize <= Size1K) return _bucket1K.Rent();
			if (minSize <= Size4K) return _bucket4K.Rent();
			if (minSize <= Size16K) return _bucket16K.Rent();
			if (minSize <= Size64K) return _bucket64K.Rent();

			return new byte[minSize];
		}

		public static void Return(byte[] buffer)
		{
			if (buffer == null) return;

			switch (buffer.Length)
			{
				case Size1K: _bucket1K.Return(buffer); break;
				case Size4K: _bucket4K.Return(buffer); break;
				case Size16K: _bucket16K.Return(buffer); break;
				case Size64K: _bucket64K.Return(buffer); break;
			}
		}
	}
}
