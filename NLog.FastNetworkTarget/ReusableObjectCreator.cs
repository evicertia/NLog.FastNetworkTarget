// 
// Copyright (c) 2004-2021 Jaroslaw Kowalski <jaak@jkowalski.net>, Kim Christensen, Julian Verdurmen
// 
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without 
// modification, are permitted provided that the following conditions 
// are met:
// 
// * Redistributions of source code must retain the above copyright notice, 
//   this list of conditions and the following disclaimer. 
// 
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution. 
// 
// * Neither the name of Jaroslaw Kowalski nor the names of its 
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission. 
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
// THE POSSIBILITY OF SUCH DAMAGE.
// 

using System;
using System.Text;
using System.Diagnostics;

namespace NLog.Internal
{
	internal class ReusableObjectCreator<T>
		where T : class
	{
		#region Inner Types

		public struct LockOject : IDisposable
		{
			#region Fields

			private readonly ReusableObjectCreator<T> _owner;
			public readonly T Result;

			#endregion

			#region .ctors

			public LockOject(ReusableObjectCreator<T> owner, T reusableObject)
			{
				Result = reusableObject;
				_owner = owner;
			}

			#endregion

			public void Dispose()
			{
				_owner?.Deallocate(Result);
			}
		}

		#endregion

		#region Fields

		private readonly Func<int, T> _createObject;
		private readonly Action<T> _clearObject;
		private readonly int _initialCapacity;
		protected T _reusableObject;

		#endregion

		#region .ctors

		protected ReusableObjectCreator(int initialCapacity, Func<int, T> createObject, Action<T> clearObject)
		{
			_reusableObject = createObject(initialCapacity);
			_initialCapacity = initialCapacity;
			_createObject = createObject;
			_clearObject = clearObject;
		}

		#endregion

		private void Deallocate(T reusableObject)
		{
			_clearObject(reusableObject);
			_reusableObject = reusableObject;
		}

		/// <summary>
		/// Creates handle to the reusable char[]-buffer for active usage
		/// </summary>
		/// <returns>Handle to the reusable item, that can release it again</returns>
		public LockOject Allocate()
		{
			var reusableObject = _reusableObject ?? _createObject(_initialCapacity);

			Debug.Assert(_reusableObject != null);
			_reusableObject = null;

			return new LockOject(this, reusableObject);
		}
	}

	#region Implementations

	/// <summary>
	/// Controls a single allocated char[]-buffer for reuse (only one active user)
	/// </summary>
	internal class ReusableBufferCreator : ReusableObjectCreator<char[]>
	{
		public ReusableBufferCreator(int capacity)
			: base(capacity, cap => new char[cap], b => { })
		{ }
	}

	/// <summary>
	/// Controls a single allocated StringBuilder for reuse (only one active user)
	/// </summary>
	internal class ReusableBuilderCreator : ReusableObjectCreator<StringBuilder>
	{
		public ReusableBuilderCreator()
			: base(128, (cap) => new StringBuilder(cap), sb => sb.Clear())
		{ }
	}

	#endregion
}