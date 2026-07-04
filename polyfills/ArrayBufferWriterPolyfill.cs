// Polyfill for System.Buffers.ArrayBufferWriter<T> (netstandard2.1 / netcoreapp2.1+). On
// netstandard2.0 the type exists only as an internal in System.Memory, so a public copy is
// provided here. #if-gated to netstandard2.0 only.

#if NETSTANDARD2_0
namespace System.Buffers
{
    internal sealed class ArrayBufferWriter<T> : IBufferWriter<T>
    {
        private const int DefaultInitialBufferSize = 256;

        private T[] _buffer;
        private int _index;

        public ArrayBufferWriter()
        {
            _buffer = Array.Empty<T>();
            _index = 0;
        }

        public ArrayBufferWriter(int initialCapacity)
        {
            if (initialCapacity <= 0)
            {
                throw new ArgumentException("Initial capacity must be positive.", nameof(initialCapacity));
            }

            _buffer = new T[initialCapacity];
            _index = 0;
        }

        public ReadOnlyMemory<T> WrittenMemory => _buffer.AsMemory(0, _index);

        public ReadOnlySpan<T> WrittenSpan => _buffer.AsSpan(0, _index);

        public int WrittenCount => _index;

        public int Capacity => _buffer.Length;

        public int FreeCapacity => _buffer.Length - _index;

        public void Clear()
        {
            _buffer.AsSpan(0, _index).Clear();
            _index = 0;
        }

        public void Advance(int count)
        {
            if (count < 0)
            {
                throw new ArgumentException("Count must be non-negative.", nameof(count));
            }

            if (_index > _buffer.Length - count)
            {
                throw new InvalidOperationException("Cannot advance past the end of the buffer.");
            }

            _index += count;
        }

        public Memory<T> GetMemory(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            return _buffer.AsMemory(_index);
        }

        public Span<T> GetSpan(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            return _buffer.AsSpan(_index);
        }

        private void CheckAndResizeBuffer(int sizeHint)
        {
            if (sizeHint < 0)
            {
                throw new ArgumentException("Size hint must be non-negative.", nameof(sizeHint));
            }

            if (sizeHint == 0)
            {
                sizeHint = 1;
            }

            if (sizeHint <= FreeCapacity)
            {
                return;
            }

            var currentLength = _buffer.Length;
            var growBy = Math.Max(sizeHint, currentLength);
            if (currentLength == 0)
            {
                growBy = Math.Max(growBy, DefaultInitialBufferSize);
            }

            var newSize = currentLength + growBy;
            if ((uint)newSize > int.MaxValue)
            {
                newSize = currentLength + sizeHint;
                if ((uint)newSize > int.MaxValue)
                {
                    throw new InvalidOperationException(
                        "Buffer writer cannot grow beyond the maximum array length.");
                }
            }

            Array.Resize(ref _buffer, newSize);
        }
    }
}
#endif
