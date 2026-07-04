// Polyfill for System.Buffers.SequenceReader<T> (netcoreapp3.0 / netstandard2.1+). netstandard2.0's
// System.Memory package does not ship it. This is a faithful port of the members Dekaf's protocol
// reader uses; #if-gated to netstandard2.0 only (every other TFM provides the real type).

#if NETSTANDARD2_0
namespace System.Buffers
{
    internal ref struct SequenceReader<T>
        where T : unmanaged, IEquatable<T>
    {
        private readonly ReadOnlySequence<T> _sequence;
        private SequencePosition _currentPosition;
        private SequencePosition _nextPosition;
        private ReadOnlySpan<T> _currentSpan;
        private int _currentSpanIndex;
        private long _consumed;
        private bool _moreData;
        private readonly long _length;

        public SequenceReader(ReadOnlySequence<T> sequence)
        {
            _sequence = sequence;
            _currentSpanIndex = 0;
            _consumed = 0;
            _currentPosition = sequence.Start;
            _length = sequence.Length;

            var first = sequence.First.Span;
            _nextPosition = sequence.GetPosition(first.Length);
            _currentSpan = first;
            _moreData = first.Length > 0;

            if (!_moreData && !sequence.IsSingleSegment)
            {
                _moreData = true;
                GetNextSpan();
            }
        }

        public readonly bool End => !_moreData;

        public readonly ReadOnlySequence<T> Sequence => _sequence;

        public readonly long Length => _length;

        public readonly long Consumed => _consumed;

        public readonly long Remaining => _length - _consumed;

        public readonly SequencePosition Position => _sequence.GetPosition(_consumed);

        public readonly ReadOnlySpan<T> UnreadSpan => _currentSpan.Slice(_currentSpanIndex);

        public readonly ReadOnlySequence<T> UnreadSequence => _sequence.Slice(Position);

        public bool TryRead(out T value)
        {
            if (End)
            {
                value = default;
                return false;
            }

            value = _currentSpan[_currentSpanIndex];
            _currentSpanIndex++;
            _consumed++;

            if (_currentSpanIndex >= _currentSpan.Length)
            {
                GetNextSpan();
            }

            return true;
        }

        public void Advance(long count)
        {
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            if (count <= _currentSpan.Length - _currentSpanIndex)
            {
                _currentSpanIndex += (int)count;
                _consumed += count;
                if (_currentSpanIndex >= _currentSpan.Length)
                {
                    GetNextSpan();
                }

                return;
            }

            AdvanceToNextSpan(count);
        }

        public readonly bool TryCopyTo(Span<T> destination)
        {
            var firstSpan = UnreadSpan;
            if (firstSpan.Length >= destination.Length)
            {
                firstSpan.Slice(0, destination.Length).CopyTo(destination);
                return true;
            }

            return TryCopyMultisegment(destination);
        }

        private readonly bool TryCopyMultisegment(Span<T> destination)
        {
            if (Remaining < destination.Length)
            {
                return false;
            }

            var firstSpan = UnreadSpan;
            firstSpan.CopyTo(destination);
            var copied = firstSpan.Length;

            var next = _nextPosition;
            while (_sequence.TryGet(ref next, out var nextSegment, advance: true))
            {
                var nextSpan = nextSegment.Span;
                if (nextSpan.Length > 0)
                {
                    var toCopy = Math.Min(nextSpan.Length, destination.Length - copied);
                    nextSpan.Slice(0, toCopy).CopyTo(destination.Slice(copied));
                    copied += toCopy;
                    if (copied >= destination.Length)
                    {
                        break;
                    }
                }
            }

            return true;
        }

        private void AdvanceToNextSpan(long count)
        {
            _consumed += count;
            while (_moreData)
            {
                var remaining = _currentSpan.Length - _currentSpanIndex;
                if (remaining > count)
                {
                    _currentSpanIndex += (int)count;
                    count = 0;
                    break;
                }

                _currentSpanIndex += remaining;
                count -= remaining;
                GetNextSpan();

                if (count == 0)
                {
                    break;
                }
            }

            if (count != 0)
            {
                _consumed -= count;
                throw new ArgumentOutOfRangeException(nameof(count));
            }
        }

        private void GetNextSpan()
        {
            if (!_sequence.IsSingleSegment)
            {
                var previousNextPosition = _nextPosition;
                while (_sequence.TryGet(ref _nextPosition, out var memory, advance: true))
                {
                    _currentPosition = previousNextPosition;
                    if (memory.Length > 0)
                    {
                        _currentSpan = memory.Span;
                        _currentSpanIndex = 0;
                        _moreData = true;
                        return;
                    }

                    _currentSpan = default;
                    _currentSpanIndex = 0;
                    previousNextPosition = _nextPosition;
                }

                _moreData = false;
            }
            else
            {
                _moreData = false;
            }
        }
    }
}
#endif
