using System.Collections.Generic;

#if !NET5_0_OR_GREATER
namespace System.Collections.Generic
{
    /// <summary>
    /// Polyfill for <see cref="IReadOnlySet{T}"/> (net5.0+). Public because Dekaf exposes it on
    /// its public API (e.g. consumer Subscription/Assignment). The member signatures match
    /// <see cref="HashSet{T}"/> so a HashSet-derived type satisfies it via inherited members.
    /// </summary>
    public interface IReadOnlySet<T> : IReadOnlyCollection<T>
    {
        bool Contains(T item);
        bool IsProperSubsetOf(IEnumerable<T> other);
        bool IsProperSupersetOf(IEnumerable<T> other);
        bool IsSubsetOf(IEnumerable<T> other);
        bool IsSupersetOf(IEnumerable<T> other);
        bool Overlaps(IEnumerable<T> other);
        bool SetEquals(IEnumerable<T> other);
    }
}
#endif

namespace Dekaf
{
    /// <summary>
    /// A <see cref="HashSet{T}"/> that also implements <c>IReadOnlySet&lt;T&gt;</c> on target
    /// frameworks whose <see cref="HashSet{T}"/> predates that interface (netstandard). On net5.0+
    /// it is simply a <see cref="HashSet{T}"/>. Used wherever Dekaf exposes a set as
    /// <c>IReadOnlySet&lt;T&gt;</c> so the assignment compiles on every TFM.
    /// </summary>
    internal sealed class DekafSet<T> : HashSet<T>
#if !NET5_0_OR_GREATER
        , IReadOnlySet<T>
#endif
    {
        public DekafSet()
        {
        }

        public DekafSet(int capacity)
#if NETSTANDARD2_0
            // netstandard2.0's HashSet has no capacity constructor; capacity is only a hint.
            : base()
#else
            : base(capacity)
#endif
        {
        }

        public DekafSet(IEnumerable<T> collection)
            : base(collection)
        {
        }

        public DekafSet(IEqualityComparer<T>? comparer)
            : base(comparer)
        {
        }

        public DekafSet(IEnumerable<T> collection, IEqualityComparer<T>? comparer)
            : base(collection, comparer)
        {
        }
    }
}
