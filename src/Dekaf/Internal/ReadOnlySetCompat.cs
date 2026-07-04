using System.Collections.Concurrent;
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

    /// <summary>
    /// Collection helpers used to bridge BCL API gaps on the lower target frameworks while keeping
    /// call sites uniform across all TFMs.
    /// </summary>
    internal static class DekafCollectionExtensions
    {
        /// <summary>
        /// Materializes a sequence into a <see cref="DekafSet{T}"/> (which satisfies
        /// <c>IReadOnlySet&lt;T&gt;</c> on every TFM, unlike <see cref="HashSet{T}"/> on netstandard).
        /// </summary>
        public static DekafSet<T> ToDekafSet<T>(this IEnumerable<T> source) => new DekafSet<T>(source);

        /// <summary>
        /// Copies an <see cref="IReadOnlyDictionary{TKey,TValue}"/> into a mutable
        /// <see cref="Dictionary{TKey,TValue}"/>. netstandard2.0's Dictionary has no ctor that
        /// accepts an IReadOnlyDictionary/IEnumerable, so this bridges that gap uniformly.
        /// </summary>
        public static Dictionary<TKey, TValue> CopyToDictionary<TKey, TValue>(
            this IReadOnlyDictionary<TKey, TValue> source, IEqualityComparer<TKey>? comparer = null)
            where TKey : notnull
        {
            var result = new Dictionary<TKey, TValue>(source.Count, comparer);
            foreach (var pair in source)
            {
                result[pair.Key] = pair.Value;
            }

            return result;
        }

#if NETSTANDARD
        /// <summary>
        /// Polyfill for <c>ConcurrentDictionary.TryRemove(KeyValuePair)</c> (net5.0+): removes the
        /// entry only if both key and value match.
        /// </summary>
        public static bool TryRemove<TKey, TValue>(
            this ConcurrentDictionary<TKey, TValue> dictionary,
            KeyValuePair<TKey, TValue> item)
            where TKey : notnull
            => ((ICollection<KeyValuePair<TKey, TValue>>)dictionary).Remove(item);
#endif
    }
}
