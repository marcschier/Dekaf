// Polyfill for System.HashCode (netstandard2.1 / netcoreapp2.1+). netstandard2.0 has no HashCode.
// This is a lightweight order-sensitive combiner sufficient for GetHashCode (not cryptographic).
// #if-gated to netstandard2.0 only.

#if NETSTANDARD2_0
namespace System
{
    using System.Collections.Generic;

    internal struct HashCode
    {
        private const int Seed = unchecked((int)2166136261);
        private const int Prime = 16777619;

        private int _hash;
        private bool _initialized;

        public void Add<T>(T value)
        {
            Mix(value is null ? 0 : value.GetHashCode());
        }

        public void Add<T>(T value, IEqualityComparer<T>? comparer)
        {
            Mix(value is null ? 0 : comparer?.GetHashCode(value) ?? value.GetHashCode());
        }

        public readonly int ToHashCode() => _initialized ? _hash : Seed;

        private void Mix(int value)
        {
            if (!_initialized)
            {
                _hash = Seed;
                _initialized = true;
            }

            _hash = unchecked((_hash ^ value) * Prime);
        }

        public static int Combine<T1>(T1 v1)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2>(T1 v1, T2 v2)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2, T3>(T1 v1, T2 v2, T3 v3)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            hc.Add(v3);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2, T3, T4>(T1 v1, T2 v2, T3 v3, T4 v4)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            hc.Add(v3);
            hc.Add(v4);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2, T3, T4, T5>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            hc.Add(v3);
            hc.Add(v4);
            hc.Add(v5);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2, T3, T4, T5, T6>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            hc.Add(v3);
            hc.Add(v4);
            hc.Add(v5);
            hc.Add(v6);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2, T3, T4, T5, T6, T7>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            hc.Add(v3);
            hc.Add(v4);
            hc.Add(v5);
            hc.Add(v6);
            hc.Add(v7);
            return hc.ToHashCode();
        }

        public static int Combine<T1, T2, T3, T4, T5, T6, T7, T8>(
            T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7, T8 v8)
        {
            var hc = default(HashCode);
            hc.Add(v1);
            hc.Add(v2);
            hc.Add(v3);
            hc.Add(v4);
            hc.Add(v5);
            hc.Add(v6);
            hc.Add(v7);
            hc.Add(v8);
            return hc.ToHashCode();
        }
    }
}
#endif
