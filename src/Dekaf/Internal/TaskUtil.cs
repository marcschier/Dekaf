using System.Runtime.CompilerServices;

namespace Dekaf.Internal;

/// <summary>
/// Task combinators that use the allocation-free <see cref="ReadOnlySpan{T}"/> overloads on
/// net9.0+ and fall back to array-based overloads on earlier targets.
/// </summary>
internal static class TaskUtil
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task WhenAll(ReadOnlySpan<Task> tasks)
#if NET9_0_OR_GREATER
        => Task.WhenAll(tasks);
#else
        => Task.WhenAll(tasks.ToArray());
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task<T[]> WhenAll<T>(ReadOnlySpan<Task<T>> tasks)
#if NET9_0_OR_GREATER
        => Task.WhenAll(tasks);
#else
        => Task.WhenAll(tasks.ToArray());
#endif
}
