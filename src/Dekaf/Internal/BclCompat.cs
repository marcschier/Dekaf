using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace Dekaf.Internal;

/// <summary>
/// Compatibility shims for BCL surface that only exists on net8.0+ (static members / newer
/// overloads that cannot be provided as extension methods, so the source-only Polyfill package
/// does not cover them). Every member forwards to the native API on net8.0+ (aggressively inlined,
/// so the modern fast paths are unchanged) and provides a managed fallback on netstandard2.0/2.1.
/// </summary>
internal static class BclCompat
{
    /// <summary>Maximum length of a single-dimension array (<see cref="Array.MaxLength"/> on net6.0+).</summary>
    public static readonly int MaxArrayLength =
#if NETSTANDARD
        0x7FFFFFC7;
#else
        Array.MaxLength;
#endif

#if NETSTANDARD
    private static readonly Stopwatch MonotonicClock = Stopwatch.StartNew();
#endif

    /// <summary>Monotonic millisecond tick count (<see cref="Environment.TickCount64"/> on net6.0+).</summary>
    public static long TickCount64 =>
#if NETSTANDARD
        MonotonicClock.ElapsedMilliseconds;
#else
        Environment.TickCount64;
#endif

    /// <summary>Approximate total memory available to the process.</summary>
    public static long TotalAvailableMemoryBytes =>
#if NETSTANDARD
        // GCMemoryInfo is unavailable; assume a conservative 4 GiB ceiling for budgeting purposes.
        4L * 1024 * 1024 * 1024;
#else
        GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> FromException<T>(Exception exception)
#if NETSTANDARD
    {
        var tcs = new TaskCompletionSource<T>();
        tcs.SetException(exception);
        return new ValueTask<T>(tcs.Task);
    }
#else
        => ValueTask.FromException<T>(exception);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint RoundUpToPowerOf2(uint value)
#if NETSTANDARD
    {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
#else
        => System.Numerics.BitOperations.RoundUpToPowerOf2(value);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Log2(uint value)
#if NETSTANDARD
    {
        var result = 0;
        while ((value >>= 1) != 0)
        {
            result++;
        }

        return result;
    }
#else
        => System.Numerics.BitOperations.Log2(value);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Log2(ulong value)
#if NETSTANDARD
    {
        var result = 0;
        while ((value >>= 1) != 0)
        {
            result++;
        }

        return result;
    }
#else
        => System.Numerics.BitOperations.Log2(value);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool AsciiIsValid(ReadOnlySpan<char> text)
    {
#if NETSTANDARD
        foreach (var c in text)
        {
            if (c > '\x007F')
            {
                return false;
            }
        }

        return true;
#else
        return System.Text.Ascii.IsValid(text);
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Guid ReadGuidBigEndian(ReadOnlySpan<byte> source)
    {
#if NETSTANDARD
        var a = BinaryPrimitives.ReadInt32BigEndian(source);
        var b = BinaryPrimitives.ReadInt16BigEndian(source.Slice(4));
        var c = BinaryPrimitives.ReadInt16BigEndian(source.Slice(6));
        return new Guid(
            a, b, c, source[8], source[9], source[10], source[11], source[12], source[13], source[14], source[15]);
#else
        return new Guid(source, bigEndian: true);
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteGuidBigEndian(Guid value, Span<byte> destination)
    {
#if NETSTANDARD
        var little = value.ToByteArray();
        BinaryPrimitives.WriteInt32BigEndian(destination, BinaryPrimitives.ReadInt32LittleEndian(little));
        BinaryPrimitives.WriteInt16BigEndian(destination.Slice(4), BinaryPrimitives.ReadInt16LittleEndian(little.AsSpan(4)));
        BinaryPrimitives.WriteInt16BigEndian(destination.Slice(6), BinaryPrimitives.ReadInt16LittleEndian(little.AsSpan(6)));
        little.AsSpan(8, 8).CopyTo(destination.Slice(8));
#else
        value.TryWriteBytes(destination, bigEndian: true, out _);
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint InterlockedIncrement(ref uint location)
#if NETSTANDARD
        => unchecked((uint)Interlocked.Increment(ref Unsafe.As<uint, int>(ref location)));
#else
        => Interlocked.Increment(ref location);
#endif

    public static Task<IPAddress[]> GetHostAddressesAsync(string hostNameOrAddress, CancellationToken cancellationToken)
#if NETSTANDARD
        => Dns.GetHostAddressesAsync(hostNameOrAddress); // pre-net6 has no cancellation support.
#else
        => Dns.GetHostAddressesAsync(hostNameOrAddress, cancellationToken);
#endif

    public static byte[] Pbkdf2(byte[] password, byte[] salt, int iterations, HashAlgorithmName hashAlgorithm, int outputLength)
    {
#if NETSTANDARD
        // The hash algorithm is an explicit SHA-256/384/512 supplied by the caller (SCRAM/admin),
        // so this is not a weak-hash use; the native Rfc2898DeriveBytes.Pbkdf2 is used on net8.0+.
#pragma warning disable CA5379
        using var derive = new Rfc2898DeriveBytes(password, salt, iterations, hashAlgorithm);
#pragma warning restore CA5379
        return derive.GetBytes(outputLength);
#else
        return Rfc2898DeriveBytes.Pbkdf2(password, salt, iterations, hashAlgorithm, outputLength);
#endif
    }

    public static byte[] RandomBytes(int count)
    {
#if NETSTANDARD
        var bytes = new byte[count];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(bytes);
        return bytes;
#else
        return RandomNumberGenerator.GetBytes(count);
#endif
    }

    public static byte[] HmacHashData(HashAlgorithmName hashAlgorithm, byte[] key, byte[] message)
    {
#if NETSTANDARD
        using HMAC hmac = hashAlgorithm == HashAlgorithmName.SHA256
            ? new HMACSHA256(key)
            : new HMACSHA512(key);
        return hmac.ComputeHash(message);
#else
        return hashAlgorithm == HashAlgorithmName.SHA256
            ? HMACSHA256.HashData(key, message)
            : HMACSHA512.HashData(key, message);
#endif
    }

    public static byte[] ShaHashData(HashAlgorithmName hashAlgorithm, byte[] data)
    {
#if NETSTANDARD
        using HashAlgorithm sha = hashAlgorithm == HashAlgorithmName.SHA256
            ? SHA256.Create()
            : SHA512.Create();
        return sha.ComputeHash(data);
#else
        return hashAlgorithm == HashAlgorithmName.SHA256
            ? SHA256.HashData(data)
            : SHA512.HashData(data);
#endif
    }

    /// <summary>
    /// Resets a pooled <see cref="CancellationTokenSource"/> for reuse
    /// (<see cref="CancellationTokenSource.TryReset"/> on net6.0+); returns false on the lower
    /// TFMs so the caller allocates a fresh source instead of pooling.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryResetCompat(this CancellationTokenSource source)
#if NETSTANDARD
        => false;
#else
        => source.TryReset();
#endif
}
