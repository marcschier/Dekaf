// netstandard2.0-only extension polyfills for members that exist on netstandard2.1+ (as instance
// methods / newer overloads) and therefore cannot be reached via the source-only Polyfill package.
// #if-gated to netstandard2.0 only.

#if NETSTANDARD2_0
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Dekaf.Internal
{
    internal static class Netstandard20Compat
    {
        /// <summary>Polyfill for <c>ChannelReader&lt;T&gt;.ReadAllAsync</c> (netcoreapp3.0+).</summary>
        public static async IAsyncEnumerable<T> ReadAllAsync<T>(
            this ChannelReader<T> reader,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Polyfill for the <c>ConcurrentDictionary.AddOrUpdate</c> factory-argument overload (net5.0+).
        /// The argument avoids a closure on capable TFMs; here it is passed through captured lambdas.
        /// </summary>
        public static TValue AddOrUpdate<TKey, TValue, TArg>(
            this ConcurrentDictionary<TKey, TValue> dictionary,
            TKey key,
            Func<TKey, TArg, TValue> addValueFactory,
            Func<TKey, TValue, TArg, TValue> updateValueFactory,
            TArg factoryArgument)
            where TKey : notnull
            => dictionary.AddOrUpdate(
                key,
                k => addValueFactory(k, factoryArgument),
                (k, existing) => updateValueFactory(k, existing, factoryArgument));

        /// <summary>Polyfill for <c>Socket.ReceiveAsync(Memory&lt;byte&gt;, SocketFlags)</c> (netcoreapp2.1+).</summary>
        public static async ValueTask<int> ReceiveAsync(this Socket socket, Memory<byte> buffer, SocketFlags socketFlags)
        {
            if (MemoryMarshal.TryGetArray<byte>(buffer, out var segment))
            {
                return await socket.ReceiveAsync(segment, socketFlags).ConfigureAwait(false);
            }

            var rented = buffer.ToArray();
            var read = await socket.ReceiveAsync(new ArraySegment<byte>(rented), socketFlags).ConfigureAwait(false);
            new ReadOnlySpan<byte>(rented, 0, read).CopyTo(buffer.Span);
            return read;
        }
    }
}
#endif
