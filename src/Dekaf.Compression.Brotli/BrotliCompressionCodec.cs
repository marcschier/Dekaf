using System.Buffers;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using Dekaf.Protocol.Records;

namespace Dekaf.Compression.Brotli;

/// <summary>
/// Brotli compression codec using the built-in <see cref="BrotliStream"/> from <c>System.IO.Compression</c>.
/// <para>
/// <strong>Important:</strong> Brotli is NOT a standard Kafka compression type.
/// Standard Kafka clients (Java, librdkafka, Confluent.Kafka) do not support Brotli.
/// Both the producer and consumer must have the <c>Dekaf.Compression.Brotli</c> package installed
/// for messages to be compressed and decompressed correctly.
/// </para>
/// <para>
/// Brotli provides excellent compression ratios, especially for text-heavy payloads,
/// but has higher CPU cost than LZ4 or Snappy. Consider using Zstd for a better
/// balance of compression ratio and speed in most Kafka workloads.
/// </para>
/// </summary>
/// <example>
/// <code>
/// // Register the Brotli codec with default settings
/// CompressionCodecRegistry.Default.AddBrotli();
///
/// // Register with a specific compression level
/// CompressionCodecRegistry.Default.AddBrotli(CompressionLevel.SmallestSize);
///
/// // Use with a producer builder
/// var producer = Kafka.CreateProducer&lt;string, string&gt;()
///     .WithBootstrapServers("localhost:9092")
///     .UseBrotliCompression()
///     .Build();
/// </code>
/// </example>
public sealed class BrotliCompressionCodec : ICompressionCodec
{
#if !NETSTANDARD2_0
    private readonly CompressionLevel _compressionLevel;
#endif

    /// <summary>
    /// Creates a new Brotli compression codec with the specified .NET compression level.
    /// </summary>
    /// <param name="compressionLevel">The .NET <see cref="CompressionLevel"/> to use. Default is <see cref="CompressionLevel.Fastest"/>.</param>
    public BrotliCompressionCodec(CompressionLevel compressionLevel = CompressionLevel.Fastest)
    {
#if !NETSTANDARD2_0
        _compressionLevel = compressionLevel;
#else
        _ = compressionLevel;
#endif
    }

    /// <inheritdoc />
    public CompressionType Type => CompressionType.Brotli;

    /// <inheritdoc />
    public void Compress(ReadOnlySequence<byte> source, IBufferWriter<byte> destination)
    {
#if NETSTANDARD2_0
        _ = (source, destination);
        throw new PlatformNotSupportedException(
            "Brotli compression requires .NET Standard 2.1 or later " +
            "(System.IO.Compression.BrotliStream is unavailable on netstandard2.0).");
#else
        using var outputStream = new BufferWriterStream(destination);
        using var brotliStream = new BrotliStream(outputStream, _compressionLevel, leaveOpen: true);

        foreach (var segment in source)
        {
            brotliStream.Write(segment.Span);
        }

        brotliStream.Flush();
#endif
    }

    /// <inheritdoc />
    public void Decompress(ReadOnlySequence<byte> source, IBufferWriter<byte> destination)
    {
#if NETSTANDARD2_0
        _ = (source, destination);
        throw new PlatformNotSupportedException(
            "Brotli decompression requires .NET Standard 2.1 or later " +
            "(System.IO.Compression.BrotliStream is unavailable on netstandard2.0).");
#else
        using var inputStream = new ReadOnlySequenceStream(source);
        using var brotliStream = new BrotliStream(inputStream, CompressionMode.Decompress);

        CompressionStreamCopy.CopyToBufferWriter(brotliStream, destination);
#endif
    }
}

internal static class BrotliModuleInit
{
    [ModuleInitializer]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2255:The 'ModuleInitializer' attribute should not be used in libraries")]
    internal static void Register()
    {
        CompressionCodecRegistry.Default.AddBrotli();
    }
}

/// <summary>
/// Extension methods for configuring Brotli compression on the producer builder.
/// </summary>
public static class BrotliProducerBuilderExtensions
{
    /// <summary>
    /// Configures the producer to use Brotli compression.
    /// <para>
    /// <strong>Important:</strong> Brotli is NOT a standard Kafka compression type.
    /// Both the producer and consumer must have the <c>Dekaf.Compression.Brotli</c> package installed.
    /// </para>
    /// </summary>
    public static ProducerBuilder<TKey, TValue> UseBrotliCompression<TKey, TValue>(this ProducerBuilder<TKey, TValue> builder)
    {
        return builder.UseCompression(CompressionType.Brotli);
    }
}

/// <summary>
/// Extension methods for registering Brotli compression.
/// </summary>
public static class BrotliCompressionExtensions
{
    /// <summary>
    /// Registers the Brotli compression codec with the specified .NET compression level.
    /// <para>
    /// <strong>Important:</strong> Brotli is NOT a standard Kafka compression type.
    /// Both the producer and consumer must have the <c>Dekaf.Compression.Brotli</c> package installed.
    /// Standard Kafka clients (Java, librdkafka, Confluent.Kafka) cannot decompress Brotli-compressed messages.
    /// </para>
    /// </summary>
    /// <param name="registry">The compression codec registry.</param>
    /// <param name="compressionLevel">
    /// The .NET <see cref="CompressionLevel"/> to use. Default is <see cref="CompressionLevel.Fastest"/>.
    /// </param>
    /// <returns>The registry for fluent chaining.</returns>
    public static CompressionCodecRegistry AddBrotli(this CompressionCodecRegistry registry, CompressionLevel compressionLevel = CompressionLevel.Fastest)
    {
        registry.Register(new BrotliCompressionCodec(compressionLevel));
        return registry;
    }

}
