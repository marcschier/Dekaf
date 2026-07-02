using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text.Json;
using Dekaf.Serialization;

namespace Dekaf.SchemaRegistry;

/// <summary>
/// JSON serializer that integrates with Schema Registry.
/// Uses the Schema Registry wire format: [magic byte (0)] [schema ID (4 bytes)] [JSON payload].
/// </summary>
/// <remarks>
/// <para>
/// This serializer uses lazy caching for schema IDs. The first time a schema is needed for a
/// particular subject, a synchronous blocking call to the Schema Registry is made.
/// After the first fetch, subsequent serialization calls use the cached schema ID without blocking.
/// </para>
/// <para>
/// The blocking call includes a timeout to prevent indefinite hangs.
/// </para>
/// </remarks>
/// <typeparam name="T">The type to serialize.</typeparam>
public sealed class JsonSchemaRegistrySerializer<T> : ISerializer<T>, IAsyncDisposable
{
    private const byte MagicByte = 0x00;
    private static readonly TimeSpan SchemaRegistryTimeout = TimeSpan.FromSeconds(30);

    private readonly ISchemaRegistryClient _schemaRegistry;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly SubjectNameStrategy _subjectNameStrategy;
    private readonly ISubjectNameStrategy? _customSubjectNameStrategy;
    private readonly bool _autoRegisterSchemas;
    private readonly Schema _schema;
    private readonly bool _ownsClient;

    private int _cachedSchemaId = -1;
    private string? _cachedSubject;
    private readonly ConcurrentDictionary<SubjectCacheKey, SubjectSchemaIdCacheEntry> _subjectSchemaIdCache = new();
    private SubjectSchemaIdCacheEntry? _lastSubjectSchemaId;

    /// <summary>
    /// Creates a new JSON Schema Registry serializer.
    /// </summary>
    /// <param name="schemaRegistry">The Schema Registry client.</param>
    /// <param name="jsonSchema">The JSON schema string for type T.</param>
    /// <param name="jsonOptions">JSON serializer options.</param>
    /// <param name="subjectNameStrategy">Strategy for determining subject names.</param>
    /// <param name="autoRegisterSchemas">Whether to auto-register schemas.</param>
    /// <param name="ownsClient">Whether this serializer owns the client and should dispose it.</param>
    public JsonSchemaRegistrySerializer(
        ISchemaRegistryClient schemaRegistry,
        string jsonSchema,
        JsonSerializerOptions? jsonOptions = null,
        SubjectNameStrategy subjectNameStrategy = SubjectNameStrategy.TopicName,
        bool autoRegisterSchemas = true,
        bool ownsClient = false)
    {
        _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        _subjectNameStrategy = subjectNameStrategy;
        _autoRegisterSchemas = autoRegisterSchemas;
        _ownsClient = ownsClient;
        _schema = new Schema
        {
            SchemaType = SchemaType.Json,
            SchemaString = jsonSchema
        };
    }

    /// <summary>
    /// Creates a new JSON Schema Registry serializer with a custom subject name strategy.
    /// </summary>
    /// <param name="schemaRegistry">The Schema Registry client.</param>
    /// <param name="jsonSchema">The JSON schema string for type T.</param>
    /// <param name="customSubjectNameStrategy">Custom strategy for determining subject names.</param>
    /// <param name="jsonOptions">JSON serializer options.</param>
    /// <param name="autoRegisterSchemas">Whether to auto-register schemas.</param>
    /// <param name="ownsClient">Whether this serializer owns the client and should dispose it.</param>
    public JsonSchemaRegistrySerializer(
        ISchemaRegistryClient schemaRegistry,
        string jsonSchema,
        ISubjectNameStrategy customSubjectNameStrategy,
        JsonSerializerOptions? jsonOptions = null,
        bool autoRegisterSchemas = true,
        bool ownsClient = false)
    {
        _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
        _customSubjectNameStrategy = customSubjectNameStrategy ?? throw new ArgumentNullException(nameof(customSubjectNameStrategy));
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        _autoRegisterSchemas = autoRegisterSchemas;
        _ownsClient = ownsClient;
        _schema = new Schema
        {
            SchemaType = SchemaType.Json,
            SchemaString = jsonSchema
        };
    }

    public void Serialize<TWriter>(T value, ref TWriter destination, SerializationContext context)
        where TWriter : IBufferWriter<byte>, allows ref struct
    {
        var schemaId = GetSchemaIdForContext(context.Topic, context.Component == SerializationComponent.Key);

        // Serialize to JSON
        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(value, _jsonOptions);

        // Write wire format: [0x00] [schema ID] [JSON payload]
        var totalSize = 1 + 4 + jsonBytes.Length;
        var span = destination.GetSpan(totalSize);

        span[0] = MagicByte;
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(1, 4), schemaId);
        jsonBytes.AsSpan().CopyTo(span.Slice(5));

        destination.Advance(totalSize);
    }

    private int GetSchemaIdForContext(string topic, bool isKey)
    {
        var last = _lastSubjectSchemaId;
        if (last is not null && last.Matches(topic, isKey))
            return last.SchemaId;

        var key = new SubjectCacheKey(topic, isKey);
        if (_subjectSchemaIdCache.TryGetValue(key, out var cached))
        {
            _lastSubjectSchemaId = cached;
            return cached.SchemaId;
        }

        var subject = GetSubjectName(topic, isKey);
        var schemaId = GetSchemaIdSync(subject);
        var entry = new SubjectSchemaIdCacheEntry(topic, isKey, schemaId);
        _subjectSchemaIdCache[key] = entry;
        _lastSubjectSchemaId = entry;
        return schemaId;
    }

    private int GetSchemaIdSync(string subject)
    {
        if (_cachedSchemaId >= 0 && _cachedSubject == subject)
            return _cachedSchemaId;

        var task = _autoRegisterSchemas
            ? _schemaRegistry.GetOrRegisterSchemaAsync(subject, _schema)
            : _schemaRegistry.GetSchemaBySubjectAsync(subject).ContinueWith(t => t.Result.Id, TaskScheduler.Default);

        // Add timeout to prevent indefinite blocking
        var id = task.WaitAsync(SchemaRegistryTimeout).ConfigureAwait(false).GetAwaiter().GetResult();

        _cachedSchemaId = id;
        _cachedSubject = subject;

        return id;
    }

    private string GetSubjectName(string topic, bool isKey)
    {
        if (_customSubjectNameStrategy is not null)
        {
            return _customSubjectNameStrategy.GetSubjectName(topic, typeof(T).FullName, isKey);
        }

        var suffix = isKey ? "-key" : "-value";
        return _subjectNameStrategy switch
        {
            SubjectNameStrategy.TopicName => topic + suffix,
            SubjectNameStrategy.RecordName => typeof(T).FullName + suffix,
            SubjectNameStrategy.TopicRecordName => $"{topic}-{typeof(T).FullName}{suffix}",
            _ => topic + suffix
        };
    }

    private readonly record struct SubjectCacheKey(string Topic, bool IsKey);

    private sealed record SubjectSchemaIdCacheEntry(string Topic, bool IsKey, int SchemaId)
    {
        public bool Matches(string topic, bool isKey) =>
            IsKey == isKey && string.Equals(Topic, topic, StringComparison.Ordinal);
    }

    public ValueTask DisposeAsync()
    {
        if (_ownsClient)
            _schemaRegistry.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// JSON deserializer that integrates with Schema Registry.
/// Handles the wire format: [magic byte (0)] [schema ID (4 bytes)] [JSON payload].
/// </summary>
/// <remarks>
/// <para>
/// This deserializer fetches the schema from Schema Registry for validation on first access.
/// Schemas are cached internally by the Schema Registry client after first fetch.
/// </para>
/// <para>
/// The blocking call includes a timeout to prevent indefinite hangs.
/// </para>
/// </remarks>
/// <typeparam name="T">The type to deserialize.</typeparam>
public sealed class JsonSchemaRegistryDeserializer<T> : IDeserializer<T>, IAsyncDisposable
{
    private const byte MagicByte = 0x00;
    private static readonly TimeSpan SchemaRegistryTimeout = TimeSpan.FromSeconds(30);

    private readonly ISchemaRegistryClient _schemaRegistry;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly bool _ownsClient;

    /// <summary>
    /// Creates a new JSON Schema Registry deserializer.
    /// </summary>
    /// <param name="schemaRegistry">The Schema Registry client.</param>
    /// <param name="jsonOptions">JSON serializer options.</param>
    /// <param name="ownsClient">Whether this deserializer owns the client and should dispose it.</param>
    public JsonSchemaRegistryDeserializer(
        ISchemaRegistryClient schemaRegistry,
        JsonSerializerOptions? jsonOptions = null,
        bool ownsClient = false)
    {
        _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
        _jsonOptions = jsonOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        _ownsClient = ownsClient;
    }

    public T Deserialize(ReadOnlyMemory<byte> data, SerializationContext context)
    {
        var span = data.Span;

        if (span.Length < 5)
            throw new InvalidOperationException("Message too short to contain Schema Registry wire format");

        if (span[0] != MagicByte)
            throw new InvalidOperationException($"Unknown magic byte: {span[0]}. Expected Schema Registry format.");

        var schemaId = BinaryPrimitives.ReadInt32BigEndian(span.Slice(1, 4));

        // Verify the schema exists. Cache hits avoid Task allocation and sync-over-async.
        _ = _schemaRegistry.GetSchemaSync(schemaId, SchemaRegistryTimeout);

        // Extract JSON payload and deserialize
        return JsonSerializer.Deserialize<T>(span.Slice(5), _jsonOptions)!;
    }

    public ValueTask DisposeAsync()
    {
        if (_ownsClient)
            _schemaRegistry.Dispose();
        return ValueTask.CompletedTask;
    }
}
