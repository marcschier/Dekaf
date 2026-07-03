namespace Dekaf.Protocol.Messages;

/// <summary>
/// AlterClientQuotas response (API key 49).
/// </summary>
public sealed class AlterClientQuotasResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.AlterClientQuotas;
    public static short LowestSupportedVersion => 1;
    public static short HighestSupportedVersion => 1;

    public int ThrottleTimeMs { get; init; }

    public required IReadOnlyList<AlterClientQuotasResponseEntry> Entries { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var entries = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => AlterClientQuotasResponseEntry.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new AlterClientQuotasResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            Entries = entries
        };
    }
}

public sealed class AlterClientQuotasResponseEntry
{
    public ErrorCode ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    public required IReadOnlyList<AlterClientQuotasResponseEntity> Entity { get; init; }

    public static AlterClientQuotasResponseEntry Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();
        var entity = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => AlterClientQuotasResponseEntity.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new AlterClientQuotasResponseEntry
        {
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Entity = entity
        };
    }
}

public sealed class AlterClientQuotasResponseEntity
{
    public required string EntityType { get; init; }

    public string? EntityName { get; init; }

    public static AlterClientQuotasResponseEntity Read(ref KafkaProtocolReader reader, short version)
    {
        var entityType = reader.ReadCompactString() ?? string.Empty;
        var entityName = reader.ReadCompactString();
        reader.SkipTaggedFields();

        return new AlterClientQuotasResponseEntity
        {
            EntityType = entityType,
            EntityName = entityName
        };
    }
}
