namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeClientQuotas response (API key 48).
/// </summary>
public sealed class DescribeClientQuotasResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.DescribeClientQuotas;
    public static short LowestSupportedVersion => 1;
    public static short HighestSupportedVersion => 1;

    public int ThrottleTimeMs { get; init; }

    public ErrorCode ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    public required IReadOnlyList<DescribeClientQuotasEntry> Entries { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();
        var entries = reader.ReadCompactNullableArray(
            (ref KafkaProtocolReader r) => DescribeClientQuotasEntry.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new DescribeClientQuotasResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Entries = entries
        };
    }
}

public sealed class DescribeClientQuotasEntry
{
    public required IReadOnlyList<DescribeClientQuotasEntity> Entity { get; init; }

    public required IReadOnlyList<DescribeClientQuotasValue> Values { get; init; }

    public static DescribeClientQuotasEntry Read(ref KafkaProtocolReader reader, short version)
    {
        var entity = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeClientQuotasEntity.Read(ref r, version)) ?? [];
        var values = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeClientQuotasValue.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new DescribeClientQuotasEntry
        {
            Entity = entity,
            Values = values
        };
    }
}

public sealed class DescribeClientQuotasEntity
{
    public required string EntityType { get; init; }

    public string? EntityName { get; init; }

    public static DescribeClientQuotasEntity Read(ref KafkaProtocolReader reader, short version)
    {
        var entityType = reader.ReadCompactString() ?? string.Empty;
        var entityName = reader.ReadCompactString();
        reader.SkipTaggedFields();

        return new DescribeClientQuotasEntity
        {
            EntityType = entityType,
            EntityName = entityName
        };
    }
}

public sealed class DescribeClientQuotasValue
{
    public required string Key { get; init; }

    public double Value { get; init; }

    public static DescribeClientQuotasValue Read(ref KafkaProtocolReader reader, short version)
    {
        var key = reader.ReadCompactString() ?? string.Empty;
        var value = reader.ReadFloat64();
        reader.SkipTaggedFields();

        return new DescribeClientQuotasValue
        {
            Key = key,
            Value = value
        };
    }
}
