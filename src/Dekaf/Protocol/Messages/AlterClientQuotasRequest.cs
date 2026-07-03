namespace Dekaf.Protocol.Messages;

/// <summary>
/// AlterClientQuotas request (API key 49).
/// </summary>
public sealed class AlterClientQuotasRequest : IKafkaRequest<AlterClientQuotasResponse>
{
    public static ApiKey ApiKey => ApiKey.AlterClientQuotas;
    public static short LowestSupportedVersion => 1;
    public static short HighestSupportedVersion => 1;

    public required IReadOnlyList<AlterClientQuotasEntry> Entries { get; init; }

    public bool ValidateOnly { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactArray(
            Entries,
            static (ref KafkaProtocolWriter w, AlterClientQuotasEntry e, short v) => e.Write(ref w, v),
            version);
        writer.WriteBoolean(ValidateOnly);
        writer.WriteEmptyTaggedFields();
    }
}

public sealed class AlterClientQuotasEntry
{
    public required IReadOnlyList<AlterClientQuotasEntity> Entity { get; init; }

    public required IReadOnlyList<AlterClientQuotasOp> Ops { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactArray(
            Entity,
            static (ref KafkaProtocolWriter w, AlterClientQuotasEntity e, short v) => e.Write(ref w, v),
            version);
        writer.WriteCompactArray(
            Ops,
            static (ref KafkaProtocolWriter w, AlterClientQuotasOp o, short v) => o.Write(ref w, v),
            version);
        writer.WriteEmptyTaggedFields();
    }
}

public sealed class AlterClientQuotasEntity
{
    public required string EntityType { get; init; }

    public string? EntityName { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(EntityType);
        writer.WriteCompactNullableString(EntityName);
        writer.WriteEmptyTaggedFields();
    }
}

public sealed class AlterClientQuotasOp
{
    public required string Key { get; init; }

    public double Value { get; init; }

    public bool Remove { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(Key);
        writer.WriteFloat64(Value);
        writer.WriteBoolean(Remove);
        writer.WriteEmptyTaggedFields();
    }
}
