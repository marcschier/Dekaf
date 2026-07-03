namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeClientQuotas request (API key 48).
/// </summary>
public sealed class DescribeClientQuotasRequest : IKafkaRequest<DescribeClientQuotasResponse>
{
    public static ApiKey ApiKey => ApiKey.DescribeClientQuotas;
    public static short LowestSupportedVersion => 1;
    public static short HighestSupportedVersion => 1;

    public required IReadOnlyList<DescribeClientQuotasComponent> Components { get; init; }

    public bool Strict { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactArray(
            Components,
            static (ref KafkaProtocolWriter w, DescribeClientQuotasComponent c, short v) => c.Write(ref w, v),
            version);
        writer.WriteBoolean(Strict);
        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// A quota entity filter component.
/// </summary>
public sealed class DescribeClientQuotasComponent
{
    public required string EntityType { get; init; }

    public sbyte MatchType { get; init; }

    public string? Match { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(EntityType);
        writer.WriteInt8(MatchType);
        writer.WriteCompactNullableString(Match);
        writer.WriteEmptyTaggedFields();
    }
}
