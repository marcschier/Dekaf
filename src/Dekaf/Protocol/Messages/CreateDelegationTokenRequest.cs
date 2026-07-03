namespace Dekaf.Protocol.Messages;

/// <summary>
/// CreateDelegationToken request (API key 38).
/// </summary>
public sealed class CreateDelegationTokenRequest : IKafkaRequest<CreateDelegationTokenResponse>
{
    public static ApiKey ApiKey => ApiKey.CreateDelegationToken;
    public static short LowestSupportedVersion => 3;
    public static short HighestSupportedVersion => 3;

    public string? OwnerPrincipalType { get; init; }

    public string? OwnerPrincipalName { get; init; }

    public required IReadOnlyList<DelegationTokenPrincipalData> Renewers { get; init; }

    public long MaxLifetimeMs { get; init; } = -1;

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactNullableString(OwnerPrincipalType);
        writer.WriteCompactNullableString(OwnerPrincipalName);
        writer.WriteCompactArray(
            Renewers,
            static (ref KafkaProtocolWriter w, DelegationTokenPrincipalData r, short v) => r.Write(ref w, v),
            version);
        writer.WriteInt64(MaxLifetimeMs);
        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// Kafka principal data used by delegation token protocol messages.
/// </summary>
public sealed class DelegationTokenPrincipalData
{
    public required string PrincipalType { get; init; }

    public required string PrincipalName { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(PrincipalType);
        writer.WriteCompactString(PrincipalName);
        writer.WriteEmptyTaggedFields();
    }

    public static DelegationTokenPrincipalData Read(ref KafkaProtocolReader reader, short version)
    {
        var principalType = reader.ReadCompactString() ?? string.Empty;
        var principalName = reader.ReadCompactString() ?? string.Empty;
        reader.SkipTaggedFields();

        return new DelegationTokenPrincipalData
        {
            PrincipalType = principalType,
            PrincipalName = principalName
        };
    }
}
