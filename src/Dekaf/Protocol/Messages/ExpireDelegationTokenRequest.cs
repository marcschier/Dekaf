namespace Dekaf.Protocol.Messages;

/// <summary>
/// ExpireDelegationToken request (API key 40).
/// </summary>
public sealed class ExpireDelegationTokenRequest : IKafkaRequest<ExpireDelegationTokenResponse>
{
    public static ApiKey ApiKey => ApiKey.ExpireDelegationToken;
    public static short LowestSupportedVersion => 2;
    public static short HighestSupportedVersion => 2;

    public required byte[] Hmac { get; init; }

    public long ExpiryTimePeriodMs { get; init; } = -1;

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactBytes(Hmac);
        writer.WriteInt64(ExpiryTimePeriodMs);
        writer.WriteEmptyTaggedFields();
    }
}
