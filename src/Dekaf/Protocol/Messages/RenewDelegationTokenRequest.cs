namespace Dekaf.Protocol.Messages;

/// <summary>
/// RenewDelegationToken request (API key 39).
/// </summary>
public sealed class RenewDelegationTokenRequest : IKafkaRequest<RenewDelegationTokenResponse>
{
    public static ApiKey ApiKey => ApiKey.RenewDelegationToken;
    public static short LowestSupportedVersion => 2;
    public static short HighestSupportedVersion => 2;

    public required byte[] Hmac { get; init; }

    public long RenewPeriodMs { get; init; } = -1;

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactBytes(Hmac);
        writer.WriteInt64(RenewPeriodMs);
        writer.WriteEmptyTaggedFields();
    }
}
