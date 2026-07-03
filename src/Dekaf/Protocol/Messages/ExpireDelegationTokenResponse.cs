namespace Dekaf.Protocol.Messages;

/// <summary>
/// ExpireDelegationToken response (API key 40).
/// </summary>
public sealed class ExpireDelegationTokenResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.ExpireDelegationToken;
    public static short LowestSupportedVersion => 2;
    public static short HighestSupportedVersion => 2;

    public ErrorCode ErrorCode { get; init; }

    public long ExpiryTimestampMs { get; init; }

    public int ThrottleTimeMs { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var expiryTimestampMs = reader.ReadInt64();
        var throttleTimeMs = reader.ReadInt32();
        reader.SkipTaggedFields();

        return new ExpireDelegationTokenResponse
        {
            ErrorCode = errorCode,
            ExpiryTimestampMs = expiryTimestampMs,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}
