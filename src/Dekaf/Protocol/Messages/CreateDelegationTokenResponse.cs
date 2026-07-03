namespace Dekaf.Protocol.Messages;

/// <summary>
/// CreateDelegationToken response (API key 38).
/// </summary>
public sealed class CreateDelegationTokenResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.CreateDelegationToken;
    public static short LowestSupportedVersion => 3;
    public static short HighestSupportedVersion => 3;

    public ErrorCode ErrorCode { get; init; }

    public required string PrincipalType { get; init; }

    public required string PrincipalName { get; init; }

    public required string TokenRequesterPrincipalType { get; init; }

    public required string TokenRequesterPrincipalName { get; init; }

    public long IssueTimestampMs { get; init; }

    public long ExpiryTimestampMs { get; init; }

    public long MaxTimestampMs { get; init; }

    public required string TokenId { get; init; }

    public required byte[] Hmac { get; init; }

    public int ThrottleTimeMs { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var principalType = reader.ReadCompactString() ?? string.Empty;
        var principalName = reader.ReadCompactString() ?? string.Empty;
        var tokenRequesterPrincipalType = reader.ReadCompactString() ?? string.Empty;
        var tokenRequesterPrincipalName = reader.ReadCompactString() ?? string.Empty;
        var issueTimestampMs = reader.ReadInt64();
        var expiryTimestampMs = reader.ReadInt64();
        var maxTimestampMs = reader.ReadInt64();
        var tokenId = reader.ReadCompactString() ?? string.Empty;
        var hmac = reader.ReadCompactBytes() ?? [];
        var throttleTimeMs = reader.ReadInt32();
        reader.SkipTaggedFields();

        return new CreateDelegationTokenResponse
        {
            ErrorCode = errorCode,
            PrincipalType = principalType,
            PrincipalName = principalName,
            TokenRequesterPrincipalType = tokenRequesterPrincipalType,
            TokenRequesterPrincipalName = tokenRequesterPrincipalName,
            IssueTimestampMs = issueTimestampMs,
            ExpiryTimestampMs = expiryTimestampMs,
            MaxTimestampMs = maxTimestampMs,
            TokenId = tokenId,
            Hmac = hmac,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}
