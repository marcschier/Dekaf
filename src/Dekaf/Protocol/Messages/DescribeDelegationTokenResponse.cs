namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeDelegationToken response (API key 41).
/// </summary>
public sealed class DescribeDelegationTokenResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.DescribeDelegationToken;
    public static short LowestSupportedVersion => 3;
    public static short HighestSupportedVersion => 3;

    public ErrorCode ErrorCode { get; init; }

    public required IReadOnlyList<DescribedDelegationToken> Tokens { get; init; }

    public int ThrottleTimeMs { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var tokens = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribedDelegationToken.Read(ref r, version)) ?? [];
        var throttleTimeMs = reader.ReadInt32();
        reader.SkipTaggedFields();

        return new DescribeDelegationTokenResponse
        {
            ErrorCode = errorCode,
            Tokens = tokens,
            ThrottleTimeMs = throttleTimeMs
        };
    }
}

/// <summary>
/// A delegation token returned by DescribeDelegationToken.
/// </summary>
public sealed class DescribedDelegationToken
{
    public required string PrincipalType { get; init; }

    public required string PrincipalName { get; init; }

    public required string TokenRequesterPrincipalType { get; init; }

    public required string TokenRequesterPrincipalName { get; init; }

    public long IssueTimestampMs { get; init; }

    public long ExpiryTimestampMs { get; init; }

    public long MaxTimestampMs { get; init; }

    public required string TokenId { get; init; }

    public required byte[] Hmac { get; init; }

    public required IReadOnlyList<DelegationTokenPrincipalData> Renewers { get; init; }

    public static DescribedDelegationToken Read(ref KafkaProtocolReader reader, short version)
    {
        var principalType = reader.ReadCompactString() ?? string.Empty;
        var principalName = reader.ReadCompactString() ?? string.Empty;
        var tokenRequesterPrincipalType = reader.ReadCompactString() ?? string.Empty;
        var tokenRequesterPrincipalName = reader.ReadCompactString() ?? string.Empty;
        var issueTimestampMs = reader.ReadInt64();
        var expiryTimestampMs = reader.ReadInt64();
        var maxTimestampMs = reader.ReadInt64();
        var tokenId = reader.ReadCompactString() ?? string.Empty;
        var hmac = reader.ReadCompactBytes() ?? [];
        var renewers = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DelegationTokenPrincipalData.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new DescribedDelegationToken
        {
            PrincipalType = principalType,
            PrincipalName = principalName,
            TokenRequesterPrincipalType = tokenRequesterPrincipalType,
            TokenRequesterPrincipalName = tokenRequesterPrincipalName,
            IssueTimestampMs = issueTimestampMs,
            ExpiryTimestampMs = expiryTimestampMs,
            MaxTimestampMs = maxTimestampMs,
            TokenId = tokenId,
            Hmac = hmac,
            Renewers = renewers
        };
    }
}
