namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeDelegationToken request (API key 41).
/// </summary>
public sealed class DescribeDelegationTokenRequest : IKafkaRequest<DescribeDelegationTokenResponse>
{
    public static ApiKey ApiKey => ApiKey.DescribeDelegationToken;
    public static short LowestSupportedVersion => 3;
    public static short HighestSupportedVersion => 3;

    public IReadOnlyList<DelegationTokenPrincipalData>? Owners { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactNullableArray(
            Owners,
            static (ref KafkaProtocolWriter w, DelegationTokenPrincipalData o, short v) => o.Write(ref w, v),
            version);
        writer.WriteEmptyTaggedFields();
    }
}
