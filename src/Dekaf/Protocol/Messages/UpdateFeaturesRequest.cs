namespace Dekaf.Protocol.Messages;

/// <summary>
/// UpdateFeatures request (API key 57).
/// </summary>
public sealed class UpdateFeaturesRequest : IKafkaRequest<UpdateFeaturesResponse>
{
    public static ApiKey ApiKey => ApiKey.UpdateFeatures;
    public static short LowestSupportedVersion => 1;
    public static short HighestSupportedVersion => 1;

    public int TimeoutMs { get; init; } = 60000;

    public required IReadOnlyList<UpdateFeaturesFeatureUpdate> FeatureUpdates { get; init; }

    public bool ValidateOnly { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteInt32(TimeoutMs);
        writer.WriteCompactArray(
            FeatureUpdates,
            static (ref KafkaProtocolWriter w, UpdateFeaturesFeatureUpdate f, short v) => f.Write(ref w, v),
            version);
        writer.WriteBoolean(ValidateOnly);
        writer.WriteEmptyTaggedFields();
    }
}

public sealed class UpdateFeaturesFeatureUpdate
{
    public required string Feature { get; init; }

    public short MaxVersionLevel { get; init; }

    public sbyte UpgradeType { get; init; } = 1;

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(Feature);
        writer.WriteInt16(MaxVersionLevel);
        writer.WriteInt8(UpgradeType);
        writer.WriteEmptyTaggedFields();
    }
}
