namespace Dekaf.Protocol.Messages;

/// <summary>
/// UpdateFeatures response (API key 57).
/// </summary>
public sealed class UpdateFeaturesResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.UpdateFeatures;
    public static short LowestSupportedVersion => 1;
    public static short HighestSupportedVersion => 1;

    public int ThrottleTimeMs { get; init; }

    public ErrorCode ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    public required IReadOnlyList<UpdateFeaturesResult> Results { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();
        var results = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => UpdateFeaturesResult.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new UpdateFeaturesResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Results = results
        };
    }
}

public sealed class UpdateFeaturesResult
{
    public required string Feature { get; init; }

    public ErrorCode ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    public static UpdateFeaturesResult Read(ref KafkaProtocolReader reader, short version)
    {
        var feature = reader.ReadCompactString() ?? string.Empty;
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();
        reader.SkipTaggedFields();

        return new UpdateFeaturesResult
        {
            Feature = feature,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}
