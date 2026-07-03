namespace Dekaf.Protocol.Messages;

/// <summary>
/// AlterPartitionReassignments response (API key 45).
/// </summary>
public sealed class AlterPartitionReassignmentsResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.AlterPartitionReassignments;
    public static short LowestSupportedVersion => 0;
    public static short HighestSupportedVersion => 0;

    /// <summary>
    /// The duration in milliseconds for which the request was throttled.
    /// </summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>
    /// The top-level error code, or None on success.
    /// </summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// The top-level error message, or null on success.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// The results for each topic.
    /// </summary>
    public required IReadOnlyList<ReassignableTopicResponse> Responses { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        IReadOnlyList<ReassignableTopicResponse> responses;
        responses = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => ReassignableTopicResponse.Read(ref r, version)) ?? [];

        reader.SkipTaggedFields();

        return new AlterPartitionReassignmentsResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Responses = responses
        };
    }
}

/// <summary>
/// Per-topic results in an AlterPartitionReassignments response.
/// </summary>
public sealed class ReassignableTopicResponse
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The results for each partition.
    /// </summary>
    public required IReadOnlyList<ReassignablePartitionResponse> Partitions { get; init; }

    public static ReassignableTopicResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var name = reader.ReadCompactString() ?? string.Empty;

        IReadOnlyList<ReassignablePartitionResponse> partitions;
        partitions = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => ReassignablePartitionResponse.Read(ref r, version)) ?? [];

        reader.SkipTaggedFields();

        return new ReassignableTopicResponse
        {
            Name = name,
            Partitions = partitions
        };
    }
}

/// <summary>
/// Per-partition result in an AlterPartitionReassignments response.
/// </summary>
public sealed class ReassignablePartitionResponse
{
    /// <summary>
    /// The partition index.
    /// </summary>
    public int PartitionIndex { get; init; }

    /// <summary>
    /// The error code for this partition, or None on success.
    /// </summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// The error message for this partition, or null on success.
    /// </summary>
    public string? ErrorMessage { get; init; }

    public static ReassignablePartitionResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var partitionIndex = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        reader.SkipTaggedFields();

        return new ReassignablePartitionResponse
        {
            PartitionIndex = partitionIndex,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}
