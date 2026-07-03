namespace Dekaf.Protocol.Messages;

/// <summary>
/// ListPartitionReassignments response (API key 46).
/// </summary>
public sealed class ListPartitionReassignmentsResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.ListPartitionReassignments;
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
    /// The ongoing reassignments for each topic.
    /// </summary>
    public required IReadOnlyList<OngoingTopicReassignment> Topics { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var errorMessage = reader.ReadCompactString();

        IReadOnlyList<OngoingTopicReassignment> topics;
        topics = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => OngoingTopicReassignment.Read(ref r, version)) ?? [];

        reader.SkipTaggedFields();

        return new ListPartitionReassignmentsResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            Topics = topics
        };
    }
}

/// <summary>
/// The ongoing reassignments for a single topic.
/// </summary>
public sealed class OngoingTopicReassignment
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The ongoing reassignments for each partition.
    /// </summary>
    public required IReadOnlyList<OngoingPartitionReassignment> Partitions { get; init; }

    public static OngoingTopicReassignment Read(ref KafkaProtocolReader reader, short version)
    {
        var name = reader.ReadCompactString() ?? string.Empty;

        IReadOnlyList<OngoingPartitionReassignment> partitions;
        partitions = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => OngoingPartitionReassignment.Read(ref r, version)) ?? [];

        reader.SkipTaggedFields();

        return new OngoingTopicReassignment
        {
            Name = name,
            Partitions = partitions
        };
    }
}

/// <summary>
/// The ongoing reassignment for a single partition.
/// </summary>
public sealed class OngoingPartitionReassignment
{
    /// <summary>
    /// The partition index.
    /// </summary>
    public int PartitionIndex { get; init; }

    /// <summary>
    /// The current replica set (broker IDs).
    /// </summary>
    public required IReadOnlyList<int> Replicas { get; init; }

    /// <summary>
    /// The replicas being added as part of this reassignment.
    /// </summary>
    public required IReadOnlyList<int> AddingReplicas { get; init; }

    /// <summary>
    /// The replicas being removed as part of this reassignment.
    /// </summary>
    public required IReadOnlyList<int> RemovingReplicas { get; init; }

    public static OngoingPartitionReassignment Read(ref KafkaProtocolReader reader, short version)
    {
        var partitionIndex = reader.ReadInt32();
        var replicas = reader.ReadCompactArray((ref KafkaProtocolReader r) => r.ReadInt32());
        var addingReplicas = reader.ReadCompactArray((ref KafkaProtocolReader r) => r.ReadInt32());
        var removingReplicas = reader.ReadCompactArray((ref KafkaProtocolReader r) => r.ReadInt32());

        reader.SkipTaggedFields();

        return new OngoingPartitionReassignment
        {
            PartitionIndex = partitionIndex,
            Replicas = replicas,
            AddingReplicas = addingReplicas,
            RemovingReplicas = removingReplicas
        };
    }
}
