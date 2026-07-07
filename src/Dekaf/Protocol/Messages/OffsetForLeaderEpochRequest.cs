namespace Dekaf.Protocol.Messages;

/// <summary>
/// OffsetForLeaderEpoch request (API key 23).
/// Used by consumers to detect log truncation after a leader change (KIP-320):
/// for each partition the client supplies the last consumed leader epoch and the
/// broker returns the end offset of that epoch, allowing the consumer to reset its
/// position if it diverged from the new leader's log.
/// </summary>
public sealed class OffsetForLeaderEpochRequest : IKafkaRequest<OffsetForLeaderEpochResponse>
{
    public static ApiKey ApiKey => ApiKey.OffsetForLeaderEpoch;
    public static short LowestSupportedVersion => 4;
    public static short HighestSupportedVersion => 4;

    /// <summary>
    /// The broker ID of the follower, or -1 if this request is from a consumer.
    /// </summary>
    public int ReplicaId { get; init; } = -1;

    /// <summary>
    /// The topics to get the epoch end offsets for.
    /// </summary>
    public required IReadOnlyList<OffsetForLeaderTopic> Topics { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteInt32(ReplicaId);

        writer.WriteCompactArray(
            Topics,
            static (ref KafkaProtocolWriter w, OffsetForLeaderTopic t, short v) => t.Write(ref w, v),
            version);

        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// A topic in an OffsetForLeaderEpoch request.
/// </summary>
public sealed class OffsetForLeaderTopic
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partitions to get epoch end offsets for.
    /// </summary>
    public required IReadOnlyList<OffsetForLeaderPartition> Partitions { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(Topic);

        writer.WriteCompactArray(
            Partitions,
            static (ref KafkaProtocolWriter w, OffsetForLeaderPartition p, short v) => p.Write(ref w, v),
            version);

        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// A partition in an OffsetForLeaderEpoch request.
/// </summary>
public sealed class OffsetForLeaderPartition
{
    /// <summary>
    /// The partition index.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// The current leader epoch known to the client, or -1 if unknown.
    /// The broker fences the request with FENCED_LEADER_EPOCH / UNKNOWN_LEADER_EPOCH
    /// if this value is stale relative to its own metadata.
    /// </summary>
    public int CurrentLeaderEpoch { get; init; } = -1;

    /// <summary>
    /// The leader epoch to look up an end offset for (the last epoch the client consumed).
    /// </summary>
    public int LeaderEpoch { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteInt32(Partition);
        writer.WriteInt32(CurrentLeaderEpoch);
        writer.WriteInt32(LeaderEpoch);
        writer.WriteEmptyTaggedFields();
    }
}
