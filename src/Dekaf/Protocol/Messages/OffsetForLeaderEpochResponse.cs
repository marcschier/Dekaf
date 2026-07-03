namespace Dekaf.Protocol.Messages;

/// <summary>
/// OffsetForLeaderEpoch response (API key 23).
/// Returns, per partition, the end offset of the requested leader epoch. A consumer
/// compares this end offset against its fetch position to detect and recover from log
/// truncation after a leader change (KIP-320).
/// </summary>
public sealed class OffsetForLeaderEpochResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.OffsetForLeaderEpoch;
    public static short LowestSupportedVersion => 4;
    public static short HighestSupportedVersion => 4;

    /// <summary>
    /// The duration in milliseconds for which the request was throttled.
    /// </summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>
    /// The results for each topic.
    /// </summary>
    public required IReadOnlyList<OffsetForLeaderTopicResult> Topics { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();

        IReadOnlyList<OffsetForLeaderTopicResult> topics;
        topics = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => OffsetForLeaderTopicResult.Read(ref r, version)) ?? [];

        reader.SkipTaggedFields();

        return new OffsetForLeaderEpochResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            Topics = topics
        };
    }
}

/// <summary>
/// Per-topic results in an OffsetForLeaderEpoch response.
/// </summary>
public sealed class OffsetForLeaderTopicResult
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The results for each partition.
    /// </summary>
    public required IReadOnlyList<OffsetForLeaderEpochEndOffset> Partitions { get; init; }

    public static OffsetForLeaderTopicResult Read(ref KafkaProtocolReader reader, short version)
    {
        var topic = reader.ReadCompactString() ?? string.Empty;

        IReadOnlyList<OffsetForLeaderEpochEndOffset> partitions;
        partitions = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => OffsetForLeaderEpochEndOffset.Read(ref r, version)) ?? [];

        reader.SkipTaggedFields();

        return new OffsetForLeaderTopicResult
        {
            Topic = topic,
            Partitions = partitions
        };
    }
}

/// <summary>
/// Per-partition epoch end offset in an OffsetForLeaderEpoch response.
/// </summary>
public sealed class OffsetForLeaderEpochEndOffset
{
    /// <summary>
    /// Error code for this partition, or None on success.
    /// </summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// The partition index.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// The leader epoch of the partition, or -1 if unknown.
    /// </summary>
    public int LeaderEpoch { get; init; } = -1;

    /// <summary>
    /// The end offset of the requested epoch, or -1 if unknown. If the consumer's fetch
    /// position is beyond this offset for the requested epoch, the log has been truncated.
    /// </summary>
    public long EndOffset { get; init; } = -1;

    public static OffsetForLeaderEpochEndOffset Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var partition = reader.ReadInt32();
        var leaderEpoch = reader.ReadInt32();
        var endOffset = reader.ReadInt64();

        reader.SkipTaggedFields();

        return new OffsetForLeaderEpochEndOffset
        {
            ErrorCode = errorCode,
            Partition = partition,
            LeaderEpoch = leaderEpoch,
            EndOffset = endOffset
        };
    }
}
