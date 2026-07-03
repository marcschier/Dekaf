namespace Dekaf.Protocol.Messages;

/// <summary>
/// AlterPartitionReassignments request (API key 45).
/// Reassigns partition replicas across brokers. A null replica set for a partition
/// cancels an ongoing reassignment for that partition.
/// </summary>
public sealed class AlterPartitionReassignmentsRequest : IKafkaRequest<AlterPartitionReassignmentsResponse>
{
    public static ApiKey ApiKey => ApiKey.AlterPartitionReassignments;
    public static short LowestSupportedVersion => 0;
    public static short HighestSupportedVersion => 0;

    /// <summary>
    /// The time in milliseconds to wait for the request to complete.
    /// </summary>
    public int TimeoutMs { get; init; } = 60000;

    /// <summary>
    /// The topics to reassign.
    /// </summary>
    public required IReadOnlyList<ReassignableTopic> Topics { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteInt32(TimeoutMs);

        writer.WriteCompactArray(
            Topics,
            static (ref KafkaProtocolWriter w, ReassignableTopic t, short v) => t.Write(ref w, v),
            version);

        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// A topic whose partitions are being reassigned.
/// </summary>
public sealed class ReassignableTopic
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The partitions to reassign.
    /// </summary>
    public required IReadOnlyList<ReassignablePartition> Partitions { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(Name);

        writer.WriteCompactArray(
            Partitions,
            static (ref KafkaProtocolWriter w, ReassignablePartition p, short v) => p.Write(ref w, v),
            version);

        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// A partition to reassign.
/// </summary>
public sealed class ReassignablePartition
{
    /// <summary>
    /// The partition index.
    /// </summary>
    public int PartitionIndex { get; init; }

    /// <summary>
    /// The broker IDs to place the replicas on, or null to cancel a pending reassignment.
    /// </summary>
    public IReadOnlyList<int>? Replicas { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteInt32(PartitionIndex);

        writer.WriteCompactNullableArray(
            Replicas,
            static (ref KafkaProtocolWriter w, int r) => w.WriteInt32(r));

        writer.WriteEmptyTaggedFields();
    }
}
