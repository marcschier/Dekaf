namespace Dekaf.Protocol.Messages;

/// <summary>
/// ListPartitionReassignments request (API key 46).
/// Lists the ongoing partition reassignments in the cluster. A null topic list
/// requests all ongoing reassignments.
/// </summary>
public sealed class ListPartitionReassignmentsRequest : IKafkaRequest<ListPartitionReassignmentsResponse>
{
    public static ApiKey ApiKey => ApiKey.ListPartitionReassignments;
    public static short LowestSupportedVersion => 0;
    public static short HighestSupportedVersion => 0;

    /// <summary>
    /// The time in milliseconds to wait for the request to complete.
    /// </summary>
    public int TimeoutMs { get; init; } = 60000;

    /// <summary>
    /// The topics to list ongoing reassignments for, or null to list all.
    /// </summary>
    public IReadOnlyList<ListPartitionReassignmentsTopics>? Topics { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteInt32(TimeoutMs);

        writer.WriteCompactNullableArray(
            Topics,
            static (ref KafkaProtocolWriter w, ListPartitionReassignmentsTopics t, short v) => t.Write(ref w, v),
            version);

        writer.WriteEmptyTaggedFields();
    }
}

/// <summary>
/// A topic to list ongoing reassignments for.
/// </summary>
public sealed class ListPartitionReassignmentsTopics
{
    /// <summary>
    /// The topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The partitions to list ongoing reassignments for.
    /// </summary>
    public required IReadOnlyList<int> PartitionIndexes { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(Name);

        writer.WriteCompactArray(
            PartitionIndexes,
            static (ref KafkaProtocolWriter w, int p) => w.WriteInt32(p));

        writer.WriteEmptyTaggedFields();
    }
}
