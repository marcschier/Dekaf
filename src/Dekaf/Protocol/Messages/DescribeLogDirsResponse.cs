namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeLogDirs response (API key 35).
/// </summary>
public sealed class DescribeLogDirsResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.DescribeLogDirs;
    public static short LowestSupportedVersion => 4;
    public static short HighestSupportedVersion => 4;

    public int ThrottleTimeMs { get; init; }

    public ErrorCode ErrorCode { get; init; }

    public required IReadOnlyList<DescribeLogDirsResult> Results { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var results = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeLogDirsResult.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new DescribeLogDirsResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            Results = results
        };
    }
}

public sealed class DescribeLogDirsResult
{
    public ErrorCode ErrorCode { get; init; }

    public required string LogDir { get; init; }

    public required IReadOnlyList<DescribeLogDirsResponseTopic> Topics { get; init; }

    public long TotalBytes { get; init; }

    public long UsableBytes { get; init; }

    public static DescribeLogDirsResult Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var logDir = reader.ReadCompactString() ?? string.Empty;
        var topics = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeLogDirsResponseTopic.Read(ref r, version)) ?? [];
        var totalBytes = reader.ReadInt64();
        var usableBytes = reader.ReadInt64();
        reader.SkipTaggedFields();

        return new DescribeLogDirsResult
        {
            ErrorCode = errorCode,
            LogDir = logDir,
            Topics = topics,
            TotalBytes = totalBytes,
            UsableBytes = usableBytes
        };
    }
}

public sealed class DescribeLogDirsResponseTopic
{
    public required string Name { get; init; }

    public required IReadOnlyList<DescribeLogDirsPartition> Partitions { get; init; }

    public static DescribeLogDirsResponseTopic Read(ref KafkaProtocolReader reader, short version)
    {
        var name = reader.ReadCompactString() ?? string.Empty;
        var partitions = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeLogDirsPartition.Read(ref r, version)) ?? [];
        reader.SkipTaggedFields();

        return new DescribeLogDirsResponseTopic
        {
            Name = name,
            Partitions = partitions
        };
    }
}

public sealed class DescribeLogDirsPartition
{
    public int PartitionIndex { get; init; }

    public long PartitionSize { get; init; }

    public long OffsetLag { get; init; }

    public bool IsFutureKey { get; init; }

    public static DescribeLogDirsPartition Read(ref KafkaProtocolReader reader, short version)
    {
        var partitionIndex = reader.ReadInt32();
        var partitionSize = reader.ReadInt64();
        var offsetLag = reader.ReadInt64();
        var isFutureKey = reader.ReadBoolean();
        reader.SkipTaggedFields();

        return new DescribeLogDirsPartition
        {
            PartitionIndex = partitionIndex,
            PartitionSize = partitionSize,
            OffsetLag = offsetLag,
            IsFutureKey = isFutureKey
        };
    }
}
