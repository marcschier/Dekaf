namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeLogDirs request (API key 35).
/// </summary>
public sealed class DescribeLogDirsRequest : IKafkaRequest<DescribeLogDirsResponse>
{
    public static ApiKey ApiKey => ApiKey.DescribeLogDirs;
    public static short LowestSupportedVersion => 4;
    public static short HighestSupportedVersion => 4;

    public IReadOnlyList<DescribeLogDirsTopic>? Topics { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactNullableArray(
            Topics,
            static (ref KafkaProtocolWriter w, DescribeLogDirsTopic t, short v) => t.Write(ref w, v),
            version);
        writer.WriteEmptyTaggedFields();
    }
}

public sealed class DescribeLogDirsTopic
{
    public required string Topic { get; init; }

    public required IReadOnlyList<int> Partitions { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactString(Topic);
        writer.WriteCompactArray(
            Partitions,
            static (ref KafkaProtocolWriter w, int p) => w.WriteInt32(p));
        writer.WriteEmptyTaggedFields();
    }
}
