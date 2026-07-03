using System.Collections.Concurrent;
using Dekaf.Consumer;
using Dekaf.Metadata;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Consumer;

public sealed class LogTruncationDetectionTests
{
    [Test]
    public async Task ComputeTruncationResetOffset_CurrentPositionPastEpochEnd_ReturnsEpochEndOffset()
    {
        var resetOffset = KafkaConsumer<string, string>.ComputeTruncationResetOffset(101, 100);

        await Assert.That(resetOffset).IsEqualTo(100);
    }

    [Test]
    public async Task ComputeTruncationResetOffset_CurrentPositionEqualToEpochEnd_ReturnsNull()
    {
        var resetOffset = KafkaConsumer<string, string>.ComputeTruncationResetOffset(100, 100);

        await Assert.That(resetOffset).IsNull();
    }

    [Test]
    public async Task ComputeTruncationResetOffset_CurrentPositionBeforeEpochEnd_ReturnsNull()
    {
        var resetOffset = KafkaConsumer<string, string>.ComputeTruncationResetOffset(99, 100);

        await Assert.That(resetOffset).IsNull();
    }

    [Test]
    public async Task ComputeTruncationResetOffset_UnknownEpochEndOffset_ReturnsNull()
    {
        var resetOffset = KafkaConsumer<string, string>.ComputeTruncationResetOffset(101, -1);

        await Assert.That(resetOffset).IsNull();
    }

    [Test]
    public async Task ComputeTruncationResetOffset_ZeroEpochEndOffsetWithPositivePosition_ReturnsZero()
    {
        var resetOffset = KafkaConsumer<string, string>.ComputeTruncationResetOffset(1, 0);

        await Assert.That(resetOffset).IsEqualTo(0);
    }

    [Test]
    public async Task BuildFetchResult_WithClusterLeaderEpoch_PopulatesCurrentLeaderEpoch()
    {
        var topicPartition = new TopicPartition("topic-a", 0);
        var template = new Dictionary<string, List<(FetchRequestPartition, TopicPartition)>>
        {
            ["topic-a"] =
            [
                (new FetchRequestPartition
                {
                    Partition = 0,
                    FetchOffset = 0,
                    CurrentLeaderEpoch = -1,
                    PartitionMaxBytes = 1_048_576
                }, topicPartition)
            ]
        };
        var fetchPositions = new ConcurrentDictionary<TopicPartition, long>();
        fetchPositions[topicPartition] = 42;
        var metadata = CreateMetadata(leaderEpoch: 7);

        var result = KafkaConsumer<string, string>.BuildFetchResult(
            template,
            fetchPositions,
            clusterMetadata: metadata);

        await Assert.That(result[0].Partitions[0].CurrentLeaderEpoch).IsEqualTo(7);
    }

    private static ClusterMetadata CreateMetadata(int leaderEpoch)
    {
        var metadata = new ClusterMetadata();
        metadata.Update(new MetadataResponse
        {
            Brokers =
            [
                new BrokerMetadata { NodeId = 1, Host = "broker-1", Port = 9092 }
            ],
            Topics =
            [
                new TopicMetadata
                {
                    Name = "topic-a",
                    ErrorCode = ErrorCode.None,
                    Partitions =
                    [
                        new PartitionMetadata
                        {
                            ErrorCode = ErrorCode.None,
                            PartitionIndex = 0,
                            LeaderId = 1,
                            LeaderEpoch = leaderEpoch,
                            ReplicaNodes = [1],
                            IsrNodes = [1]
                        }
                    ]
                }
            ]
        });

        return metadata;
    }
}
