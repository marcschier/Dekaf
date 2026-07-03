using System.Collections.Concurrent;
using System.Reflection;
using Dekaf.Consumer;
using Dekaf.Metadata;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Consumer;

public sealed class RackAwareFetchTests
{
    [Test]
    public async Task ConsumerOptions_ClientRack_DefaultsToNull()
    {
        var options = new ConsumerOptions
        {
            BootstrapServers = ["localhost:9092"]
        };

        await Assert.That(options.ClientRack).IsNull();
    }

    [Test]
    public async Task ConsumerBuilder_WithClientRack_SetsOptions()
    {
        await using var consumer = Kafka.CreateConsumer<string, string>()
            .WithBootstrapServers("localhost:9092")
            .WithClientRack("rack-a")
            .Build();

        var options = GetOptions(consumer);

        await Assert.That(options.ClientRack).IsEqualTo("rack-a");
    }

    [Test]
    public async Task SelectFetchBrokerId_WithPreferredReplica_ChoosesReplica()
    {
        var partition = new TopicPartition("topic-a", 0);
        var metadata = CreateMetadata();
        var leader = metadata.GetBroker(1)!;
        var preferredReplicas = new ConcurrentDictionary<TopicPartition, int>();
        preferredReplicas[partition] = 2;

        var brokerId = KafkaConsumer<string, string>.SelectFetchBrokerId(
            partition,
            leader,
            metadata,
            preferredReplicas);

        await Assert.That(brokerId).IsEqualTo(2);
    }

    [Test]
    public async Task SelectFetchBrokerId_WithoutPreferredReplica_ChoosesLeader()
    {
        var partition = new TopicPartition("topic-a", 0);
        var metadata = CreateMetadata();
        var leader = metadata.GetBroker(1)!;
        var preferredReplicas = new ConcurrentDictionary<TopicPartition, int>();

        var brokerId = KafkaConsumer<string, string>.SelectFetchBrokerId(
            partition,
            leader,
            metadata,
            preferredReplicas);

        await Assert.That(brokerId).IsEqualTo(1);
    }

    [Test]
    public async Task SelectFetchBrokerId_WithUnknownPreferredReplica_FallsBackToLeader()
    {
        var partition = new TopicPartition("topic-a", 0);
        var metadata = CreateMetadata();
        var leader = metadata.GetBroker(1)!;
        var preferredReplicas = new ConcurrentDictionary<TopicPartition, int>();
        preferredReplicas[partition] = 99;

        var brokerId = KafkaConsumer<string, string>.SelectFetchBrokerId(
            partition,
            leader,
            metadata,
            preferredReplicas);

        await Assert.That(brokerId).IsEqualTo(1);
    }

    [Test]
    public async Task SelectFetchBrokerId_AfterPreferredReplicaCleared_FallsBackToLeader()
    {
        var partition = new TopicPartition("topic-a", 0);
        var metadata = CreateMetadata();
        var leader = metadata.GetBroker(1)!;
        var preferredReplicas = new ConcurrentDictionary<TopicPartition, int>();
        preferredReplicas[partition] = 2;
        preferredReplicas.TryRemove(partition, out _);

        var brokerId = KafkaConsumer<string, string>.SelectFetchBrokerId(
            partition,
            leader,
            metadata,
            preferredReplicas);

        await Assert.That(brokerId).IsEqualTo(1);
    }

    private static ConsumerOptions GetOptions(IKafkaConsumer<string, string> consumer)
    {
        var field = consumer.GetType().GetField("_options", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_options field not found");

        return (ConsumerOptions)field.GetValue(consumer)!;
    }

    private static ClusterMetadata CreateMetadata()
    {
        var metadata = new ClusterMetadata();
        metadata.Update(new MetadataResponse
        {
            Brokers =
            [
                new BrokerMetadata { NodeId = 1, Host = "broker-1", Port = 9092 },
                new BrokerMetadata { NodeId = 2, Host = "broker-2", Port = 9093 }
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
                            ReplicaNodes = [1, 2],
                            IsrNodes = [1, 2]
                        }
                    ]
                }
            ]
        });

        return metadata;
    }
}
