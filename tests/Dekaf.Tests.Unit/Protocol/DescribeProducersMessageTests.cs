using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

/// <summary>
/// Tests for DescribeProducers request/response encoding and decoding.
/// </summary>
public class DescribeProducersMessageTests
{
    [Test]
    public async Task DescribeProducersRequest_V0_WritesTopicsAndPartitions()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new DescribeProducersRequest
        {
            Topics =
            [
                new DescribeProducersRequestTopic
                {
                    Name = "orders",
                    PartitionIndexes = [0, 3]
                }
            ]
        };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var topicCount = reader.ReadUnsignedVarInt();
        var topicName = reader.ReadCompactString();
        var partitionIndexes = reader.ReadCompactArray(static (ref KafkaProtocolReader r) => r.ReadInt32());
        reader.SkipTaggedFields();
        reader.SkipTaggedFields();

        await Assert.That(topicCount).IsEqualTo(2);
        await Assert.That(topicName).IsEqualTo("orders");
        await Assert.That(partitionIndexes.Length).IsEqualTo(2);
        await Assert.That(partitionIndexes[0]).IsEqualTo(0);
        await Assert.That(partitionIndexes[1]).IsEqualTo(3);
    }

    [Test]
    public async Task DescribeProducersResponse_V0_ReadsActiveProducers()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        writer.WriteInt32(12);
        writer.WriteUnsignedVarInt(2);
        writer.WriteCompactString("orders");
        writer.WriteUnsignedVarInt(2);
        writer.WriteInt32(0);
        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteCompactNullableString(null);
        writer.WriteUnsignedVarInt(2);
        writer.WriteInt64(55L);
        writer.WriteInt32(4);
        writer.WriteInt32(101);
        writer.WriteInt64(987654321L);
        writer.WriteInt32(8);
        writer.WriteInt64(2048L);
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (DescribeProducersResponse)DescribeProducersResponse.Read(ref reader, version: 0);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(12);
        await Assert.That(response.Topics.Count).IsEqualTo(1);
        await Assert.That(response.Topics[0].Name).IsEqualTo("orders");
        await Assert.That(response.Topics[0].Partitions.Count).IsEqualTo(1);
        var partition = response.Topics[0].Partitions[0];
        await Assert.That(partition.PartitionIndex).IsEqualTo(0);
        await Assert.That(partition.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(partition.ErrorMessage).IsNull();
        await Assert.That(partition.ActiveProducers.Count).IsEqualTo(1);
        var producer = partition.ActiveProducers[0];
        await Assert.That(producer.ProducerId).IsEqualTo(55L);
        await Assert.That(producer.ProducerEpoch).IsEqualTo(4);
        await Assert.That(producer.LastSequence).IsEqualTo(101);
        await Assert.That(producer.LastTimestamp).IsEqualTo(987654321L);
        await Assert.That(producer.CoordinatorEpoch).IsEqualTo(8);
        await Assert.That(producer.CurrentTxnStartOffset).IsEqualTo(2048L);
    }
}
