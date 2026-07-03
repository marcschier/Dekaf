using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

/// <summary>
/// Tests for AlterPartitionReassignments and ListPartitionReassignments encoding/decoding.
/// </summary>
public class PartitionReassignmentMessageTests
{
    [Test]
    public async Task AlterPartitionReassignmentsRequest_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new AlterPartitionReassignmentsRequest
        {
            TimeoutMs = 60000,
            Topics =
            [
                new ReassignableTopic
                {
                    Name = "test",
                    Partitions =
                    [
                        new ReassignablePartition
                        {
                            PartitionIndex = 0,
                            Replicas = [1, 2, 3]
                        }
                    ]
                }
            ]
        };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var timeoutMs = reader.ReadInt32();
        var topicsLength = reader.ReadUnsignedVarInt();
        var name = reader.ReadCompactString();
        var partitionsLength = reader.ReadUnsignedVarInt();
        var partitionIndex = reader.ReadInt32();
        var replicasLength = reader.ReadUnsignedVarInt();
        var r0 = reader.ReadInt32();
        var r1 = reader.ReadInt32();
        var r2 = reader.ReadInt32();

        await Assert.That(timeoutMs).IsEqualTo(60000);
        await Assert.That(topicsLength).IsEqualTo(2); // 1 topic + 1
        await Assert.That(name).IsEqualTo("test");
        await Assert.That(partitionsLength).IsEqualTo(2); // 1 partition + 1
        await Assert.That(partitionIndex).IsEqualTo(0);
        await Assert.That(replicasLength).IsEqualTo(4); // 3 replicas + 1
        await Assert.That(r0).IsEqualTo(1);
        await Assert.That(r1).IsEqualTo(2);
        await Assert.That(r2).IsEqualTo(3);
    }

    [Test]
    public async Task AlterPartitionReassignmentsRequest_NullReplicas_EncodesAsNull()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new AlterPartitionReassignmentsRequest
        {
            Topics =
            [
                new ReassignableTopic
                {
                    Name = "t",
                    Partitions = [new ReassignablePartition { PartitionIndex = 2, Replicas = null }]
                }
            ]
        };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        reader.ReadInt32(); // TimeoutMs
        reader.ReadUnsignedVarInt(); // topics length
        reader.ReadCompactString(); // name
        reader.ReadUnsignedVarInt(); // partitions length
        reader.ReadInt32(); // partition index
        var replicasLength = reader.ReadUnsignedVarInt();

        await Assert.That(replicasLength).IsEqualTo(0); // null
    }

    [Test]
    public async Task AlterPartitionReassignmentsResponse_CanBeParsed()
    {
        var data = new List<byte>();
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00 }); // ThrottleTimeMs
        data.AddRange(new byte[] { 0x00, 0x00 }); // ErrorCode = None
        data.Add(0x00); // ErrorMessage = null
        data.Add(0x02); // Responses COMPACT_ARRAY (1 topic)
        data.Add(0x05); // topic name len+1
        data.AddRange("test"u8.ToArray());
        data.Add(0x02); // Partitions COMPACT_ARRAY (1 partition)
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00 }); // PartitionIndex
        data.AddRange(new byte[] { 0x00, 0x00 }); // ErrorCode = None
        data.Add(0x00); // ErrorMessage = null
        data.Add(0x00); // partition tagged fields
        data.Add(0x00); // topic tagged fields
        data.Add(0x00); // response tagged fields

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (AlterPartitionReassignmentsResponse)
            AlterPartitionReassignmentsResponse.Read(ref reader, version: 0);

        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Responses.Count).IsEqualTo(1);
        await Assert.That(response.Responses[0].Name).IsEqualTo("test");
        await Assert.That(response.Responses[0].Partitions[0].PartitionIndex).IsEqualTo(0);
        await Assert.That(response.Responses[0].Partitions[0].ErrorCode).IsEqualTo(ErrorCode.None);
    }

    [Test]
    public async Task ListPartitionReassignmentsRequest_NullTopics_EncodesAsNull()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new ListPartitionReassignmentsRequest { TimeoutMs = 60000, Topics = null };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        reader.ReadInt32(); // TimeoutMs
        var topicsLength = reader.ReadUnsignedVarInt();

        await Assert.That(topicsLength).IsEqualTo(0); // null
    }

    [Test]
    public async Task ListPartitionReassignmentsRequest_WritesTopics()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new ListPartitionReassignmentsRequest
        {
            Topics =
            [
                new ListPartitionReassignmentsTopics { Name = "test", PartitionIndexes = [0, 1] }
            ]
        };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        reader.ReadInt32(); // TimeoutMs
        var topicsLength = reader.ReadUnsignedVarInt();
        var name = reader.ReadCompactString();
        var partitionsLength = reader.ReadUnsignedVarInt();
        var p0 = reader.ReadInt32();
        var p1 = reader.ReadInt32();

        await Assert.That(topicsLength).IsEqualTo(2); // 1 topic + 1
        await Assert.That(name).IsEqualTo("test");
        await Assert.That(partitionsLength).IsEqualTo(3); // 2 partitions + 1
        await Assert.That(p0).IsEqualTo(0);
        await Assert.That(p1).IsEqualTo(1);
    }

    [Test]
    public async Task ListPartitionReassignmentsResponse_CanBeParsed()
    {
        var data = new List<byte>();
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00 }); // ThrottleTimeMs
        data.AddRange(new byte[] { 0x00, 0x00 }); // ErrorCode = None
        data.Add(0x00); // ErrorMessage = null
        data.Add(0x02); // Topics COMPACT_ARRAY (1 topic)
        data.Add(0x05); // topic name len+1
        data.AddRange("test"u8.ToArray());
        data.Add(0x02); // Partitions COMPACT_ARRAY (1 partition)
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00 }); // PartitionIndex
        // Replicas COMPACT_ARRAY [1,2,3]
        data.Add(0x04);
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x01 });
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x02 });
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x03 });
        // AddingReplicas COMPACT_ARRAY [4]
        data.Add(0x02);
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x04 });
        // RemovingReplicas COMPACT_ARRAY []
        data.Add(0x01);
        data.Add(0x00); // partition tagged fields
        data.Add(0x00); // topic tagged fields
        data.Add(0x00); // response tagged fields

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (ListPartitionReassignmentsResponse)
            ListPartitionReassignmentsResponse.Read(ref reader, version: 0);

        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Topics.Count).IsEqualTo(1);
        var partition = response.Topics[0].Partitions[0];
        await Assert.That(partition.PartitionIndex).IsEqualTo(0);
        await Assert.That(partition.Replicas.Count).IsEqualTo(3);
        await Assert.That(partition.Replicas[0]).IsEqualTo(1);
        await Assert.That(partition.Replicas[1]).IsEqualTo(2);
        await Assert.That(partition.Replicas[2]).IsEqualTo(3);
        await Assert.That(partition.AddingReplicas.Count).IsEqualTo(1);
        await Assert.That(partition.AddingReplicas[0]).IsEqualTo(4);
        await Assert.That(partition.RemovingReplicas.Count).IsEqualTo(0);
    }
}
