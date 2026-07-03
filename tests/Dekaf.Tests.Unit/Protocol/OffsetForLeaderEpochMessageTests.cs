using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

/// <summary>
/// Tests for OffsetForLeaderEpoch request/response encoding and decoding (KIP-320).
/// </summary>
public class OffsetForLeaderEpochMessageTests
{
    [Test]
    public async Task OffsetForLeaderEpochRequest_V4_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new OffsetForLeaderEpochRequest
        {
            ReplicaId = -1,
            Topics =
            [
                new OffsetForLeaderTopic
                {
                    Topic = "test",
                    Partitions =
                    [
                        new OffsetForLeaderPartition
                        {
                            Partition = 5,
                            CurrentLeaderEpoch = 10,
                            LeaderEpoch = 7
                        }
                    ]
                }
            ]
        };
        request.Write(ref writer, version: 4);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var replicaId = reader.ReadInt32();
        var topicsLength = reader.ReadUnsignedVarInt();
        var topic = reader.ReadCompactString();
        var partitionsLength = reader.ReadUnsignedVarInt();
        var partition = reader.ReadInt32();
        var currentLeaderEpoch = reader.ReadInt32();
        var leaderEpoch = reader.ReadInt32();

        await Assert.That(replicaId).IsEqualTo(-1);
        await Assert.That(topicsLength).IsEqualTo(2); // 1 topic + 1
        await Assert.That(topic).IsEqualTo("test");
        await Assert.That(partitionsLength).IsEqualTo(2); // 1 partition + 1
        await Assert.That(partition).IsEqualTo(5);
        await Assert.That(currentLeaderEpoch).IsEqualTo(10);
        await Assert.That(leaderEpoch).IsEqualTo(7);
    }

    [Test]
    public async Task OffsetForLeaderEpochResponse_V4_CanBeParsed()
    {
        var data = new List<byte>();
        // ThrottleTimeMs (INT32)
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00 });
        // Topics COMPACT_ARRAY (length + 1 = 2)
        data.Add(0x02);
        // Topic (COMPACT_STRING length + 1 = 5)
        data.Add(0x05);
        data.AddRange("test"u8.ToArray());
        // Partitions COMPACT_ARRAY (length + 1 = 2)
        data.Add(0x02);
        // ErrorCode (INT16) = None
        data.AddRange(new byte[] { 0x00, 0x00 });
        // Partition (INT32) = 5
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x05 });
        // LeaderEpoch (INT32) = 7
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x07 });
        // EndOffset (INT64) = 100
        data.AddRange(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64 });
        // Partition tagged fields
        data.Add(0x00);
        // Topic tagged fields
        data.Add(0x00);
        // Response tagged fields
        data.Add(0x00);

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (OffsetForLeaderEpochResponse)OffsetForLeaderEpochResponse.Read(ref reader, version: 4);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(0);
        await Assert.That(response.Topics.Count).IsEqualTo(1);
        await Assert.That(response.Topics[0].Topic).IsEqualTo("test");
        await Assert.That(response.Topics[0].Partitions.Count).IsEqualTo(1);

        var p = response.Topics[0].Partitions[0];
        await Assert.That(p.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(p.Partition).IsEqualTo(5);
        await Assert.That(p.LeaderEpoch).IsEqualTo(7);
        await Assert.That(p.EndOffset).IsEqualTo(100L);
    }

    [Test]
    public async Task OffsetForLeaderEpochRequest_ExposesApiKeyAndVersions()
    {
        await Assert.That(OffsetForLeaderEpochRequest.ApiKey).IsEqualTo(ApiKey.OffsetForLeaderEpoch);
        await Assert.That(OffsetForLeaderEpochRequest.HighestSupportedVersion).IsEqualTo((short)4);
        await Assert.That(OffsetForLeaderEpochResponse.ApiKey).IsEqualTo(ApiKey.OffsetForLeaderEpoch);
    }
}
