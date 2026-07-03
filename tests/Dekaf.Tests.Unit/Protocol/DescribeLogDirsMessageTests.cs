using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class DescribeLogDirsMessageTests
{
    [Test]
    public async Task DescribeLogDirsRequest_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var request = new DescribeLogDirsRequest
        {
            Topics =
            [
                new DescribeLogDirsTopic { Topic = "orders", Partitions = [0, 2] }
            ]
        };

        request.Write(ref writer, version: 4);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var topicsLength = reader.ReadUnsignedVarInt();
        var topic = reader.ReadCompactString();
        var partitionsLength = reader.ReadUnsignedVarInt();
        var firstPartition = reader.ReadInt32();
        var secondPartition = reader.ReadInt32();
        reader.SkipTaggedFields();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(topicsLength).IsEqualTo(2);
        await Assert.That(topic).IsEqualTo("orders");
        await Assert.That(partitionsLength).IsEqualTo(3);
        await Assert.That(firstPartition).IsEqualTo(0);
        await Assert.That(secondPartition).IsEqualTo(2);
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task DescribeLogDirsResponse_CanBeParsed()
    {
        var data = new List<byte>();
        AddInt32(data, 5);
        AddInt16(data, 0);
        AddCompactArrayLength(data, 1);
        AddInt16(data, 0);
        AddCompactString(data, "/var/lib/kafka/data");
        AddCompactArrayLength(data, 1);
        AddCompactString(data, "orders");
        AddCompactArrayLength(data, 1);
        AddInt32(data, 2);
        AddInt64(data, 4096);
        AddInt64(data, 12);
        AddBoolean(data, true);
        AddEmptyTags(data);
        AddEmptyTags(data);
        AddInt64(data, 100000);
        AddInt64(data, 50000);
        AddEmptyTags(data);
        AddEmptyTags(data);

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (DescribeLogDirsResponse)DescribeLogDirsResponse.Read(ref reader, version: 4);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(5);
        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Results.Count).IsEqualTo(1);
        await Assert.That(response.Results[0].ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Results[0].LogDir).IsEqualTo("/var/lib/kafka/data");
        await Assert.That(response.Results[0].TotalBytes).IsEqualTo(100000);
        await Assert.That(response.Results[0].UsableBytes).IsEqualTo(50000);
        await Assert.That(response.Results[0].Topics.Count).IsEqualTo(1);
        await Assert.That(response.Results[0].Topics[0].Name).IsEqualTo("orders");
        await Assert.That(response.Results[0].Topics[0].Partitions.Count).IsEqualTo(1);
        await Assert.That(response.Results[0].Topics[0].Partitions[0].PartitionIndex).IsEqualTo(2);
        await Assert.That(response.Results[0].Topics[0].Partitions[0].PartitionSize).IsEqualTo(4096);
        await Assert.That(response.Results[0].Topics[0].Partitions[0].OffsetLag).IsEqualTo(12);
        await Assert.That(response.Results[0].Topics[0].Partitions[0].IsFutureKey).IsTrue();
    }

    private static void AddInt16(List<byte> data, short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        AddBytes(data, buffer);
    }

    private static void AddInt32(List<byte> data, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        AddBytes(data, buffer);
    }

    private static void AddInt64(List<byte> data, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        AddBytes(data, buffer);
    }

    private static void AddBoolean(List<byte> data, bool value) => data.Add(value ? (byte)1 : (byte)0);
    private static void AddCompactArrayLength(List<byte> data, int count) => data.Add((byte)(count + 1));
    private static void AddEmptyTags(List<byte> data) => data.Add(0);

    private static void AddCompactString(List<byte> data, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        data.Add((byte)(bytes.Length + 1));
        data.AddRange(bytes);
    }

    private static void AddBytes(List<byte> data, ReadOnlySpan<byte> bytes)
    {
        foreach (var b in bytes)
        {
            data.Add(b);
        }
    }
}
