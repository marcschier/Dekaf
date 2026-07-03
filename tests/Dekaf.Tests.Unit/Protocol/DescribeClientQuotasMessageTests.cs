using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class DescribeClientQuotasMessageTests
{
    [Test]
    public async Task DescribeClientQuotasRequest_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var request = new DescribeClientQuotasRequest
        {
            Components =
            [
                new DescribeClientQuotasComponent { EntityType = "user", MatchType = 0, Match = "alice" },
                new DescribeClientQuotasComponent { EntityType = "client-id", MatchType = 2, Match = null }
            ],
            Strict = true
        };

        request.Write(ref writer, version: 1);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var componentsLength = reader.ReadUnsignedVarInt();
        var firstEntityType = reader.ReadCompactString();
        var firstMatchType = reader.ReadInt8();
        var firstMatch = reader.ReadCompactString();
        reader.SkipTaggedFields();
        var secondEntityType = reader.ReadCompactString();
        var secondMatchType = reader.ReadInt8();
        var secondMatch = reader.ReadCompactString();
        reader.SkipTaggedFields();
        var strict = reader.ReadBoolean();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(componentsLength).IsEqualTo(3);
        await Assert.That(firstEntityType).IsEqualTo("user");
        await Assert.That(firstMatchType).IsEqualTo((sbyte)0);
        await Assert.That(firstMatch).IsEqualTo("alice");
        await Assert.That(secondEntityType).IsEqualTo("client-id");
        await Assert.That(secondMatchType).IsEqualTo((sbyte)2);
        await Assert.That(secondMatch).IsNull();
        await Assert.That(strict).IsTrue();
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task DescribeClientQuotasResponse_CanBeParsed()
    {
        var data = new List<byte>();
        AddInt32(data, 12);
        AddInt16(data, 0);
        AddCompactNull(data);
        AddCompactArrayLength(data, 1);
        AddCompactArrayLength(data, 2);
        AddCompactString(data, "user");
        AddCompactString(data, "alice");
        AddEmptyTags(data);
        AddCompactString(data, "client-id");
        AddCompactNull(data);
        AddEmptyTags(data);
        AddCompactArrayLength(data, 1);
        AddCompactString(data, "producer_byte_rate");
        AddFloat64(data, 1024.5);
        AddEmptyTags(data);
        AddEmptyTags(data);
        AddEmptyTags(data);

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (DescribeClientQuotasResponse)DescribeClientQuotasResponse.Read(ref reader, version: 1);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(12);
        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.ErrorMessage).IsNull();
        await Assert.That(response.Entries.Count).IsEqualTo(1);
        await Assert.That(response.Entries[0].Entity.Count).IsEqualTo(2);
        await Assert.That(response.Entries[0].Entity[0].EntityType).IsEqualTo("user");
        await Assert.That(response.Entries[0].Entity[0].EntityName).IsEqualTo("alice");
        await Assert.That(response.Entries[0].Entity[1].EntityType).IsEqualTo("client-id");
        await Assert.That(response.Entries[0].Entity[1].EntityName).IsNull();
        await Assert.That(response.Entries[0].Values.Count).IsEqualTo(1);
        await Assert.That(response.Entries[0].Values[0].Key).IsEqualTo("producer_byte_rate");
        await Assert.That(response.Entries[0].Values[0].Value).IsEqualTo(1024.5);
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

    private static void AddFloat64(List<byte> data, double value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteDoubleBigEndian(buffer, value);
        AddBytes(data, buffer);
    }

    private static void AddCompactArrayLength(List<byte> data, int count) => data.Add((byte)(count + 1));
    private static void AddCompactNull(List<byte> data) => data.Add(0);
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
