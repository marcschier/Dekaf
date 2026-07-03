using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class AlterClientQuotasMessageTests
{
    [Test]
    public async Task AlterClientQuotasRequest_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var request = new AlterClientQuotasRequest
        {
            Entries =
            [
                new AlterClientQuotasEntry
                {
                    Entity = [new AlterClientQuotasEntity { EntityType = "user", EntityName = "alice" }],
                    Ops = [new AlterClientQuotasOp { Key = "consumer_byte_rate", Value = 2048.25, Remove = false }]
                }
            ],
            ValidateOnly = true
        };

        request.Write(ref writer, version: 1);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var entriesLength = reader.ReadUnsignedVarInt();
        var entityLength = reader.ReadUnsignedVarInt();
        var entityType = reader.ReadCompactString();
        var entityName = reader.ReadCompactString();
        reader.SkipTaggedFields();
        var opsLength = reader.ReadUnsignedVarInt();
        var key = reader.ReadCompactString();
        var value = reader.ReadFloat64();
        var remove = reader.ReadBoolean();
        reader.SkipTaggedFields();
        reader.SkipTaggedFields();
        var validateOnly = reader.ReadBoolean();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(entriesLength).IsEqualTo(2);
        await Assert.That(entityLength).IsEqualTo(2);
        await Assert.That(entityType).IsEqualTo("user");
        await Assert.That(entityName).IsEqualTo("alice");
        await Assert.That(opsLength).IsEqualTo(2);
        await Assert.That(key).IsEqualTo("consumer_byte_rate");
        await Assert.That(value).IsEqualTo(2048.25);
        await Assert.That(remove).IsFalse();
        await Assert.That(validateOnly).IsTrue();
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task AlterClientQuotasResponse_CanBeParsed()
    {
        var data = new List<byte>();
        AddInt32(data, 7);
        AddCompactArrayLength(data, 1);
        AddInt16(data, 0);
        AddCompactNull(data);
        AddCompactArrayLength(data, 1);
        AddCompactString(data, "user");
        AddCompactString(data, "alice");
        AddEmptyTags(data);
        AddEmptyTags(data);
        AddEmptyTags(data);

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (AlterClientQuotasResponse)AlterClientQuotasResponse.Read(ref reader, version: 1);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(7);
        await Assert.That(response.Entries.Count).IsEqualTo(1);
        await Assert.That(response.Entries[0].ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Entries[0].ErrorMessage).IsNull();
        await Assert.That(response.Entries[0].Entity.Count).IsEqualTo(1);
        await Assert.That(response.Entries[0].Entity[0].EntityType).IsEqualTo("user");
        await Assert.That(response.Entries[0].Entity[0].EntityName).IsEqualTo("alice");
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
