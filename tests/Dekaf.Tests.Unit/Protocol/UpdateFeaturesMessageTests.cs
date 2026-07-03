using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class UpdateFeaturesMessageTests
{
    [Test]
    public async Task UpdateFeaturesRequest_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var request = new UpdateFeaturesRequest
        {
            TimeoutMs = 45000,
            FeatureUpdates =
            [
                new UpdateFeaturesFeatureUpdate { Feature = "metadata.version", MaxVersionLevel = 20, UpgradeType = 1 }
            ],
            ValidateOnly = true
        };

        request.Write(ref writer, version: 1);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var timeoutMs = reader.ReadInt32();
        var updatesLength = reader.ReadUnsignedVarInt();
        var feature = reader.ReadCompactString();
        var maxVersionLevel = reader.ReadInt16();
        var upgradeType = reader.ReadInt8();
        reader.SkipTaggedFields();
        var validateOnly = reader.ReadBoolean();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(timeoutMs).IsEqualTo(45000);
        await Assert.That(updatesLength).IsEqualTo(2);
        await Assert.That(feature).IsEqualTo("metadata.version");
        await Assert.That(maxVersionLevel).IsEqualTo((short)20);
        await Assert.That(upgradeType).IsEqualTo((sbyte)1);
        await Assert.That(validateOnly).IsTrue();
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task UpdateFeaturesResponse_CanBeParsed()
    {
        var data = new List<byte>();
        AddInt32(data, 3);
        AddInt16(data, 0);
        AddCompactNull(data);
        AddCompactArrayLength(data, 1);
        AddCompactString(data, "metadata.version");
        AddInt16(data, 0);
        AddCompactNull(data);
        AddEmptyTags(data);
        AddEmptyTags(data);

        var reader = new KafkaProtocolReader(data.ToArray());
        var response = (UpdateFeaturesResponse)UpdateFeaturesResponse.Read(ref reader, version: 1);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(3);
        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.ErrorMessage).IsNull();
        await Assert.That(response.Results.Count).IsEqualTo(1);
        await Assert.That(response.Results[0].Feature).IsEqualTo("metadata.version");
        await Assert.That(response.Results[0].ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Results[0].ErrorMessage).IsNull();
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
