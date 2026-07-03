using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class ExpireDelegationTokenMessageTests
{
    [Test]
    public async Task ExpireDelegationTokenRequest_V2_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var hmac = new byte[2];
        hmac[0] = 30;
        hmac[1] = 40;
        var request = new ExpireDelegationTokenRequest
        {
            Hmac = hmac,
            ExpiryTimePeriodMs = 0
        };

        request.Write(ref writer, version: 2);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var parsedHmac = reader.ReadCompactBytes();
        var expiryTimePeriodMs = reader.ReadInt64();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(parsedHmac).IsNotNull();
        await Assert.That(parsedHmac!.Length).IsEqualTo(2);
        await Assert.That(parsedHmac[0]).IsEqualTo((byte)30);
        await Assert.That(parsedHmac[1]).IsEqualTo((byte)40);
        await Assert.That(expiryTimePeriodMs).IsEqualTo(0);
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task ExpireDelegationTokenResponse_V2_CanBeParsed()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteInt64(987654321);
        writer.WriteInt32(9);
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (ExpireDelegationTokenResponse)ExpireDelegationTokenResponse.Read(ref reader, version: 2);
        var end = reader.End;

        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.ExpiryTimestampMs).IsEqualTo(987654321);
        await Assert.That(response.ThrottleTimeMs).IsEqualTo(9);
        await Assert.That(end).IsTrue();
    }
}
