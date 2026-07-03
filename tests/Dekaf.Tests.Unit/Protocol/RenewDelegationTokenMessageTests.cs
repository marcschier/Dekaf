using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class RenewDelegationTokenMessageTests
{
    [Test]
    public async Task RenewDelegationTokenRequest_V2_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var hmac = new byte[2];
        hmac[0] = 10;
        hmac[1] = 20;
        var request = new RenewDelegationTokenRequest
        {
            Hmac = hmac,
            RenewPeriodMs = 60_000
        };

        request.Write(ref writer, version: 2);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var parsedHmac = reader.ReadCompactBytes();
        var renewPeriodMs = reader.ReadInt64();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(parsedHmac).IsNotNull();
        await Assert.That(parsedHmac!.Length).IsEqualTo(2);
        await Assert.That(parsedHmac[0]).IsEqualTo((byte)10);
        await Assert.That(parsedHmac[1]).IsEqualTo((byte)20);
        await Assert.That(renewPeriodMs).IsEqualTo(60_000);
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task RenewDelegationTokenResponse_V2_CanBeParsed()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteInt64(123456789);
        writer.WriteInt32(7);
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (RenewDelegationTokenResponse)RenewDelegationTokenResponse.Read(ref reader, version: 2);
        var end = reader.End;

        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.ExpiryTimestampMs).IsEqualTo(123456789);
        await Assert.That(response.ThrottleTimeMs).IsEqualTo(7);
        await Assert.That(end).IsTrue();
    }
}
