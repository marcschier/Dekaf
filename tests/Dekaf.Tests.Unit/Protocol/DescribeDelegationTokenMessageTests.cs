using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class DescribeDelegationTokenMessageTests
{
    [Test]
    public async Task DescribeDelegationTokenRequest_V3_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var request = new DescribeDelegationTokenRequest
        {
            Owners =
            [
                new DelegationTokenPrincipalData
                {
                    PrincipalType = "User",
                    PrincipalName = "alice"
                }
            ]
        };

        request.Write(ref writer, version: 3);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var ownersLength = reader.ReadUnsignedVarInt();
        var ownerPrincipalType = reader.ReadCompactString();
        var ownerPrincipalName = reader.ReadCompactString();
        reader.SkipTaggedFields();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(ownersLength).IsEqualTo(2);
        await Assert.That(ownerPrincipalType).IsEqualTo("User");
        await Assert.That(ownerPrincipalName).IsEqualTo("alice");
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task DescribeDelegationTokenResponse_V3_CanBeParsed()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var hmac = new byte[2];
        hmac[0] = 50;
        hmac[1] = 60;

        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteUnsignedVarInt(2);
        writer.WriteCompactString("User");
        writer.WriteCompactString("alice");
        writer.WriteCompactString("User");
        writer.WriteCompactString("requester");
        writer.WriteInt64(1000);
        writer.WriteInt64(2000);
        writer.WriteInt64(3000);
        writer.WriteCompactString("token-id");
        writer.WriteCompactBytes(hmac);
        writer.WriteUnsignedVarInt(2);
        writer.WriteCompactString("User");
        writer.WriteCompactString("renewer");
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();
        writer.WriteInt32(11);
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (DescribeDelegationTokenResponse)DescribeDelegationTokenResponse.Read(ref reader, version: 3);
        var end = reader.End;

        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.Tokens.Count).IsEqualTo(1);
        var token = response.Tokens[0];
        await Assert.That(token.PrincipalType).IsEqualTo("User");
        await Assert.That(token.PrincipalName).IsEqualTo("alice");
        await Assert.That(token.TokenRequesterPrincipalType).IsEqualTo("User");
        await Assert.That(token.TokenRequesterPrincipalName).IsEqualTo("requester");
        await Assert.That(token.IssueTimestampMs).IsEqualTo(1000);
        await Assert.That(token.ExpiryTimestampMs).IsEqualTo(2000);
        await Assert.That(token.MaxTimestampMs).IsEqualTo(3000);
        await Assert.That(token.TokenId).IsEqualTo("token-id");
        await Assert.That(token.Hmac.Length).IsEqualTo(2);
        await Assert.That(token.Hmac[0]).IsEqualTo((byte)50);
        await Assert.That(token.Hmac[1]).IsEqualTo((byte)60);
        await Assert.That(token.Renewers.Count).IsEqualTo(1);
        await Assert.That(token.Renewers[0].PrincipalType).IsEqualTo("User");
        await Assert.That(token.Renewers[0].PrincipalName).IsEqualTo("renewer");
        await Assert.That(response.ThrottleTimeMs).IsEqualTo(11);
        await Assert.That(end).IsTrue();
    }
}
