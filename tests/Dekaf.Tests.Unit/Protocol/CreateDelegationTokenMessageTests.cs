using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

public class CreateDelegationTokenMessageTests
{
    [Test]
    public async Task CreateDelegationTokenRequest_V3_WritesFieldsInOrder()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var request = new CreateDelegationTokenRequest
        {
            OwnerPrincipalType = "User",
            OwnerPrincipalName = "alice",
            Renewers =
            [
                new DelegationTokenPrincipalData
                {
                    PrincipalType = "User",
                    PrincipalName = "renewer"
                }
            ],
            MaxLifetimeMs = 86_400_000
        };

        request.Write(ref writer, version: 3);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var ownerPrincipalType = reader.ReadCompactString();
        var ownerPrincipalName = reader.ReadCompactString();
        var renewersLength = reader.ReadUnsignedVarInt();
        var renewerPrincipalType = reader.ReadCompactString();
        var renewerPrincipalName = reader.ReadCompactString();
        reader.SkipTaggedFields();
        var maxLifetimeMs = reader.ReadInt64();
        reader.SkipTaggedFields();
        var end = reader.End;

        await Assert.That(ownerPrincipalType).IsEqualTo("User");
        await Assert.That(ownerPrincipalName).IsEqualTo("alice");
        await Assert.That(renewersLength).IsEqualTo(2);
        await Assert.That(renewerPrincipalType).IsEqualTo("User");
        await Assert.That(renewerPrincipalName).IsEqualTo("renewer");
        await Assert.That(maxLifetimeMs).IsEqualTo(86_400_000);
        await Assert.That(end).IsTrue();
    }

    [Test]
    public async Task CreateDelegationTokenResponse_V3_CanBeParsed()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);
        var hmac = new byte[3];
        hmac[0] = 1;
        hmac[1] = 2;
        hmac[2] = 3;

        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteCompactString("User");
        writer.WriteCompactString("alice");
        writer.WriteCompactString("User");
        writer.WriteCompactString("requester");
        writer.WriteInt64(1000);
        writer.WriteInt64(2000);
        writer.WriteInt64(3000);
        writer.WriteCompactString("token-id");
        writer.WriteCompactBytes(hmac);
        writer.WriteInt32(42);
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (CreateDelegationTokenResponse)CreateDelegationTokenResponse.Read(ref reader, version: 3);
        var end = reader.End;

        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.PrincipalType).IsEqualTo("User");
        await Assert.That(response.PrincipalName).IsEqualTo("alice");
        await Assert.That(response.TokenRequesterPrincipalType).IsEqualTo("User");
        await Assert.That(response.TokenRequesterPrincipalName).IsEqualTo("requester");
        await Assert.That(response.IssueTimestampMs).IsEqualTo(1000);
        await Assert.That(response.ExpiryTimestampMs).IsEqualTo(2000);
        await Assert.That(response.MaxTimestampMs).IsEqualTo(3000);
        await Assert.That(response.TokenId).IsEqualTo("token-id");
        await Assert.That(response.Hmac.Length).IsEqualTo(3);
        await Assert.That(response.Hmac[0]).IsEqualTo((byte)1);
        await Assert.That(response.Hmac[1]).IsEqualTo((byte)2);
        await Assert.That(response.Hmac[2]).IsEqualTo((byte)3);
        await Assert.That(response.ThrottleTimeMs).IsEqualTo(42);
        await Assert.That(end).IsTrue();
    }
}
