using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

/// <summary>
/// Tests for ListTransactions request/response encoding and decoding.
/// </summary>
public class ListTransactionsMessageTests
{
    [Test]
    public async Task ListTransactionsRequest_V0_WritesCompactFilters()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new ListTransactionsRequest
        {
            StateFilters = ["Ongoing", "PrepareCommit"],
            ProducerIdFilters = [17L, 23L]
        };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var states = reader.ReadCompactArray(static (ref KafkaProtocolReader r) => r.ReadCompactString() ?? string.Empty);
        var producerIds = reader.ReadCompactArray(static (ref KafkaProtocolReader r) => r.ReadInt64());
        reader.SkipTaggedFields();

        await Assert.That(states.Length).IsEqualTo(2);
        await Assert.That(states[0]).IsEqualTo("Ongoing");
        await Assert.That(states[1]).IsEqualTo("PrepareCommit");
        await Assert.That(producerIds.Length).IsEqualTo(2);
        await Assert.That(producerIds[0]).IsEqualTo(17L);
        await Assert.That(producerIds[1]).IsEqualTo(23L);
    }

    [Test]
    public async Task ListTransactionsResponse_V0_ReadsTopLevelAndStates()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        writer.WriteInt32(9);
        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteUnsignedVarInt(2);
        writer.WriteCompactString("UnknownState");
        writer.WriteUnsignedVarInt(2);
        writer.WriteCompactString("txn-a");
        writer.WriteInt64(123L);
        writer.WriteCompactString("Ongoing");
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (ListTransactionsResponse)ListTransactionsResponse.Read(ref reader, version: 0);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(9);
        await Assert.That(response.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(response.UnknownStateFilters.Count).IsEqualTo(1);
        await Assert.That(response.UnknownStateFilters[0]).IsEqualTo("UnknownState");
        await Assert.That(response.TransactionStates.Count).IsEqualTo(1);
        await Assert.That(response.TransactionStates[0].TransactionalId).IsEqualTo("txn-a");
        await Assert.That(response.TransactionStates[0].ProducerId).IsEqualTo(123L);
        await Assert.That(response.TransactionStates[0].TransactionState).IsEqualTo("Ongoing");
    }
}
