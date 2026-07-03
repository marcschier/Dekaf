using System.Buffers;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;

namespace Dekaf.Tests.Unit.Protocol;

/// <summary>
/// Tests for DescribeTransactions request/response encoding and decoding.
/// </summary>
public class DescribeTransactionsMessageTests
{
    [Test]
    public async Task DescribeTransactionsRequest_V0_WritesCompactTransactionalIds()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        var request = new DescribeTransactionsRequest
        {
            TransactionalIds = ["txn-a", "txn-b"]
        };
        request.Write(ref writer, version: 0);

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var transactionalIds = reader.ReadCompactArray(static (ref KafkaProtocolReader r) => r.ReadCompactString() ?? string.Empty);
        reader.SkipTaggedFields();

        await Assert.That(transactionalIds.Length).IsEqualTo(2);
        await Assert.That(transactionalIds[0]).IsEqualTo("txn-a");
        await Assert.That(transactionalIds[1]).IsEqualTo("txn-b");
    }

    [Test]
    public async Task DescribeTransactionsResponse_V0_ReadsNestedTransactionState()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new KafkaProtocolWriter(buffer);

        writer.WriteInt32(42);
        writer.WriteUnsignedVarInt(2);
        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteCompactString("txn-a");
        writer.WriteCompactString("Ongoing");
        writer.WriteInt32(60000);
        writer.WriteInt64(123456789L);
        writer.WriteInt64(99L);
        writer.WriteInt16(7);
        writer.WriteUnsignedVarInt(2);
        writer.WriteCompactString("orders");
        writer.WriteUnsignedVarInt(3);
        writer.WriteInt32(0);
        writer.WriteInt32(2);
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();
        writer.WriteEmptyTaggedFields();

        var reader = new KafkaProtocolReader(buffer.WrittenMemory);
        var response = (DescribeTransactionsResponse)DescribeTransactionsResponse.Read(ref reader, version: 0);

        await Assert.That(response.ThrottleTimeMs).IsEqualTo(42);
        await Assert.That(response.TransactionStates.Count).IsEqualTo(1);
        var state = response.TransactionStates[0];
        await Assert.That(state.ErrorCode).IsEqualTo(ErrorCode.None);
        await Assert.That(state.TransactionalId).IsEqualTo("txn-a");
        await Assert.That(state.TransactionState).IsEqualTo("Ongoing");
        await Assert.That(state.TransactionTimeoutMs).IsEqualTo(60000);
        await Assert.That(state.TransactionStartTimeMs).IsEqualTo(123456789L);
        await Assert.That(state.ProducerId).IsEqualTo(99L);
        await Assert.That(state.ProducerEpoch).IsEqualTo((short)7);
        await Assert.That(state.Topics.Count).IsEqualTo(1);
        await Assert.That(state.Topics[0].Topic).IsEqualTo("orders");
        await Assert.That(state.Topics[0].Partitions.Count).IsEqualTo(2);
        await Assert.That(state.Topics[0].Partitions[0]).IsEqualTo(0);
        await Assert.That(state.Topics[0].Partitions[1]).IsEqualTo(2);
    }
}
