namespace Dekaf.Protocol.Messages;

/// <summary>
/// DescribeTransactions response (API key 65).
/// Contains transaction state for transactional IDs.
/// </summary>
public sealed class DescribeTransactionsResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.DescribeTransactions;
    public static short LowestSupportedVersion => 0;
    public static short HighestSupportedVersion => 0;

    /// <summary>
    /// Throttle time in milliseconds.
    /// </summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>
    /// Transaction states returned by the broker.
    /// </summary>
    public required IReadOnlyList<DescribeTransactionsState> TransactionStates { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var transactionStates = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeTransactionsState.Read(ref r, version));
        reader.SkipTaggedFields();

        return new DescribeTransactionsResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            TransactionStates = transactionStates
        };
    }
}

/// <summary>
/// State for one transactional ID.
/// </summary>
public sealed class DescribeTransactionsState
{
    public ErrorCode ErrorCode { get; init; }
    public required string TransactionalId { get; init; }
    public required string TransactionState { get; init; }
    public int TransactionTimeoutMs { get; init; }
    public long TransactionStartTimeMs { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public required IReadOnlyList<DescribeTransactionsTopic> Topics { get; init; }

    public static DescribeTransactionsState Read(ref KafkaProtocolReader reader, short version)
    {
        var errorCode = (ErrorCode)reader.ReadInt16();
        var transactionalId = reader.ReadCompactString() ?? string.Empty;
        var transactionState = reader.ReadCompactString() ?? string.Empty;
        var transactionTimeoutMs = reader.ReadInt32();
        var transactionStartTimeMs = reader.ReadInt64();
        var producerId = reader.ReadInt64();
        var producerEpoch = reader.ReadInt16();
        var topics = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => DescribeTransactionsTopic.Read(ref r, version));
        reader.SkipTaggedFields();

        return new DescribeTransactionsState
        {
            ErrorCode = errorCode,
            TransactionalId = transactionalId,
            TransactionState = transactionState,
            TransactionTimeoutMs = transactionTimeoutMs,
            TransactionStartTimeMs = transactionStartTimeMs,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Topics = topics
        };
    }
}

/// <summary>
/// Topic included in a transaction.
/// </summary>
public sealed class DescribeTransactionsTopic
{
    public required string Topic { get; init; }
    public required IReadOnlyList<int> Partitions { get; init; }

    public static DescribeTransactionsTopic Read(ref KafkaProtocolReader reader, short version)
    {
        var topic = reader.ReadCompactString() ?? string.Empty;
        var partitions = reader.ReadCompactArray(static (ref KafkaProtocolReader r) => r.ReadInt32());
        reader.SkipTaggedFields();

        return new DescribeTransactionsTopic
        {
            Topic = topic,
            Partitions = partitions
        };
    }
}
