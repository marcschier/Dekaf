namespace Dekaf.Protocol.Messages;

/// <summary>
/// ListTransactions response (API key 66).
/// Contains transactions matching the requested filters.
/// </summary>
public sealed class ListTransactionsResponse : IKafkaResponse
{
    public static ApiKey ApiKey => ApiKey.ListTransactions;
    public static short LowestSupportedVersion => 0;
    public static short HighestSupportedVersion => 0;

    /// <summary>
    /// Throttle time in milliseconds.
    /// </summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>
    /// Top-level error code.
    /// </summary>
    public ErrorCode ErrorCode { get; init; }

    /// <summary>
    /// State filters unknown to the broker.
    /// </summary>
    public required IReadOnlyList<string> UnknownStateFilters { get; init; }

    /// <summary>
    /// Transaction states matching the filters.
    /// </summary>
    public required IReadOnlyList<ListTransactionsState> TransactionStates { get; init; }

    public static IKafkaResponse Read(ref KafkaProtocolReader reader, short version)
    {
        var throttleTimeMs = reader.ReadInt32();
        var errorCode = (ErrorCode)reader.ReadInt16();
        var unknownStateFilters = reader.ReadCompactArray(static (ref KafkaProtocolReader r) => r.ReadCompactString() ?? string.Empty);
        var transactionStates = reader.ReadCompactArray(
            (ref KafkaProtocolReader r) => ListTransactionsState.Read(ref r, version));
        reader.SkipTaggedFields();

        return new ListTransactionsResponse
        {
            ThrottleTimeMs = throttleTimeMs,
            ErrorCode = errorCode,
            UnknownStateFilters = unknownStateFilters,
            TransactionStates = transactionStates
        };
    }
}

/// <summary>
/// Summary for one listed transaction.
/// </summary>
public sealed class ListTransactionsState
{
    public required string TransactionalId { get; init; }
    public long ProducerId { get; init; }
    public required string TransactionState { get; init; }

    public static ListTransactionsState Read(ref KafkaProtocolReader reader, short version)
    {
        var transactionalId = reader.ReadCompactString() ?? string.Empty;
        var producerId = reader.ReadInt64();
        var transactionState = reader.ReadCompactString() ?? string.Empty;
        reader.SkipTaggedFields();

        return new ListTransactionsState
        {
            TransactionalId = transactionalId,
            ProducerId = producerId,
            TransactionState = transactionState
        };
    }
}
