namespace Dekaf.Protocol.Messages;

/// <summary>
/// ListTransactions request (API key 66).
/// Lists transactions matching state and producer ID filters.
/// </summary>
public sealed class ListTransactionsRequest : IKafkaRequest<ListTransactionsResponse>
{
    public static ApiKey ApiKey => ApiKey.ListTransactions;
    public static short LowestSupportedVersion => 0;
    public static short HighestSupportedVersion => 0;

    /// <summary>
    /// Transaction states to include.
    /// </summary>
    public required IReadOnlyList<string> StateFilters { get; init; }

    /// <summary>
    /// Producer IDs to include.
    /// </summary>
    public required IReadOnlyList<long> ProducerIdFilters { get; init; }

    public void Write(ref KafkaProtocolWriter writer, short version)
    {
        writer.WriteCompactArray(
            StateFilters,
            static (ref KafkaProtocolWriter w, string stateFilter) => w.WriteCompactString(stateFilter));
        writer.WriteCompactArray(
            ProducerIdFilters,
            static (ref KafkaProtocolWriter w, long producerId) => w.WriteInt64(producerId));
        writer.WriteEmptyTaggedFields();
    }
}
