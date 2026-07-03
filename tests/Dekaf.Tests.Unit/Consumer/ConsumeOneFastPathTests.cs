using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using Dekaf.Consumer;
using Dekaf.Protocol;
using Dekaf.Protocol.Records;
using Dekaf.Serialization;

namespace Dekaf.Tests.Unit.Consumer;

public sealed class ConsumeOneFastPathTests
{
    private const string Topic = "test-topic";
    private const int Partition = 0;

    [Test]
    public async Task ConsumeOneAsync_WithPendingFetch_ReturnsSequentialRecordsWithoutAsyncIterator()
    {
        var fetch = PendingFetchData.Create(Topic, Partition,
        [
            CreateBatch(20,
                CreateRecord(0, "a", "one"),
                CreateRecord(1, "b", "two"))
        ]);

        await using var consumer = CreateInitializedConsumer(fetch);
        var tp = new TopicPartition(Topic, Partition);

        var firstTask = consumer.ConsumeOneAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
        await Assert.That(firstTask.IsCompletedSuccessfully).IsTrue();
        var first = await firstTask;

        var secondTask = consumer.ConsumeOneAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
        await Assert.That(secondTask.IsCompletedSuccessfully).IsTrue();
        var second = await secondTask;

        await Assert.That(first).IsNotNull();
        await Assert.That(first!.Value.Offset).IsEqualTo(20L);
        await Assert.That(first.Value.Value).IsEqualTo("one");
        await Assert.That(second).IsNotNull();
        await Assert.That(second!.Value.Offset).IsEqualTo(21L);
        await Assert.That(second.Value.Value).IsEqualTo("two");
        await Assert.That(consumer.GetPosition(tp)).IsEqualTo(22L);
    }

    [Test]
    public async Task ConsumeOneAsync_WithPrefetchBuffer_ReturnsRecord()
    {
        var fetch = PendingFetchData.Create(Topic, Partition,
        [
            CreateBatch(30, CreateRecord(0, "a", "one"))
        ]);

        await using var consumer = CreateInitializedConsumer(queuedMinMessages: 2);
        SetPrefetchStarted(consumer);
        AssignTestPartition(consumer);
        SetPrefetchedBytes(consumer, KafkaConsumer<string, string>.EstimatePendingFetchBytes(fetch));
        await Assert.That(GetPrefetchBuffer(consumer).TryWrite(fetch)).IsTrue();

        var resultTask = consumer.ConsumeOneAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
        await Assert.That(resultTask.IsCompletedSuccessfully).IsTrue();
        var result = await resultTask;

        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Value.Offset).IsEqualTo(30L);
        await Assert.That(result.Value.Value).IsEqualTo("one");
        await Assert.That(GetPrefetchedBytes(consumer)).IsEqualTo(0L);
    }

    [Test]
    public async Task ConsumeOneAsync_WhenTimeoutExpires_ReturnsNull()
    {
        await using var consumer = CreateInitializedConsumer();

        var result = await consumer.ConsumeOneAsync(TimeSpan.FromMilliseconds(10), CancellationToken.None);

        await Assert.That(result).IsNull();
    }

    private static KafkaConsumer<string, string> CreateInitializedConsumer(params PendingFetchData[] fetches)
    {
        return CreateInitializedConsumer(queuedMinMessages: 1, fetches);
    }

    private static KafkaConsumer<string, string> CreateInitializedConsumer(
        int queuedMinMessages,
        params PendingFetchData[] fetches)
    {
        var consumer = new KafkaConsumer<string, string>(
            new ConsumerOptions
            {
                BootstrapServers = ["localhost:9092"],
                OffsetCommitMode = OffsetCommitMode.Manual,
                QueuedMinMessages = queuedMinMessages
            },
            Serializers.String,
            Serializers.String);

        SetInitialized(consumer);

        if (fetches.Length == 0)
            return consumer;

        AssignTestPartition(consumer);
        var pendingFetches = GetPendingFetches(consumer);
        foreach (var fetch in fetches)
            pendingFetches.Enqueue(fetch);

        return consumer;
    }

    private static void AssignTestPartition(KafkaConsumer<string, string> consumer)
    {
        var tp = new TopicPartition(Topic, Partition);
        consumer.Assign(tp);
        GetFetchPositions(consumer)[tp] = 0;
    }

    private static void SetInitialized(KafkaConsumer<string, string> consumer)
    {
        var initializedField = typeof(KafkaConsumer<string, string>)
            .GetField("_initialized", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_initialized field not found.");

        initializedField.SetValue(consumer, true);
    }

    private static ConcurrentDictionary<TopicPartition, long> GetFetchPositions(
        KafkaConsumer<string, string> consumer)
    {
        var field = typeof(KafkaConsumer<string, string>)
            .GetField("_fetchPositions", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_fetchPositions field not found.");

        return (ConcurrentDictionary<TopicPartition, long>)field.GetValue(consumer)!;
    }

    private static Queue<PendingFetchData> GetPendingFetches(KafkaConsumer<string, string> consumer)
    {
        var field = typeof(KafkaConsumer<string, string>)
            .GetField("_pendingFetches", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_pendingFetches field not found.");

        return (Queue<PendingFetchData>)field.GetValue(consumer)!;
    }

    private static MpscFetchBuffer GetPrefetchBuffer(KafkaConsumer<string, string> consumer)
    {
        var field = typeof(KafkaConsumer<string, string>)
            .GetField("_prefetchBuffer", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_prefetchBuffer field not found.");

        return (MpscFetchBuffer)field.GetValue(consumer)!;
    }

    private static void SetPrefetchStarted(KafkaConsumer<string, string> consumer)
    {
        var field = typeof(KafkaConsumer<string, string>)
            .GetField("_prefetchTask", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_prefetchTask field not found.");

        field.SetValue(consumer, Task.CompletedTask);
    }

    private static long GetPrefetchedBytes(KafkaConsumer<string, string> consumer)
    {
        var field = typeof(KafkaConsumer<string, string>)
            .GetField("_prefetchedBytes", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_prefetchedBytes field not found.");

        return (long)field.GetValue(consumer)!;
    }

    private static void SetPrefetchedBytes(KafkaConsumer<string, string> consumer, long bytes)
    {
        var field = typeof(KafkaConsumer<string, string>)
            .GetField("_prefetchedBytes", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("_prefetchedBytes field not found.");

        field.SetValue(consumer, bytes);
    }

    private static RecordBatch CreateBatch(long baseOffset, params Record[] records)
    {
        return new RecordBatch
        {
            BaseOffset = baseOffset,
            BaseTimestamp = 1700000000000L,
            Attributes = 0,
            Records = records
        };
    }

    private static Record CreateRecord(int offsetDelta, string key, string value)
    {
        return new Record
        {
            OffsetDelta = offsetDelta,
            TimestampDelta = 0,
            Key = Encoding.UTF8.GetBytes(key),
            IsKeyNull = false,
            Value = Encoding.UTF8.GetBytes(value),
            IsValueNull = false,
            Headers = null,
            HeaderCount = 0
        };
    }
}
