using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;
using Dekaf.Protocol;
using Dekaf.Protocol.Messages;
using Dekaf.Protocol.Records;

namespace Dekaf.Benchmarks.Benchmarks.Unit;

/// <summary>
/// Benchmarks fetch protocol encode/decode hot paths used by consumers.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class FetchProtocolBenchmarks
{
    private const short FetchVersion = 16;
    private const int PartitionCount = 10;
    private const int RecordsPerBatch = 10;
    private static readonly Guid s_topicId = new("00112233-4455-6677-8899-aabbccddeeff");

    private ArrayBufferWriter<byte> _buffer = null!;
    private FetchRequest _fetchRequest = null!;
    private byte[] _fetchResponseBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _buffer = new ArrayBufferWriter<byte>(65536);
        _fetchRequest = CreateFetchRequest();
        _fetchResponseBytes = CreateFetchResponseBytes();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _buffer.Clear();
    }

    [Benchmark(Description = "Write FetchRequest v16 (10 partitions)")]
    public int WriteFetchRequest()
    {
        var writer = new KafkaProtocolWriter(_buffer);
        _fetchRequest.Write(ref writer, FetchVersion);
        return writer.BytesWritten;
    }

    [Benchmark(Description = "Read FetchResponse v16 (10 partitions)")]
    public int ReadFetchResponse()
    {
        var reader = new KafkaProtocolReader(_fetchResponseBytes);
        var response = (FetchResponse)FetchResponse.Read(ref reader, FetchVersion);
        return ConsumeAndReturn(response, iterateRecords: false);
    }

    [Benchmark(Description = "Read FetchResponse v16 + records (100 messages)")]
    public int ReadFetchResponseAndRecords()
    {
        var reader = new KafkaProtocolReader(_fetchResponseBytes);
        var response = (FetchResponse)FetchResponse.Read(ref reader, FetchVersion);
        return ConsumeAndReturn(response, iterateRecords: true);
    }

    private static FetchRequest CreateFetchRequest()
    {
        var partitions = new FetchRequestPartition[PartitionCount];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = new FetchRequestPartition
            {
                Partition = i,
                CurrentLeaderEpoch = 7,
                FetchOffset = 1_000_000L + i,
                LastFetchedEpoch = 6,
                LogStartOffset = 0,
                PartitionMaxBytes = 1_048_576
            };
        }

        var topics = new FetchRequestTopic[]
        {
            new()
            {
                TopicId = s_topicId,
                Partitions = partitions
            }
        };

        return new FetchRequest
        {
            ClusterId = "bench-cluster",
            ReplicaState = new ReplicaState
            {
                ReplicaId = -1,
                ReplicaEpoch = -1
            },
            MaxWaitMs = 500,
            MinBytes = 1,
            MaxBytes = 10 * 1_048_576,
            IsolationLevel = IsolationLevel.ReadUncommitted,
            SessionId = 42,
            SessionEpoch = 3,
            Topics = topics,
            ForgottenTopicsData = Array.Empty<ForgottenTopic>(),
            RackId = "rack-a"
        };
    }

    private static byte[] CreateFetchResponseBytes()
    {
        var recordBatchBytes = CreateRecordBatchBytes();
        var buffer = new ArrayBufferWriter<byte>(65536);
        var writer = new KafkaProtocolWriter(buffer);

        writer.WriteInt32(0); // throttle_time_ms
        writer.WriteInt16((short)ErrorCode.None);
        writer.WriteInt32(42); // session_id

        writer.WriteUnsignedVarInt(2); // one topic, compact-array length + 1
        writer.WriteUuid(s_topicId);
        writer.WriteUnsignedVarInt(PartitionCount + 1);

        for (var partition = 0; partition < PartitionCount; partition++)
        {
            writer.WriteInt32(partition);
            writer.WriteInt16((short)ErrorCode.None);
            writer.WriteInt64(1_000_000L + partition + RecordsPerBatch);
            writer.WriteInt64(1_000_000L + partition + RecordsPerBatch);
            writer.WriteInt64(0);
            writer.WriteUnsignedVarInt(1); // empty aborted_transactions compact array
            writer.WriteInt32(-1); // preferred_read_replica
            writer.WriteUnsignedVarInt(recordBatchBytes.Length + 1);
            writer.WriteRawBytes(recordBatchBytes);
            writer.WriteEmptyTaggedFields();
        }

        writer.WriteEmptyTaggedFields(); // topic tagged fields
        writer.WriteEmptyTaggedFields(); // response tagged fields

        return buffer.WrittenSpan.ToArray();
    }

    private static byte[] CreateRecordBatchBytes()
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var records = new Record[RecordsPerBatch];
        for (var i = 0; i < records.Length; i++)
        {
            records[i] = new Record
            {
                TimestampDelta = i,
                OffsetDelta = i,
                Key = Encoding.UTF8.GetBytes($"key-{i}"),
                Value = Encoding.UTF8.GetBytes($"value-{i}-payload-{new string('x', 64)}")
            };
        }

        var batch = new RecordBatch
        {
            BaseOffset = 1_000_000,
            BaseTimestamp = timestamp,
            MaxTimestamp = timestamp + RecordsPerBatch - 1,
            LastOffsetDelta = RecordsPerBatch - 1,
            Records = records
        };

        var buffer = new ArrayBufferWriter<byte>(8192);
        batch.Write(buffer);
        return buffer.WrittenSpan.ToArray();
    }

    private static int ConsumeAndReturn(FetchResponse response, bool iterateRecords)
    {
        var sum = response.Responses.Count;

        for (var topicIndex = 0; topicIndex < response.Responses.Count; topicIndex++)
        {
            var topic = response.Responses[topicIndex];
            sum += topic.Partitions.Count;

            for (var partitionIndex = 0; partitionIndex < topic.Partitions.Count; partitionIndex++)
            {
                var partition = topic.Partitions[partitionIndex];
                sum += partition.PartitionIndex;

                var batches = partition.Records;
                if (batches is null)
                    continue;

                for (var batchIndex = 0; batchIndex < batches.Count; batchIndex++)
                {
                    var batch = batches[batchIndex];
                    sum += batch.LastOffsetDelta;

                    if (iterateRecords)
                    {
                        var records = batch.Records;
                        for (var recordIndex = 0; recordIndex < records.Count; recordIndex++)
                        {
                            var record = records[recordIndex];
                            sum += record.OffsetDelta + record.Value.Length;
                        }
                    }

                    batch.Dispose();
                }
            }
        }

        response.ReturnToPool();
        return sum;
    }
}
