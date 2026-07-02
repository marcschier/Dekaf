using Dekaf.Errors;
using Dekaf.Protocol;

namespace Dekaf.Consumer;

internal static class AutoOffsetResetStrategy
{
    public static long GetListOffsetsTimestamp(ConsumerOptions options, DateTimeOffset now)
    {
        return options.AutoOffsetReset switch
        {
            AutoOffsetReset.Earliest => -2,
            AutoOffsetReset.Latest => -1,
            AutoOffsetReset.ByDuration => GetByDurationTimestamp(options.AutoOffsetResetDuration, now),
            AutoOffsetReset.None => throw new KafkaException(
                ErrorCode.OffsetOutOfRange,
                "No committed offset and auto.offset.reset is 'none'"),
            _ => throw new InvalidOperationException($"Unknown AutoOffsetReset value: {options.AutoOffsetReset}")
        };
    }

    public static long GetByDurationTimestamp(TimeSpan? duration, DateTimeOffset now)
    {
        if (duration is null)
        {
            throw new InvalidOperationException(
                $"{nameof(ConsumerOptions.AutoOffsetResetDuration)} must be set when {nameof(ConsumerOptions.AutoOffsetReset)} is {nameof(AutoOffsetReset.ByDuration)}.");
        }

        ValidateDuration(duration.Value);
        return now.Subtract(duration.Value).ToUnixTimeMilliseconds();
    }

    public static void ValidateOptions(ConsumerOptions options)
    {
        if (options.AutoOffsetReset == AutoOffsetReset.ByDuration)
        {
            if (options.AutoOffsetResetDuration is null)
            {
                throw new InvalidOperationException(
                    $"{nameof(ConsumerOptions.AutoOffsetResetDuration)} must be set when {nameof(ConsumerOptions.AutoOffsetReset)} is {nameof(AutoOffsetReset.ByDuration)}.");
            }

            ValidateDuration(options.AutoOffsetResetDuration.Value);
        }
    }

    public static void ValidateDuration(TimeSpan duration)
    {
        if (duration < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(duration), duration, "Duration-based offset reset does not allow negative durations.");
        }
    }
}
