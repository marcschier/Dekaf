// Polyfill for the Task.WaitAsync(...) instance methods (net6.0+), provided here as extension
// methods for the lower target frameworks. Implements the timeout + cancellation semantics of
// the BCL API (throws TimeoutException / OperationCanceledException). Internal and #if-gated so
// it compiles to nothing on net6.0+ where the instance methods exist.

#if !NET6_0_OR_GREATER
namespace System.Threading.Tasks
{
    internal static class TaskWaitAsyncPolyfill
    {
        private static readonly Task NeverCompletes = new TaskCompletionSource<bool>().Task;

        public static Task WaitAsync(this Task task, CancellationToken cancellationToken)
            => task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken);

        public static async Task WaitAsync(
            this Task task, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (task.IsCompleted)
            {
                await task.ConfigureAwait(false);
                return;
            }

            var cancelSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (Register(cancellationToken, cancelSignal))
            {
                var timeoutTask = timeout == Timeout.InfiniteTimeSpan ? NeverCompletes : Task.Delay(timeout);
                var completed = await Task.WhenAny(task, cancelSignal.Task, timeoutTask).ConfigureAwait(false);

                if (completed == task)
                {
                    await task.ConfigureAwait(false);
                    return;
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException();
        }

        public static Task<TResult> WaitAsync<TResult>(this Task<TResult> task, CancellationToken cancellationToken)
            => task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken);

        public static async Task<TResult> WaitAsync<TResult>(
            this Task<TResult> task, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (task.IsCompleted)
            {
                return await task.ConfigureAwait(false);
            }

            var cancelSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (Register(cancellationToken, cancelSignal))
            {
                var timeoutTask = timeout == Timeout.InfiniteTimeSpan ? NeverCompletes : Task.Delay(timeout);
                var completed = await Task.WhenAny(task, cancelSignal.Task, timeoutTask).ConfigureAwait(false);

                if (completed == task)
                {
                    return await task.ConfigureAwait(false);
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException();
        }

        private static CancellationTokenRegistration Register(
            CancellationToken cancellationToken, TaskCompletionSource<bool> signal)
        {
            return cancellationToken.CanBeCanceled
                ? cancellationToken.Register(
                    static state => ((TaskCompletionSource<bool>)state!).TrySetResult(true), signal)
                : default;
        }
    }
}
#endif
