// Polyfill for the non-generic System.Threading.Tasks.TaskCompletionSource (net5.0+).
// Backed by TaskCompletionSource<bool> so it exposes the same completion surface Dekaf uses.
// Internal and #if-gated: compiles to nothing on net5.0+ where the BCL type exists.

#if !NET5_0_OR_GREATER
namespace System.Threading.Tasks
{
    using System.Collections.Generic;

    internal sealed class TaskCompletionSource
    {
        private readonly TaskCompletionSource<bool> _inner;

        public TaskCompletionSource() => _inner = new TaskCompletionSource<bool>();

        public TaskCompletionSource(TaskCreationOptions creationOptions)
            => _inner = new TaskCompletionSource<bool>(creationOptions);

        public TaskCompletionSource(object? state)
            => _inner = new TaskCompletionSource<bool>(state);

        public TaskCompletionSource(object? state, TaskCreationOptions creationOptions)
            => _inner = new TaskCompletionSource<bool>(state, creationOptions);

        public Task Task => _inner.Task;

        public void SetResult() => _inner.SetResult(true);

        public bool TrySetResult() => _inner.TrySetResult(true);

        public void SetException(Exception exception) => _inner.SetException(exception);

        public void SetException(IEnumerable<Exception> exceptions) => _inner.SetException(exceptions);

        public bool TrySetException(Exception exception) => _inner.TrySetException(exception);

        public bool TrySetException(IEnumerable<Exception> exceptions) => _inner.TrySetException(exceptions);

        public void SetCanceled() => _inner.SetCanceled();

        public bool TrySetCanceled() => _inner.TrySetCanceled();

        public bool TrySetCanceled(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return _inner.TrySetCanceled();
        }
    }
}
#endif
