// Polyfill for System.Threading.PeriodicTimer (net6.0+). Provides the subset used by Dekaf:
// the TimeSpan constructor, WaitForNextTickAsync(CancellationToken) and Dispose(). Ticks are
// coalesced (a single pending tick, like the BCL type). Internal and #if-gated so it compiles
// to nothing on net6.0+.

#if !NET6_0_OR_GREATER
namespace System.Threading
{
    using System.Threading.Tasks;

    internal sealed class PeriodicTimer : IDisposable
    {
        private readonly Timer _timer;
        private readonly SemaphoreSlim _tick = new SemaphoreSlim(0, 1);
        private volatile bool _disposed;

        public PeriodicTimer(TimeSpan period)
        {
            _timer = new Timer(static state => ((PeriodicTimer)state!).Signal(), this, period, period);
        }

        public async ValueTask<bool> WaitForNextTickAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                return false;
            }

            try
            {
                await _tick.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                return false;
            }

            return !_disposed;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _timer.Dispose();

            // Wake a pending waiter so it observes the disposed state and returns false.
            try
            {
                _tick.Release();
            }
            catch (SemaphoreFullException)
            {
            }
        }

        private void Signal()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                _tick.Release();
            }
            catch (SemaphoreFullException)
            {
                // A tick is already pending; coalesce.
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }
}
#endif
