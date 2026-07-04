// Polyfill for System.Threading.Lock (net9.0+). On earlier targets this Monitor-backed class
// provides the same surface (usable with the C# `lock` statement and EnterScope()).
// Internal and #if-gated so it compiles to nothing on net9.0+. Linked into every library project.

#if !NET9_0_OR_GREATER
namespace System.Threading
{
    internal sealed class Lock
    {
        // Lock on a plain object: the C# compiler special-cases the *type name*
        // System.Threading.Lock and warns (CS9216) if a value of that type is used with
        // Monitor, so the monitor target must not be the Lock instance itself.
        private readonly object _gate = new object();

        public void Enter() => Monitor.Enter(_gate);

        public bool TryEnter() => Monitor.TryEnter(_gate);

        public bool TryEnter(int millisecondsTimeout) => Monitor.TryEnter(_gate, millisecondsTimeout);

        public void Exit() => Monitor.Exit(_gate);

        public bool IsHeldByCurrentThread => Monitor.IsEntered(_gate);

        public Scope EnterScope()
        {
            Monitor.Enter(_gate);
            return new Scope(this);
        }

        public ref struct Scope
        {
            private Lock? _lockObj;

            internal Scope(Lock lockObj) => _lockObj = lockObj;

            public void Dispose()
            {
                Lock? lockObj = _lockObj;
                if (lockObj is not null)
                {
                    _lockObj = null;
                    Monitor.Exit(lockObj._gate);
                }
            }
        }
    }
}
#endif
