using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace dexih.functions
{
    public class AsyncAutoResetEvent : IDisposable
    {
        private readonly static Task SCompleted = Task.FromResult(true);
        private readonly Queue<TaskCompletionSource<bool>> _mWaits = new Queue<TaskCompletionSource<bool>>();
        private bool _mSignaled;

        public Task WaitAsync()
        {
            lock (_mWaits)
            {
                if (_mSignaled)
                {
                    _mSignaled = false;
                    return SCompleted;
                }
                var tcs = new TaskCompletionSource<bool>();
                _mWaits.Enqueue(tcs);
                return tcs.Task;
            }
        }

        public void Set()
        {
            TaskCompletionSource<bool> toRelease = null;
            lock (_mWaits)
            {
                if (_mWaits.Count > 0)
                    toRelease = _mWaits.Dequeue();
                else if (!_mSignaled)
                    _mSignaled = true;
            }
            if (toRelease != null)
                toRelease.SetResult(true);
        }

		public void Dispose()
		{

		}
	}
}
