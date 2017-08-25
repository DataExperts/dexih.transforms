using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.functions
{
    /// <summary>
    ///     Asynchronous version of <see cref="AutoResetEvent" />
    /// </summary>
    public sealed class AutoResetEventAsync
    {
        private static readonly Task<bool> Completed = Task.FromResult(true);

        private readonly ConcurrentQueue<TaskCompletionSource<bool>> handlers =
            new ConcurrentQueue<TaskCompletionSource<bool>>();

        private int isSet;

        /// <summary>
        ///     Initializes a new instance of the <see cref="AutoResetEventAsync" /> class with a Boolean value indicating whether to set the intial state to signaled.
        /// </summary>
        /// <param name="initialState">true to set the initial state signaled; false to set the initial state to nonsignaled.</param>
        public AutoResetEventAsync(bool initialState = false)
        {
            this.isSet = initialState ? 1 : 0;
        }

        /// <summary>
        ///     Sets the state of the event to signaled, allowing one waiting continuation to proceed.
        /// </summary>
        public void Set()
        {
            if (!this.TrySet())
                return;

            TaskCompletionSource<bool> handler;

            // Notify first alive handler
            while (this.handlers.TryDequeue(out handler))
                if (CheckIfAlive(handler)) // Flag check
                    lock (handler)
                    {
                        if (!CheckIfAlive(handler))
                            continue;

                        if (this.TryReset())
                            handler.SetResult(true);
                        else
                            this.handlers.Enqueue(handler);

                        break;
                    }
        }

        /// <summary>
        ///     Try to switch the state to signaled from not signaled
        /// </summary>
        /// <returns>
        ///     true if suceeded, false if failed
        /// </returns>
        private bool TrySet()
        {
            return Interlocked.CompareExchange(ref this.isSet, 1, 0) == 0;
        }

        /// <summary>
        ///     Waits for a signal asynchronously
        /// </summary>
        public Task WaitAsync()
        {
            return this.WaitAsync(CancellationToken.None);
        }

        /// <summary>
        ///     Waits for a signal asynchronously
        /// </summary>
        /// <param name="cancellationToken">
        ///     A <see cref="P:System.Threading.Tasks.TaskFactory.CancellationToken" /> to observe while waiting for a signal.
        /// </param>
        /// <exception cref="OperationCanceledException">
        ///     The <paramref name="cancellationToken" /> was canceled.
        /// </exception>
        public Task WaitAsync(CancellationToken cancellationToken)
        {
            // Short path
            if (this.TryReset())
                return Completed;

            cancellationToken.ThrowIfCancellationRequested();

            // Wait for a signal
            var handler = new TaskCompletionSource<bool>(false);

            this.handlers.Enqueue(handler);

            if (CheckIfAlive(handler)) // Flag check
                lock (handler)
                    if (CheckIfAlive(handler) && this.TryReset())
                    {
                        handler.SetResult(true);
                        return handler.Task;
                    }

            cancellationToken.Register(() => {
                if (CheckIfAlive(handler)) // Flag check
                    lock (handler)
                        if (CheckIfAlive(handler))
                            handler.SetCanceled();
            });

            return handler.Task;
        }

        private static bool CheckIfAlive(TaskCompletionSource<bool> handler)
        {
            return handler.Task.Status == TaskStatus.WaitingForActivation;
        }

        private bool TryReset()
        {
            return Interlocked.CompareExchange(ref this.isSet, 0, 1) == 1;
        }
    }
    //   public class AsyncAutoResetEvent : IDisposable
    //   {
    //       private readonly static Task SCompleted = Task.FromResult(true);
    //       private readonly Queue<TaskCompletionSource<bool>> _mWaits = new Queue<TaskCompletionSource<bool>>();
    //       private bool _mSignaled;

    //       public Task WaitAsync()
    //       {
    //           lock (_mWaits)
    //           {
    //               if (_mSignaled)
    //               {
    //                   _mSignaled = false;
    //                   return SCompleted;
    //               }
    //               var tcs = new TaskCompletionSource<bool>();
    //               _mWaits.Enqueue(tcs);
    //               return tcs.Task;
    //           }
    //       }

    //       public void Set()
    //       {
    //           TaskCompletionSource<bool> toRelease = null;
    //           lock (_mWaits)
    //           {
    //               if (_mWaits.Count > 0)
    //                   toRelease = _mWaits.Dequeue();
    //               else if (!_mSignaled)
    //                   _mSignaled = true;
    //           }
    //           if (toRelease != null)
    //               toRelease.SetResult(true);
    //       }

    //	public void Dispose()
    //	{

    //	}
    //}
}
