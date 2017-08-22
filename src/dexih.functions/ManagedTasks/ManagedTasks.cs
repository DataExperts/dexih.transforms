using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.functions
{
    /// <summary>
    /// Simple collection of managed tasks
    /// </summary>
    public class ManagedTasks : IEnumerable<ManagedTask>
    {
        private readonly int MaxConcurrent;

        public event EventHandler OnCompleted;
        public event EventHandler OnProgress;
        public event EventHandler OnCancelled;

        private readonly ConcurrentDictionary<string, ManagedTask> _runningTasks;
        private readonly ConcurrentQueue<ManagedTask> _queuedTasks;

        private AsyncAutoResetEvent _resetWhenNoTasks; //event handler that triggers when all tasks completed.
        private object _updateTasksLock = 1; // used to lock when updaging task queues.
        private Exception _exitException; //used to push exceptions to the WhenAny function.

        public long TasksCount {get;set;}

        public ManagedTasks(int maxConcurrent = 10)
        {
            MaxConcurrent = maxConcurrent;

            _runningTasks = new ConcurrentDictionary<string, ManagedTask>();
            _queuedTasks = new ConcurrentQueue<ManagedTask>();
            _resetWhenNoTasks = new AsyncAutoResetEvent();
        }
    
        public IEnumerator<ManagedTask> GetEnumerator()
        {
            return _runningTasks.Values.Concat(_queuedTasks).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        
        /// <summary>
        /// Creates & starts a new managed task.
        /// </summary>
        /// <param name="originatorId">Id that can be used to referernce where the task was started from.</param>
        /// <param name="title">Short description of the task.</param>
        /// <param name="action">The action </param>
        /// <returns></returns>
        public ManagedTask Add(string originatorId, string title, Func<IProgress<int>, CancellationToken, Task> action)
        {

            var reference = (TasksCount++).ToString();

            var managedTask = new ManagedTask()
            {
                Reference = reference,
                OriginatorId = originatorId,
                Title =  title,
                Action = action
            };

            lock (_updateTasksLock)
            {
                if (_runningTasks.Count < MaxConcurrent)
                {
                    _runningTasks.TryAdd(reference, managedTask);

                    managedTask.OnCompleted += ManagedTaskCompleted;
                    managedTask.OnProgress += ManagedTaskProgress;
                    managedTask.OnCancelled += ManagedTaskCancelled;
                    managedTask.Start();
                }
                else
                {
                    _queuedTasks.Enqueue(managedTask);
                }
            }

            return managedTask;
        }

        public void ManagedTaskCompleted(object sender, EventArgs e)
        {
            try
            {
                var currentTask = (ManagedTask)sender;

                lock (_updateTasksLock)
                {
                    ManagedTask finishedTask;
                    if (!_runningTasks.TryRemove(currentTask.Reference, out finishedTask))
                    {
                        _exitException = new Exception("Error, failed to remove running task.");
                        _resetWhenNoTasks.Set();
                        return;
                    }
                    OnCompleted?.Invoke(finishedTask, EventArgs.Empty);
                    finishedTask.Dispose();

                    while (_runningTasks.Count < MaxConcurrent && _queuedTasks.Count > 0)
                    {
                        ManagedTask managedTask;

                        if (!_queuedTasks.TryDequeue(out managedTask))
                        {
                            _exitException = new Exception("Error, failed to remove queued task.");
                            _resetWhenNoTasks.Set();
                            return;
                        }

                        if(managedTask.Status == EManagedTaskStatus.Canceled)
                        {
                            ManagedTaskCancelled(managedTask, EventArgs.Empty);
                            continue;
                        }

                        _runningTasks.TryAdd(managedTask.Reference, managedTask);
                        managedTask.OnCompleted += ManagedTaskCompleted;
                        managedTask.OnProgress += ManagedTaskProgress;
                        managedTask.OnCancelled += ManagedTaskCancelled;
                        managedTask.Start();
                    }
                    if (_runningTasks.Count == 0)
                    {
                        _resetWhenNoTasks.Set();
                    }
                }
            } catch(Exception ex)
            {
                _exitException = ex;
                _resetWhenNoTasks.Set();
            }
        }

        public void ManagedTaskProgress(object sender, EventArgs e)
        {
            var managedTask = (ManagedTask)sender;
            OnProgress?.Invoke(managedTask, EventArgs.Empty);
        }

        public void ManagedTaskCancelled(object sender, EventArgs e)
        {
            OnCancelled?.Invoke(sender, e);
        }

        public async Task WhenAll()
        {
            if (_runningTasks.Count > 0 || _queuedTasks.Count > 0 )
            {
                await _resetWhenNoTasks.WaitAsync();

                if(_exitException != null)
                {
                    throw _exitException;
                }
            }
        }
    }
}