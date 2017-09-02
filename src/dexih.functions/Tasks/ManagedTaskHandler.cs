using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace dexih.functions.Tasks
{
    public class ManagedTaskHandler
    {
        private readonly int MaxConcurrent;

        public event EventHandler<EManagedTaskStatus> OnStatus;
        public event EventHandler<int> OnProgress;
        public event EventHandler OnTasksCompleted;

        private readonly ConcurrentDictionary<string, ManagedTask> _runningTasks;
        private readonly ConcurrentQueue<ManagedTask> _queuedTasks;

        private readonly ConcurrentDictionary<string, ManagedTask> _taskChangeHistory;

        private AutoResetEventAsync _resetWhenNoTasks; //event handler that triggers when all tasks completed.
        private object _updateTasksLock = 1; // used to lock when updaging task queues.
        private Exception _exitException; //used to push exceptions to the WhenAny function.

        public ManagedTaskHandler(int maxConcurrent = 100)
        {
            MaxConcurrent = maxConcurrent;

            _runningTasks = new ConcurrentDictionary<string, ManagedTask>();
            _queuedTasks = new ConcurrentQueue<ManagedTask>();
            _resetWhenNoTasks = new AutoResetEventAsync();
            _taskChangeHistory = new ConcurrentDictionary<string, ManagedTask>();
        }

        public ManagedTask Add(ManagedTask managedTask)
        {
            lock (_updateTasksLock)
            {
                if (_runningTasks.Count < MaxConcurrent)
                {
                    _runningTasks.TryAdd(managedTask.Reference, managedTask);

                    managedTask.OnStatus += StatusChange;
                    managedTask.OnProgress += ProgressChanged;
                    managedTask.Start();
                }
                else
                {
                    _queuedTasks.Enqueue(managedTask);
                }
            }

            return managedTask;
        }

        public void StatusChange(object sender, EManagedTaskStatus newStatus)
        {
            try
            {
                var managedTask = (ManagedTask)sender;

                //store most recent update
                _taskChangeHistory.AddOrUpdate(managedTask.Reference, managedTask, (oldKey, oldValue) => managedTask );

                switch (newStatus)
                {
                    case EManagedTaskStatus.Created:
                        break;
                    case EManagedTaskStatus.Scheduled:
                        break;
                    case EManagedTaskStatus.Queued:
                        break;
                    case EManagedTaskStatus.Running:
                        break;
                    case EManagedTaskStatus.Completed:
                    case EManagedTaskStatus.Error:
                    case EManagedTaskStatus.Cancelled:
                        ResetCompletedTask(managedTask);
                        break;
                }

                OnStatus?.Invoke(sender, newStatus);
            }
            catch (Exception ex)
            {
                _exitException = ex;
                _resetWhenNoTasks.Set();
            }
        }

        public void ProgressChanged(object sender, int percentage)
        {
            var managedTask = (ManagedTask)sender;

            //store most recent update
            _taskChangeHistory.AddOrUpdate(managedTask.Reference, managedTask, (oldKey, oldValue) => managedTask);

            OnProgress?.Invoke(sender, percentage);
        }

        private void ResetCompletedTask(ManagedTask managedTask)
        {
            lock (_updateTasksLock)
            {
                ManagedTask finishedTask;
                if (!_runningTasks.TryRemove(managedTask.Reference, out finishedTask))
                {
                    _exitException = new ManagedTaskException(managedTask, "Failed to remove the task from the running tasks list.");
                    _resetWhenNoTasks.Set();
                    return;
                }
            }

            UpdateRunningQueue();

            // if there are no remainning tasks, set the trigger to allow WhenAll to run.
            if (_runningTasks.Count == 0 && _queuedTasks.Count == 0)
            {
                OnTasksCompleted?.Invoke(this, EventArgs.Empty);
                _resetWhenNoTasks.Set();
            }
        }

        private void UpdateRunningQueue()
        {
            lock (_updateTasksLock)
            {
                // update the running queue
                while (_runningTasks.Count < MaxConcurrent && _queuedTasks.Count > 0)
                {
                    ManagedTask queuedTask;

                    if (!_queuedTasks.TryDequeue(out queuedTask))
                    {
                        // something wrong with concurrency if this is hit.
                        _exitException = new ManagedTaskException(queuedTask, "Failed to remove the task from the queued tasks list.");
                        _resetWhenNoTasks.Set();
                        return;
                    }

                    // if the task is marked as cancelled just ignore it
                    if (queuedTask.Status == EManagedTaskStatus.Cancelled)
                    {
                        OnStatus?.Invoke(queuedTask, EManagedTaskStatus.Cancelled);
                        continue;
                    }

                    if (!_runningTasks.TryAdd(queuedTask.Reference, queuedTask))
                    {
                        // something wrong with concurrency if this is hit.
                        _exitException = new ManagedTaskException(queuedTask, "Failed to add the task from the running tasks list.");
                        _resetWhenNoTasks.Set();
                        return;
                    }

                    queuedTask.OnStatus += StatusChange;
                    queuedTask.OnProgress += ProgressChanged;
                    queuedTask.Start();
                }
            }
        }

        public async Task WhenAll()
        {
            if (_runningTasks.Count > 0 || _queuedTasks.Count > 0)
            {
                await _resetWhenNoTasks.WaitAsync();

                if (_exitException != null)
                {
                    throw _exitException;
                }
            }
        }

        public IEnumerable<ManagedTask> GetTaskChanges(bool resetTaskChanges = false)
        {
            var taskChanges = _taskChangeHistory.Values;
            if (resetTaskChanges) ResetTaskChanges();

            return taskChanges;
        }

        public int TaskChangesCount()
        {
            return _taskChangeHistory.Count;
        }

        public void ResetTaskChanges()
        {
            _taskChangeHistory.Clear();
        }
    }
}
