using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.functions.Tasks
{
    /// <summary>
    /// Simple collection of managed tasks
    /// </summary>
    public class ManagedTasks : IEnumerable<ManagedTask>
    {
        public event EventHandler<EManagedTaskStatus> OnStatus;
        public event EventHandler<int> OnProgress;

        private readonly ConcurrentDictionary<string, ManagedTask> _activeTasks;

        private object _updateTasksLock = 1; // used to lock when updaging task queues.
        private Exception _exitException; //used to push exceptions to the WhenAny function.
        private AsyncAutoResetEvent _resetWhenNoTasks; //event handler that triggers when all tasks completed.
        private ManagedTaskHandler _taskHandler;

        public ManagedTasks(ManagedTaskHandler taskHandler = null)
        {
            if(taskHandler == null)
            {
                _taskHandler = new ManagedTaskHandler();
            } 
            else
            {
                _taskHandler = taskHandler;
            }

            _activeTasks = new ConcurrentDictionary<string, ManagedTask>();
            _resetWhenNoTasks = new AsyncAutoResetEvent();
        }
    
        public ManagedTask Add(ManagedTask managedTask)
        {
            managedTask.OnStatus += StatusChange;
            managedTask.OnProgress += ProgressChange;

            if (!_activeTasks.TryAdd(managedTask.Reference, managedTask))
            {
                throw new ManagedTaskException(managedTask, "Failed to add the task to the active tasks list.");
            }

            // if there are no depdencies, put the task immediately on the queue.
            if ((managedTask.Triggers == null || managedTask.Triggers.Length == 0) && (managedTask.DependentReferences == null || managedTask.DependentReferences.Length == 0))
            {
                _taskHandler.Add(managedTask);
            }
            else
            {
                if (managedTask.Schedule())
                {
                    managedTask.OnTrigger += Trigger;
                }
                else
                {
                    if (managedTask.DependentReferences == null || managedTask.DependentReferences.Length == 0)
                    {
                        managedTask.Error("None of the triggers returned a future schedule time.", null);
                    }
                }
            }

            return managedTask;
        }

        public ManagedTask Add(string originatorId, string name, string category, object data, Func<IProgress<int>, CancellationToken, Task> action, ManagedTaskTrigger[] triggers, string[] dependentReferences)
        {
            var reference = Guid.NewGuid().ToString();
            return Add(reference, originatorId, name, category, data, action, triggers, dependentReferences);
        }

        /// <summary>
        /// Creates & starts a new managed task.
        /// </summary>
        /// <param name="originatorId">Id that can be used to referernce where the task was started from.</param>
        /// <param name="title">Short description of the task.</param>
        /// <param name="action">The action </param>
        /// <returns></returns>
        public ManagedTask Add(string reference, string originatorId, string name, string category, object data, Func<IProgress<int>, CancellationToken, Task> action, ManagedTaskTrigger[] triggers, string[] dependentReferences)
        {
            var managedTask = new ManagedTask()
            {
                Reference = reference,
                OriginatorId = originatorId,
                Name = name,
                Category = category,
                Data = data,
                Action = action,
                Triggers = triggers,
                DependentReferences = dependentReferences
            };

            return Add(managedTask);
        }

        private void Trigger(object sender, EventArgs e)
        {
            var managedTask = (ManagedTask)sender;
            _taskHandler.Add(managedTask);
        }

        private void StatusChange(object sender, EManagedTaskStatus newStatus)
        {
            try
            {
                var managedTask = (ManagedTask)sender;

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

        private void ResetCompletedTask(ManagedTask managedTask)
        {
            if(managedTask.Schedule())
            {
                managedTask.Reset();
                //_taskHandler.Add(managedTask);
            }
            else
            {
                if (!_activeTasks.TryRemove(managedTask.Reference, out ManagedTask activeTask))
                {
                    _exitException = new ManagedTaskException(managedTask, "Failed to add the task to the active tasks list.");
                    _resetWhenNoTasks.Set();
                }
            }

            foreach (var activeTask in _activeTasks.Values)
            {
                if (activeTask.DependentReferences != null && activeTask.DependentReferences.Length > 0)
                {
                    var depFound = false;
                    foreach (var dep in activeTask.DependentReferences)
                    {
                        if (_activeTasks.ContainsKey(dep))
                        {
                            depFound = true;
                            break;
                        }
                    }

                    // if no depdent tasks are found, then the current task is ready to go.
                    if (!depFound)
                    {
                        activeTask.SetDepdenciesMet();
                        if (activeTask.Schedule())
                        {
                            _taskHandler.Add(activeTask);
                        }
                    }
                }
            }

            if (_activeTasks.Count == 0)
            {
                _resetWhenNoTasks.Set();
            }

        }

        public void ProgressChange(object sender, int percentage)
        {
            OnProgress?.Invoke(sender, percentage);
        }

        public Task WhenAll()
        {
            CancellationToken cancellationToken = CancellationToken.None;
            return WhenAll(cancellationToken);
        }


        public async Task WhenAll(CancellationToken cancellationToken)
        {
            if (_activeTasks.Count > 0 )
            {
                var noTasks = _resetWhenNoTasks.WaitAsync();
                await Task.WhenAny(noTasks, Task.Delay(-1, cancellationToken));

                if(_exitException != null)
                {
                    throw _exitException;
                }
            }
        }

        public IEnumerator<ManagedTask> GetEnumerator()
        {
            return _activeTasks.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}