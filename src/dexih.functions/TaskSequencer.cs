using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace dexih.functions
{
    /// <summary>
    /// Sets up a task sequencer, that runs added tasks in order.
    /// </summary>
    public class TaskSequencer
    {
        #region Events
        public delegate void TaskSuccess(Task task);
        public delegate void TaskError(Task task, Exception exception);
        public delegate void TaskCancelled(Task task);

        public event TaskSuccess OnTaskSuccess;
        public event TaskError OnTaskError;
        public event TaskCancelled OnTaskCancelled;

        private object _lockControl = 1;
        private bool _completing = false;

        #endregion
        
        public TaskSequencer()
        {
            _actions = new ConcurrentQueue<Action>();
        }

        private readonly ConcurrentQueue<Action> _actions;
        private Task _task = null;

        /// <summary>
        /// Add a new action to the task queue.
        /// </summary>
        /// <param name="action"></param>
        public void AddAction(Action action)
        {
            if (_completing)
            {
                _actions.Enqueue(action);
            }
            else
            {
                lock (_lockControl)
                {
                    if (_task == null)
                    {
                        _task = StartTask(action);
                    }

                    _actions.Enqueue(action);
                }
            }
        }

        private Task StartTask(Action action)
        {
            var newTask = Task.Run(action);
            newTask.ContinueWith(task =>
            {
                lock (_lockControl)
                {
                    if (_completing) return;
                    UpdateTaskEvents(task);

                    if (_actions.TryDequeue(out var nextAction))
                    {
                        _task = StartTask(nextAction);
                    }
                    else
                    {
                        _task = null;
                    }
                }
            });

            return newTask;
        }

        private void UpdateTaskEvents(Task task)
        {
            if (task.IsFaulted)
            {
                OnTaskError?.Invoke(task, task.Exception);
            }

            if (task.IsCompletedSuccessfully)
            {
                OnTaskSuccess?.Invoke(task);
            }

            if (task.IsCanceled)
            {
                OnTaskCancelled?.Invoke(task);
            } 
        }

        /// <summary>
        /// Wait for all tasks to complete.
        /// </summary>
        /// <returns></returns>
        public async Task CompleteTasksAsync()
        {
            lock (_lockControl)
            {
                _completing = true;
            }

            if (_task != null)
            {
                await _task;
                UpdateTaskEvents(_task);
            }
            
            while (_actions.TryDequeue(out var action))
            {
                var task = Task.Run(action);
                try
                {
                    await task;
                }
                finally
                {
                    UpdateTaskEvents(task);
                }
            }

            _completing = false;
        }
    }
}