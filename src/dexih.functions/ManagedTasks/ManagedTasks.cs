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
        public delegate void Completed(ManagedTask managedTask);
        public event Completed OnCompleted;

		public delegate void Progress(ManagedTask managedTask);
		public event Progress OnProgress;

		private readonly ConcurrentDictionary<string, ManagedTask> _managedTasks;

        private AsyncAutoResetEvent _resetWhenNoTasks; //event handler that triggers when all tasks completed.

        public ManagedTasks()
        {
            _managedTasks = new ConcurrentDictionary<string, ManagedTask>();
            _resetWhenNoTasks = new AsyncAutoResetEvent();
        }
    
        public IEnumerator<ManagedTask> GetEnumerator()
        {
            return _managedTasks.Values.GetEnumerator();
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
        public string Add(string originatorId, string title, Action<IProgress<int>, CancellationToken> action)
        {
            
            var reference = Guid.NewGuid().ToString();

            var managedTask = new ManagedTask()
            {
                Reference = reference,
                OriginatorId = originatorId,
                Title =  title,
                Action = action,
                LastUpdate = DateTime.Now
            };
            
            managedTask.OnCompleted += ManagedTaskCompleted;
            managedTask.Start();

            _managedTasks.TryAdd(reference, managedTask);

            return reference;
        }

        public void ManagedTaskCompleted(string reference)
        {
            ManagedTask managedTask;
            _managedTasks.TryRemove(reference, out managedTask);
            managedTask.Dispose();
            OnCompleted?.Invoke(managedTask);
        }

        public void ManagedTaskProgress(ManagedTask managedTask)
        {
            OnProgress?.Invoke(managedTask);
        }

        public async Task WhenAll()
        {
            if (_managedTasks.Count > 0)
            {
                await _resetWhenNoTasks.WaitAsync();
            }
        }
    }
}