using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace dexih.functions
{
    public enum EManagedTask
    {
        Initialized, Started, Running, Success, Error
    }
    
    public class ManagedTask: ReturnValue, IDisposable
    {
        public delegate void Completed(string reference);
        public event Completed OnCompleted;

        public delegate void Progress(ManagedTask managedTask);
        public event Progress OnProgress;

        /// <summary>
        /// Unique key used to reference the task
        /// </summary>
        public string Reference { get; set; }
        
        /// <summary>
        /// Id that reference the originating client of the task.
        /// </summary>
        public string OriginatorId { get; set; }
        
        /// <summary>
        /// Short description for the task.
        /// </summary>
        public string Title { get; set; }
        
        /// <summary>
        /// When task was last updated.
        /// </summary>
        public DateTime LastUpdate { get; set; }

        public int Percentage { get; set; }

        /// <summary>
        /// Action that will be started and executed with the task.
        /// </summary>
        [JsonIgnore]
        public Action<IProgress<int>, CancellationToken> Action { get; set; }

        private CancellationTokenSource _cancellationTokenSource;
        
        private Task _task;
        private readonly IProgress<int> _progress;


        public ManagedTask()
        {
            var progressHandler = new Progress<int>(value =>
            {
                Percentage = value;
                OnProgress?.Invoke(this);
            });

            _progress = progressHandler;
        }

        public void Start()
        {
            _cancellationTokenSource = new CancellationTokenSource();

            _task = Task.Run(() => Action.Invoke(_progress, _cancellationTokenSource.Token), _cancellationTokenSource.Token);

            // when task finishes, raise an oncompleted event.
            _task.ContinueWith((task) => {
                switch (task.Status)
                {
                    case TaskStatus.Canceled:
                        Success = false;
                        Message = "The task was cancelled.";
                        return;
                    case TaskStatus.Created:
                        break;
                    case TaskStatus.Faulted:
                        Success = false;
                        Message = task.Exception?.Message;
                        Exception = task.Exception;
                        return;
                    case TaskStatus.RanToCompletion:
                        Success = true;
                        Message = "The task completed successfully.";
                        return;
                    case TaskStatus.Running:
                    case TaskStatus.WaitingForActivation:
                    case TaskStatus.WaitingForChildrenToComplete:
                    case TaskStatus.WaitingToRun:
                        Success = false;
                        Message = "Unexpected task status when should be completed: " + task.Status.ToString();
                        return;
                }
                OnCompleted?.Invoke(Reference);
                this.Percentage = 100;
                OnProgress?.Invoke(this);
            });
        }

        public  void Cancel()
        {
            _cancellationTokenSource.Cancel();
        }

        public void Dispose()
        {
            OnCompleted = null;
            _cancellationTokenSource.Dispose();
        }
    }
}
