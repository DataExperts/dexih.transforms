using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace dexih.functions
{
    public enum EManagedTaskStatus
    {
        Initialized, Started, Running, Success, Error, Canceled
    }
    
    public class ManagedTask: ReturnValue, IDisposable
    {
        public event EventHandler OnStarted;
        public event EventHandler OnCompleted;
        public event EventHandler OnProgress;
        public event EventHandler OnCancelled;

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

        public EManagedTaskStatus Status { get; protected set; }

        public int Percentage { get; set; }

        /// <summary>
        /// Action that will be started and executed with the task.
        /// </summary>
        [JsonIgnore]
        public Func<IProgress<int>, CancellationToken, Task> Action { get; set; }

        private CancellationTokenSource _cancellationTokenSource;
        
        private Task _task;
        private readonly IProgress<int> _progress;


        public ManagedTask()
        {
            LastUpdate = DateTime.Now;
            Status = EManagedTaskStatus.Initialized;
            _cancellationTokenSource = new CancellationTokenSource();

            var progressHandler = new Progress<int>(value =>
            {
                Percentage = value;
                OnProgress?.Invoke(this, EventArgs.Empty);
            });

            _progress = progressHandler;
        }

        public void Start()
        {
            if (_cancellationTokenSource.IsCancellationRequested)
            {
                Status = EManagedTaskStatus.Canceled;
                Success = false;
                Message = "The task was cancelled.";
                Percentage = 100;
                OnCompleted?.Invoke(this, EventArgs.Empty);
            }

            Status = EManagedTaskStatus.Started;

            _task = Task.Run(async () =>
            {
                try
                {
                    OnStarted?.Invoke(this, EventArgs.Empty);
                    await Action(_progress, _cancellationTokenSource.Token);
                    
                    if(_cancellationTokenSource.IsCancellationRequested)
                    {
                        Status = EManagedTaskStatus.Canceled;
                        Success = false;
                        Message = "The task was cancelled.";
                    }
                    else
                    {
                        Status = EManagedTaskStatus.Success;
                        Success = true;
                        Message = "The task completed successfully.";
                    }

                    Percentage = 100;
                    OnCompleted?.Invoke(this, EventArgs.Empty);
                } catch (Exception ex)
                {
                    Status = EManagedTaskStatus.Error;
                    Message = ex.Message;
                    Exception = ex;
                    Success = false;
                    Percentage = 100;
                    OnCompleted?.Invoke(this, EventArgs.Empty);
                }
            });


            Status = EManagedTaskStatus.Running;
        }

        public  void Cancel()
        {
            Status = EManagedTaskStatus.Canceled;
            Success = false;
            Message = "The task was cancelled.";

            _cancellationTokenSource.Cancel();
            OnCancelled?.Invoke(this, EventArgs.Empty);
        }

        public void Dispose()
        {
            OnCompleted = null;
            OnStarted = null;
            _cancellationTokenSource.Dispose();
        }
    }
}
