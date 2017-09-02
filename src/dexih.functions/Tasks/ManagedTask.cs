using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Converters;
using System.Collections.Generic;

namespace dexih.functions.Tasks
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EManagedTaskStatus
    {
        Created, Paused, Scheduled, Queued, Running, Cancelled, Error, Completed
    }
    
    public class ManagedTask: ReturnValue, IDisposable
    {
        public event EventHandler<EManagedTaskStatus> OnStatus;
        public event EventHandler<int> OnProgress;
        public event EventHandler OnTrigger;

        /// <summary>
        /// Unique key used to reference the task
        /// </summary>
        public string Reference { get; set; }
        
        /// <summary>
        /// Id that reference the originating client of the task.
        /// </summary>
        public string OriginatorId { get; set; }
        
        /// <summary>
        /// Short name for the task.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// A description for the task
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// When task was last updated.
        /// </summary>
        public DateTime LastUpdate { get; set; }

        public EManagedTaskStatus Status { get; set; }

        public Object Data { get; set; }

        public string Category { get; set; }
		public long CatagoryKey { get; set; }
		public long HubKey { get; set; }

        public int Percentage { get; set; }

        public IEnumerable<ManagedTaskTrigger> Triggers { get; set; }

        public DateTime? NextTriggerTime { get; protected set; }

        public int RunCount { get; protected set; } = 0;

        /// <summary>
        /// Array of task reference which must be complete prior to this task.
        /// </summary>
        public string[] DependentReferences { get; set; }


        private bool _dependenciesMet;
        /// <summary>
        /// Flag to indicate dependent tasks have been completed.
        /// </summary>
        public bool DepedenciesMet {
            get => _dependenciesMet || DependentReferences == null || DependentReferences.Length == 0;
        }

        /// <summary>
        /// Action that will be started and executed when the task starts.
        /// </summary>
        [JsonIgnore]
        public Func<IProgress<int>, CancellationToken, Task> Action { get; set; }

        private CancellationTokenSource _cancellationTokenSource;
        
        private Task _task;
        private readonly IProgress<int> _progress;
        private Task _progressInvoke;
        private bool _anotherProgressInvoke = false;

        private Timer _timer;

        private Task _eventManager;

        private void SetStatus(EManagedTaskStatus newStatus)
        {
            if(newStatus > Status)
            {
                Status = newStatus;
                 OnStatus?.Invoke(this, Status);
            }
        }

        public ManagedTask()
        {
            LastUpdate = DateTime.Now;
            Status = EManagedTaskStatus.Created;
            _cancellationTokenSource = new CancellationTokenSource();
            _progress = new Progress<int>(value =>
            {
                if (Percentage != value)
                {
                    Percentage = value;
                    if (_progressInvoke == null || _progressInvoke.IsCompleted)
                    {
                        _progressInvoke = Task.Run(() =>
                        {
                            do
                            {
                                _anotherProgressInvoke = false;
                                OnProgress?.Invoke(this, Percentage);
                            } while (_anotherProgressInvoke);
                        });
                    }
                    else
                    {
                        _anotherProgressInvoke = true;
                    }
                }
            });
        }

        /// <summary>
        /// Start task schedule based on the "Triggers".
        /// </summary>
        public bool Schedule()
        {
            if(Status == EManagedTaskStatus.Queued || Status == EManagedTaskStatus.Running || Status == EManagedTaskStatus.Scheduled)
            {
                throw new ManagedTaskException(this, "The task cannot be scheduled as the status is already set to " + Status.ToString());
            }

            bool allowSchedule = false;

            if(DependentReferences != null && DependentReferences.Length > 0 && DepedenciesMet && RunCount == 0)
            {
                allowSchedule = true;
            }

            if (Triggers != null)
            {
                // loop through the triggers to find the one scheduled the soonest.
                DateTime? startAt = null;
                ManagedTaskTrigger startTrigger = null;
                foreach (var trigger in Triggers)
                {
                    var triggerTime = trigger.NextTrigger();
                    if (triggerTime != null && (startAt == null || triggerTime < startAt))
                    {
                        startAt = triggerTime;
                        startTrigger = trigger;
                    }
                }

                if(startAt != null)
                {
                    var timeToGo = startAt.Value - DateTime.Now;

                    if (timeToGo > TimeSpan.Zero)
                    {
                        NextTriggerTime = startAt;
                        //add a schedule.
                        _timer = new Timer(x => TriggerReady(startTrigger), null, timeToGo, Timeout.InfiniteTimeSpan);
                        allowSchedule = true;
                    }
                    else
                    {
                        TriggerReady(startTrigger);
                    }
                }
            }

            return allowSchedule;
        }

        public void TriggerReady(ManagedTaskTrigger trigger)
        {
            OnTrigger.Invoke(this, EventArgs.Empty);
        }

        public void Queue()
        {
            if (Status == EManagedTaskStatus.Queued || Status == EManagedTaskStatus.Running || Status == EManagedTaskStatus.Scheduled)
            {
                throw new ManagedTaskException(this, "The task cannot be queued for execution as the status is already set to " + Status.ToString());
            }
            SetStatus(EManagedTaskStatus.Queued);
        }

        /// <summary>
        /// Immediately start the task.
        /// </summary>
        public void Start()
        {
            RunCount++;

            if(Status == EManagedTaskStatus.Running)
            {
                throw new ManagedTaskException(this, "Task cannot be started as it is already running.");
            }

            // kill any active timers.
            if (_timer != null)
            {
                _timer.Dispose();
                NextTriggerTime = null;
            }

            if (_cancellationTokenSource.IsCancellationRequested)
            {
                Success = false;
                Message = "The task was cancelled.";
                Percentage = 100;
                SetStatus(EManagedTaskStatus.Cancelled);
                return;
            }

            _task = Task.Run(async () =>
            {
                try
                {
                    SetStatus(EManagedTaskStatus.Running);

                    try
                    {
                        await Action(_progress, _cancellationTokenSource.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        Success = false;
                        Message = "The task was cancelled.";
                        SetStatus(EManagedTaskStatus.Cancelled);
                        Percentage = 100;
                        return;
                    }

                    if (_cancellationTokenSource.IsCancellationRequested)
                    {
                        Success = false;
                        Message = "The task was cancelled.";
                        SetStatus(EManagedTaskStatus.Cancelled);
                    }
                    else
                    {
                        Success = true;
                        Message = "The task completed.";
                        SetStatus(EManagedTaskStatus.Completed);
                    }

                    Percentage = 100;
                    return;

                }
                catch (Exception ex)
                {
                    Message = ex.Message;
                    Exception = ex;
                    Success = false;
                    SetStatus(EManagedTaskStatus.Error);
                    Percentage = 100;
                    return;
                }

            }); //.ContinueWith((o) => Dispose());
        }

        public  void Cancel()
        {
            _cancellationTokenSource.Cancel();
            Success = false;
            Message = "The task was cancelled.";
            SetStatus(EManagedTaskStatus.Cancelled);
			if (_timer != null) _timer.Dispose();
		}

        public void Error(string message, Exception ex)
        {
            Success = false;
            Message = message;
            Exception = ex;
            SetStatus(EManagedTaskStatus.Error);
        }

        public void Reset()
        {
            //if (_timer != null) _timer.Dispose();
            Status = EManagedTaskStatus.Created;
            // SetStatus(EManagedTaskStatus.Created);
        }

        public void SetDepdenciesMet()
        {
            _dependenciesMet = true;
        }

        public void Dispose()
        {
            if (_timer != null) _timer.Dispose();
            OnProgress = null;
            OnStatus = null;
            OnTrigger = null;
           // _cancellationTokenSource.Dispose();
        }
    }
}
