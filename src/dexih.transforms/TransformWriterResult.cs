using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms.Exceptions;
using dexih.transforms.Poco;
using static Dexih.Utils.DataType.DataType;
using MessagePack;

namespace dexih.transforms
{
    /// <summary>
    /// Stores auditing information captured when using the TransformWriter.
    /// </summary>
    [PocoTable(Name = "DexihResults")]
    [MessagePackObject]
    public class TransformWriterResult: IDisposable
    {
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult transformWriterResult);
        public delegate void StatusUpdate(TransformWriterResult transformWriterResult);
        public delegate void Finish(TransformWriterResult transformWriterResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        public event Finish OnFinish;

        #endregion
        
        private Task _task;
        private bool _updateAudit;
        private Timer _progressTimer;
        private long _previousRows;
        private long _progressCounter;


        public TransformWriterResult()
        {
           
            InitializeTime = DateTime.Now;
            LastUpdateTime = InitializeTime;
            RunStatus = ERunStatus.Initialised;
            IsCurrent = true;
            IsPrevious = false;
            IsPreviousSuccess = false;
        }
        
        public TransformWriterResult(Connection auditConnection) : this()
        {
            AuditConnection = auditConnection;
        }

        // [JsonConverter(typeof(StringEnumConverter))]
        public enum ERunStatus
        {
            Initialised = 1,
            Scheduled,
            Started,
            Running,
            RunningErrors,
            Finished,
            FinishedErrors,
            Abended,
            Cancelled,
            NotRunning,
            Passed, //used for datalink tests
            Failed //
        }

        // [JsonConverter(typeof(StringEnumConverter))]
        public enum ETriggerMethod
        {
            NotTriggered = 1,
            Manual,
            Schedule,
            FileWatcher,
            External,
            Datajob
        }
        
        [PocoColumn(Skip = true)]
        [Key(0)]
        public TransformWriterOptions TransformWriterOptions { get; set; }


        [PocoColumn(DeltaType = TableColumn.EDeltaType.DbAutoIncrement, IsKey = true)]
        [Key(1)]
        public long AuditKey { get; set; }

        [PocoColumn(MaxLength = 30)]
        [Key(2)]
        public string AuditType { get; set; }

        [Key(3)]
        public long ReferenceKey { get; set; }

        [Key(4)]
        public long ParentAuditKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        [Key(5)]
        public string ReferenceName { get; set; }

        [Key(6)]
        public long SourceTableKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        [Key(7)]
        public string SourceTableName { get; set; }

        [Key(8)]
        public long TargetTableKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        [Key(9)]
        public string TargetTableName { get; set; }

        [Key(10)]
        public long HubKey { get; set; }
        
        /// <summary>
        /// The reference to the connection use for auditing (such as profile data).
        /// </summary>
        [PocoColumn(Skip = true)]
        [Key(11)]
        public long AuditConnectionKey { get; set; }

        [PocoColumn(Skip = true)]
        [Key(12)]
        public long LastRowTotal { get; set; }

        [PocoColumn(Skip = true)]
        [Key(13)]
        public object LastMaxIncrementalValue { get; set; }

        [PocoColumn(Skip = true)]
        [Key(14)]
        public int RowsPerProgressEvent { get; set; } = 1000;

        [Key(15)]
        public long RowsTotal { get; set; }

        [Key(16)]
        public long RowsCreated { get; set; }

        [Key(17)]
        public long RowsUpdated { get; set; }

        [Key(18)]
        public long RowsDeleted { get; set; }

        [Key(19)]
        public long RowsPreserved { get; set; }

        [Key(20)]
        public long RowsIgnored { get; set; }

        [Key(21)]
        public long RowsRejected { get; set; }

        [Key(22)]
        public long RowsFiltered { get; set; }

        [Key(23)]
        public long RowsSorted { get; set; }

        [Key(24)]
        public long RowsReadPrimary { get; set; }

        [Key(25)]
        public long RowsReadReference { get; set; }

        [Key(26)]
        public long Passed { get; set; }

        [Key(27)]
        public long Failed { get; set; }

        [Key(28)]
        public long ReadTicks { get; set; }

        [Key(29)]
        public long WriteTicks { get; set; }

        [Key(30)]
        public long ProcessingTicks { get; set; }

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 255, AllowDbNull = true)]
        [Key(32)]
        public object MaxIncrementalValue { get; set; }

        [Key(33)]
        public long MaxSurrogateKey { get; set; }

        [PocoColumn(MaxLength = 4000, AllowDbNull = true)]
        [Key(34)]
        public string Message { get; set; }

        [PocoColumn(MaxLength = 255, DataType = ETypeCode.Text, AllowDbNull = true)]
        [Key(35)]
        public string ExceptionDetails { get; set; }

        [Key(36)]
        public DateTime InitializeTime { get; set; }

        [Key(37)]
        public DateTime? ScheduledTime { get; set; }

        [Key(38)]
        public DateTime? StartTime { get; set; }

        [Key(39)]
        public DateTime? EndTime { get; set; }

        [Key(40)]
        public DateTime? LastUpdateTime { get; set; }

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        [Key(41)]
        public ETriggerMethod TriggerMethod { get; set; } = ETriggerMethod.Manual;

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        [Key(42)]
        public string TriggerInfo { get; set; }

        [PocoColumn(DataType = ETypeCode.Text, AllowDbNull = true)]
        [Key(43)]
        public List<TransformPerformance> PerformanceSummary { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        [Key(44)]
        public string ProfileTableName { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        [Key(45)]
        public string RejectTableName { get; set; }

        //        [PocoColumn(Skip = true)]
        //        public bool TruncateTarget { get; set; } //once off truncate of the target table.  
        //
        //        [PocoColumn(Skip = true)]
        //        public bool ResetIncremental { get; set; }
        //
        //        [PocoColumn(Skip = true)]
        //        public object ResetIncrementalValue { get; set; }

        /// these are used when reading the from table, if record is the current version, previous version, or the previous version that was successful.
        [Key(46)]
        public bool IsCurrent { get; set; }

        [Key(47)]
        public bool IsPrevious { get; set; }

        [Key(48)]
        public bool IsPreviousSuccess { get; set; }

        [PocoColumn(Skip = true)]
        [IgnoreMember]
        public Connection AuditConnection { get; set; }

        [PocoColumn(Skip = true)]
        [Key(50)]
        public List<TransformWriterResult> ChildResults { get; set; } = new List<TransformWriterResult>();

        // [JsonConverter(typeof(StringEnumConverter))]
        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        [Key(51)]
        public ERunStatus RunStatus { get; set; } = ERunStatus.NotRunning;

        public void ResetStatistics()
        {
            RowsTotal = 0;
            RowsCreated = 0;
            RowsUpdated = 0;
            RowsDeleted = 0;
            RowsPreserved = 0;
            RowsIgnored = 0;
            RowsRejected = 0;
            RowsFiltered = 0;
            RowsSorted = 0;
            RowsReadPrimary = 0;
            RowsReadReference = 0;
            Passed = 0;
            Failed = 0;
            ReadTicks = 0;
            WriteTicks = 0;
            ProcessingTicks = 0;
            MaxIncrementalValue = null;
            MaxSurrogateKey = 0;
            Message = null;
            ExceptionDetails = null;
            InitializeTime = default;
            ScheduledTime = default;
            StartTime = default;
            EndTime = default;
            LastUpdateTime = DateTime.Now;
            PerformanceSummary = null;
        }
        public TimeSpan? TimeTaken()
        {
            if (EndTime == null || StartTime == null)
                return null;
            else
                return EndTime.Value.Subtract(StartTime.Value);
        }

        public decimal WriteThroughput()
        {
            if (WriteTicks == 0)
                return 0;
            else
            {
                var ts = TimeSpan.FromTicks(WriteTicks);
                return (decimal)RowsTotal / Convert.ToDecimal(ts.TotalSeconds);
            }
        }

        public decimal ProcessingThroughput()
        {
            if (ProcessingTicks == 0)
                return 0;
            else
            {
                var ts = TimeSpan.FromTicks(ProcessingTicks);
                return (decimal)(RowsReadPrimary + RowsReadReference) / Convert.ToDecimal(ts.TotalSeconds);
            }
        }

        public decimal ReadThroughput()
        {
            if (ReadTicks == 0) { return 0; }

            var ts = TimeSpan.FromTicks(ReadTicks);
            return (RowsReadPrimary + RowsReadReference) / Convert.ToDecimal(ts.TotalSeconds);
        }

        /// <summary>
        /// Creates a new database row (if an audit connection exists) and starts sending out periodic progress updates.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> Initialize(CancellationToken cancellationToken = default)
        {
            if (AuditConnection != null && AuditKey <= 0)
            {
                LastUpdateTime = DateTime.Now;
                await AuditConnection.InitializeAudit(this, cancellationToken);
            }
            else
            {
                
            }
            
            _progressTimer = new Timer(CheckProgress, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

            return true;
        }
        
        public void Schedule(DateTime startTime, CancellationToken cancellationToken = default)
        {
            ResetStatistics();
            ScheduledTime = startTime;
            AuditKey = 0;
            RunStatus = ERunStatus.Scheduled;
            // await AuditConnection.InitializeAudit(this, cancellationToken);
            OnStatusUpdate?.Invoke(this);
        }

        private void CheckProgress(object value)
        {
            if (_previousRows != _progressCounter)
            {
                OnProgressUpdate?.Invoke(this);
                _previousRows = _progressCounter;
            }
        }

        private void DbOperationFailed(Task task, Exception exception)
        {
            AddExceptionDetails(exception);
            RunStatus = ERunStatus.Abended;
            AddMessage($"An error occurred when updating the audit table of connection {AuditConnection.Name}.  {exception.Message}");
        }

        /// <summary>
        /// Updates the run status, the audit table, and sends a status update event
        /// </summary>
        /// <param name="newStatus"></param>
        /// <param name="message"></param>
        /// <param name="exception"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public bool SetRunStatus(ERunStatus newStatus, string message, Exception exception)
        {
            try
            {
                LastUpdateTime = DateTime.Now;
                RunStatus = newStatus;

                AddMessage(message);
                AddExceptionDetails(exception);

                if (RunStatus == ERunStatus.Started)
                {
                    StartTime = DateTime.Now;
                }

                
                if (IsFinished)
                {
                    EndTime = DateTime.Now;
                    OnFinish?.Invoke(this);
                }

                if (AuditConnection != null)
                {
                    LastUpdateTime = DateTime.Now;
                    UpdateDatabaseTask();
                }

                return true;
            }
            catch (Exception ex)
            {
                AddExceptionDetails(ex);
                RunStatus = ERunStatus.Abended;
                AddMessage($"An error occurred when updating run status.  {ex.Message}");
                return false;
            }
            finally
            {
                OnStatusUpdate?.Invoke(this);
            }
        }

        private void UpdateDatabaseTask()
        {
            _updateAudit = true;

            if (_task != null && _task.IsCompleted)
            {
                if (_task.IsFaulted)
                {
                    throw new TransformWriterException(
                        $"Error occurred saving audit data.  {_task.Exception?.Message}", _task.Exception);
                }

                _task = null;
            }

            if (_task == null)
            {
                _task = Task.Run(async () =>
                {
                    while (_updateAudit)
                    {
                        _updateAudit = false;
                        await AuditConnection.UpdateAudit(this, CancellationToken.None);
                    }
                });
            }
        }

        public async Task CompleteDatabaseWrites()
        {
            if (_task == null) return;

            if (_task.IsFaulted)
            {
                throw new TransformWriterException(
                    $"Error occurred saving audit data.  {_task.Exception?.Message}", _task.Exception);
            }

            if (_task.IsCompleted || _task.IsCanceled)
            {
                return;
            }
            
            try
            {
                await _task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new TransformWriterException(
                    $"Error occurred saving audit data.  {ex.Message}",ex);
            }
        }

        public void Dispose()
        {
            _progressTimer?.Dispose();
            if (_task == null || _task.IsCompleted)
            {
                return;
            }
            else
            {
                AsyncHelper.RunSync(() => _task);    
            }

            _task = null;
        }

        private void AddExceptionDetails(Exception exception)
        {
            if (exception == null)
            {
                return;
            }
            
            // pull out the full details of the exception.
            var properties = exception.GetType().GetProperties();
            var fields = properties
                .Select(property => new
                {
                    property.Name,
                    Value = property.GetValue(exception, null)
                })
                .Select(x => $"{x.Name} = {(x.Value != null ? x.Value.ToString() : string.Empty)}");
            var details = string.Join("\n", fields);

            if (string.IsNullOrEmpty(ExceptionDetails))
            {
                ExceptionDetails = details;
            }
            else
            {
                ExceptionDetails += details + "\n\n" + ExceptionDetails;
            }
        }

        private void AddMessage(string message)
        {
            if (string.IsNullOrEmpty(message))
            {
                return;
            }
            
            if (string.IsNullOrEmpty(Message))
            {
                Message = message;
            }
            else
            {
                Message += message + "\n\n" + Message;
            }
        }

        [PocoColumn(Skip = true)]
        public bool IsRunning => RunStatus == ERunStatus.Running || RunStatus == ERunStatus.RunningErrors || RunStatus == ERunStatus.Initialised || RunStatus == ERunStatus.Started;

        [PocoColumn(Skip = true)]
        public bool IsFinished => RunStatus == ERunStatus.Abended || RunStatus == ERunStatus.Cancelled || RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.FinishedErrors || RunStatus == ERunStatus.Passed || RunStatus == ERunStatus.Failed;

        [PocoColumn(Skip = true)]
        public bool IsScheduled => RunStatus == ERunStatus.Scheduled;

        public TimeSpan? ScheduleDuration()
        {
            if (ScheduledTime == null)
                return null;
            else
                return DateTime.Now - ScheduledTime;
        }

        public void IncrementRowsReadPrimary(long value = 1)
        {
            RowsReadPrimary += value;
            _progressCounter += value;
            IncrementAll(0);
        }

        public void IncrementRowsCreated(long value = 1)
        {
            RowsCreated += value;
            IncrementAll(value);
        }

        public void IncrementRowsUpdated(long value = 1)
        {
            RowsUpdated += value;
            IncrementAll(value);
        }

        public void IncrementRowsDeleted(long value = 1)
        {
            RowsDeleted += value;
            IncrementAll(value);
        }

        private void IncrementAll(long value)
        {
            RowsTotal += value;
            _progressCounter += value;

            if (_progressCounter >= RowsPerProgressEvent)
            {
                _progressCounter = 0;
                _previousRows = 0;
                _progressTimer.Change(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
                LastUpdateTime = DateTime.Now;
                OnProgressUpdate?.Invoke(this);
            }
        }

        [PocoColumn(Skip = true)]
        public int PercentageComplete
        {
            get
            {
                if (RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.Abended) return 100;
                if (RunStatus == ERunStatus.Initialised) return 0;
                if (LastRowTotal == 0) return 50;
                var value = Convert.ToInt32(100 * ((double)RowsTotal / LastRowTotal));
                if (value > 100) return 100; else return value;
            }
        }

        

    }
}
