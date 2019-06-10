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

namespace dexih.transforms
{
    /// <summary>
    /// Stores auditing information captured when using the TransformWriter.
    /// </summary>
    [PocoTable(Name = "DexihResults")]
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

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ERunStatus
        {
            Initialised,
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

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ETriggerMethod
        {
            NotTriggered,
            Manual,
            Schedule,
            FileWatcher,
            External,
            Datajob
        }
        
        [PocoColumn(Skip = true)]
        public TransformWriterOptions TransformWriterOptions { get; set; }


        [PocoColumn(DeltaType = TableColumn.EDeltaType.DbAutoIncrement, IsKey = true)]
        public long AuditKey { get; set; }

        [PocoColumn(MaxLength = 30)]
        public string AuditType { get; set; }

        public long ReferenceKey { get; set; }

        public long ParentAuditKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        public string ReferenceName { get; set; }
        public long SourceTableKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        public string SourceTableName { get; set; }

        public long TargetTableKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        public string TargetTableName { get; set; }

        public long HubKey { get; set; }
        
        /// <summary>
        /// The reference to the connection use for auditing (such as profile data).
        /// </summary>
        [PocoColumn(Skip = true)]
        public long AuditConnectionKey { get; set; }

        [PocoColumn(Skip = true)]
        public long LastRowTotal { get; set; }

        [PocoColumn(Skip = true)]
        public object LastMaxIncrementalValue { get; set; }

        [PocoColumn(Skip = true)]
        public int RowsPerProgressEvent { get; set; } = 1000;

        public long RowsTotal { get; set; }
        public long RowsCreated { get; set; }
        public long RowsUpdated { get; set; }
        public long RowsDeleted { get; set; }
        public long RowsPreserved { get; set; }
        public long RowsIgnored { get; set; }
        public long RowsRejected { get; set; }
        public long RowsFiltered { get; set; }
        public long RowsSorted { get; set; }
        public long RowsReadPrimary { get; set; }
        public long RowsReadReference { get; set; }
        
        public long Passed { get; set; }
        public long Failed { get; set; }

        public long ReadTicks { get; set; }
        public long WriteTicks { get; set; }
        public long ProcessingTicks { get; set; }

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 255, AllowDbNull = true)]
        public object MaxIncrementalValue { get; set; }

        public long MaxSurrogateKey { get; set; }

        [PocoColumn(MaxLength = 4000, AllowDbNull = true)]
        public string Message { get; set; }

        [PocoColumn(MaxLength = 255, DataType = ETypeCode.Text, AllowDbNull = true)]
        public string ExceptionDetails { get; set; }

        public DateTime InitializeTime { get; set; }
        public DateTime? ScheduledTime { get; set; } 
        public DateTime? StartTime { get; set; } 
        public DateTime? EndTime { get; set; } 
        public DateTime? LastUpdateTime { get; set; }

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        public ETriggerMethod TriggerMethod { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        public string TriggerInfo { get; set; }

        [PocoColumn(DataType = ETypeCode.Text, AllowDbNull = true)]
        public List<TransformPerformance> PerformanceSummary { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        public string ProfileTableName { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
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
        public bool IsCurrent { get; set; }
        public bool IsPrevious { get; set; }
        public bool IsPreviousSuccess { get; set; }

        [PocoColumn(Skip = true)]
        public Connection AuditConnection { get; set; }

        [PocoColumn(Skip = true)]
        public List<TransformWriterResult> ChildResults { get; set; } = new List<TransformWriterResult>();

        [JsonConverter(typeof(StringEnumConverter))]

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        public ERunStatus RunStatus { get; set; }

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
            if (ReadTicks == 0)
                return 0;
            else
            {
                var ts = TimeSpan.FromTicks(ReadTicks);
                return (decimal)(RowsReadPrimary + RowsReadReference) / Convert.ToDecimal(ts.TotalSeconds);
            }
        }

        public async Task<bool> Initialize(CancellationToken cancellationToken = default)
        {
            if (AuditConnection != null)
            {
                LastUpdateTime = DateTime.Now;
                await AuditConnection.InitializeAudit(this, cancellationToken);
            }

            return true;
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

        private Task _task;
        private bool _updateAudit;
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
            else
            {
            }
        }

        public async Task CompleteDatabaseWrites()
        {
            if (_task == null) return;

            try
            {
                await _task;
            }
            catch (Exception ex)
            {
                throw new TransformWriterException(
                    $"Error occurred saving audit data.  {ex.Message}",ex);
            }
        }

        public void Dispose()
        {
            if (_task == null || _task.IsCompleted)
            {
                return;
            }

            _task.Wait();
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

        private long _progressCounter;

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
