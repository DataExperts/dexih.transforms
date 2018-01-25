using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms.Poco;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms
{
    [PocoTable(Name = "DexihResults")]
    public class TransformWriterResult
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

        }

        public void SetProperties(long hubKey, long auditKey, string auditType, long referenceKey,
            long parentAuditKey, string referenceName, long sourceTableKey, string sourceTableName,
            long targetTableKey, string targetTableName, Connection auditConnection,
            TransformWriterResult lastSuccessfulResult, ETriggerMethod triggerMethod, string triggerInfo)
        {
            HubKey = hubKey;
            AuditKey = auditKey;
            AuditType = auditType;
            ReferenceKey = referenceKey;
            ParentAuditKey = parentAuditKey;
            ReferenceName = referenceName;
            SourceTableKey = sourceTableKey;
            SourceTableName = sourceTableName;
            TargetTableKey = targetTableKey;
            TargetTableName = targetTableName;
            _auditConnection = auditConnection;
            LastRowTotal = lastSuccessfulResult?.RowsTotal ?? 0;
            LastMaxIncrementalValue = lastSuccessfulResult?.MaxIncrementalValue;

            InitializeTime = DateTime.Now;
            LastUpdateTime = InitializeTime;
            RunStatus = ERunStatus.Initialised;
            TriggerMethod = triggerMethod;
            TriggerInfo = triggerInfo;

            IsCurrent = true;
            IsPrevious = false;
            IsPreviousSuccess = false;
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
            NotRunning
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


        [PocoColumn(DeltaType = TableColumn.EDeltaType.AutoIncrement, IsKey = true)]
        public long AuditKey { get; set; }

        [PocoColumn(MaxLength = 20)]
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

        [PocoColumn(MaxLength = 1024)]
        public string TriggerInfo { get; set; }

        [PocoColumn(MaxLength = 4000, AllowDbNull = true)]
        public string PerformanceSummary { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        public string ProfileTableName { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        public string RejectTableName { get; set; }


        [PocoColumn(Skip = true)]
        public bool TruncateTarget { get; set; } //once off truncate of the target table.  

        [PocoColumn(Skip = true)]
        public bool ResetIncremental { get; set; }

        [PocoColumn(Skip = true)]
        public object ResetIncrementalValue { get; set; }

        /// these are used when reading the from table, if record is the current version, previous version, or the previous version that was successful.
        public bool IsCurrent { get; set; }
        public bool IsPrevious { get; set; }
        public bool IsPreviousSuccess { get; set; }

        Connection _auditConnection;

        [PocoColumn(Skip = true)]
        public IEnumerable<TransformWriterResult> ChildResults { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        public ERunStatus RunStatus { get; set; }

        public TimeSpan? TimeTaken()
        {
            if (EndTime == null || StartTime == null)
                return null;
            else
                return EndTime.Value.Subtract((DateTime)StartTime.Value);
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


        /// <summary>
        /// Updates the run status, the audit table, and sends a status update event
        /// </summary>
        /// <param name="newStatus"></param>
        /// <param name="message"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        public async Task<bool> SetRunStatus(ERunStatus newStatus, string message, Exception exception, CancellationToken cancellationToken)
        {
            try
            {
                RunStatus = newStatus;
                if (!string.IsNullOrEmpty(message))
                {
                    Message = message;
                }

                if (exception != null)
                {
                    // pull out the full details of the exception.
                    var properties = exception.GetType().GetProperties();
                    var fields = properties
                        .Select(property => new
                        {
                            property.Name,
                            Value = property.GetValue(exception, null)
                        })
                        .Select(x => string.Format(
                            "{0} = {1}",
                            x.Name,
                            x.Value != null ? x.Value.ToString() : string.Empty
                        ));
                    ExceptionDetails = string.Join("\n", fields);
                }

                if (RunStatus == ERunStatus.Abended || RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.FinishedErrors || RunStatus == ERunStatus.Cancelled)
                {
                    EndTime = DateTime.Now;
                    OnFinish?.Invoke(this);
                }

                if (_auditConnection != null)
                {
                    LastUpdateTime = DateTime.Now;

                    try
                    {
                        await _auditConnection.UpdateAudit(this, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        RunStatus = ERunStatus.Abended;
                        Message = Message??"" + "\n" + $"An error occurred when updating the audit table of connection {_auditConnection.Name}.  {ex.Message}";
                        return false;
                    }
                }

                OnStatusUpdate?.Invoke(this);

                return true;
            }
            catch(Exception ex)
            {
                RunStatus = ERunStatus.Abended;
                Message = Message??"" + "\n" + $"An error occurred when updating run status.  {ex.Message}";
                return false;
            }
        }

        [PocoColumn(Skip = true)]
        public bool IsRunning
        {
            get
            {
                return RunStatus == ERunStatus.Running || RunStatus == ERunStatus.RunningErrors || RunStatus == ERunStatus.Initialised || RunStatus == ERunStatus.Started;
            }
        }

        [PocoColumn(Skip = true)]
        public bool IsFinished
        {
            get
            {
                return RunStatus == ERunStatus.Abended || RunStatus == ERunStatus.Cancelled || RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.FinishedErrors;
            }
        }

        [PocoColumn(Skip = true)]
        public bool IsScheduled
        {
            get
            {
                return RunStatus == ERunStatus.Scheduled;
            }
        }

        public TimeSpan? ScheduleDuration()
        {
            if (ScheduledTime == null)
                return null;
            else
                return DateTime.Now - ScheduledTime;
        }

        private int _progressCounter;

        public void IncrementRowsCreated(int value = 1)
        {
            RowsCreated += value;
            IncrementAll(value);
        }

        public void IncrementRowsUpdated(int value = 1)
        {
            RowsUpdated += value;
            IncrementAll(value);
        }

        public void IncrementRowsDeleted(int value = 1)
        {
            RowsDeleted += value;
            IncrementAll(value);
        }

        private void IncrementAll(int value)
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
