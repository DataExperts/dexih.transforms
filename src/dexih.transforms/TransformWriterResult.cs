using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
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

        public TransformWriterResult(Int64 hubKey, Int64 auditKey, string auditType, Int64 referenceKey, Int64 parentAuditKey, string referenceName, Int64 sourceTableKey, string sourceTableName, Int64 targetTableKey, string targetTableName, Connection auditConnection, TransformWriterResult lastSuccessfulResult, ETriggerMethod triggerMethod, string triggerInfo)
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
            LastRowTotal = lastSuccessfulResult == null ? 0 : lastSuccessfulResult.RowsTotal;
            LastMaxIncrementalValue = lastSuccessfulResult == null ? null : lastSuccessfulResult.MaxIncrementalValue;

            InitializeTime = DateTime.Now;
            LastUpdateTime = InitializeTime;
            RunStatus = ERunStatus.Initialised;
            TriggerMethod = triggerMethod;
            TriggerInfo = triggerInfo;
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
            DataJob
        }

        private readonly Connection _auditConnection;

        public long AuditKey { get; set; }
        public string AuditType { get; set; }
        public Int64 ReferenceKey { get; set; }
        public Int64 ParentAuditKey { get; set; }
        public string ReferenceName { get; set; }
        public Int64 SourceTableKey { get; set; }
        public string SourceTableName { get; set; }
        public Int64 TargetTableKey { get; set; }
        public string TargetTableName { get; set; }

        public Int64 HubKey { get; set; }

        public long LastRowTotal { get; set; }
        public object LastMaxIncrementalValue { get; set; }

        public Int32 RowsPerProgressEvent { get; set; } = 1000;

        public long RowsTotal { get; set; }
        public long RowsCreated { get; set; }
        public long RowsUpdated { get; set; }
        public Int64 RowsDeleted { get; set; }
        public Int64 RowsPreserved { get; set; }
        public Int64 RowsIgnored { get; set; }
        public Int64 RowsRejected { get; set; }
        public Int64 RowsFiltered { get; set; }
        public Int64 RowsSorted { get; set; }
        public Int64 RowsReadPrimary { get; set; }
        public Int64 RowsReadReference { get; set; }

        public Int64 ReadTicks { get; set; }
        public Int64 WriteTicks { get; set; }
        public Int64 ProcessingTicks { get; set; }

        public object MaxIncrementalValue { get; set; }
        public long MaxSurrogateKey { get; set; }


        public string Message { get; set; }
        public DateTime InitializeTime { get; set; }
        public DateTime? ScheduledTime { get; set; } 
        public DateTime? StartTime { get; set; } 
        public DateTime? EndTime { get; set; } 
        public DateTime? LastUpdateTime { get; set; }
        public ETriggerMethod TriggerMethod { get; set; }
        public string TriggerInfo { get; set; }
        public string PerformanceSummary { get; set; }

        public string ProfileTableName { get; set; }
        public string RejectTableName { get; set; }

        private CancellationTokenSource CancelTokenSource { get; set; }

        public bool TruncateTarget { get; set; } //once off truncate of the target table.  
        public bool ResetIncremental { get; set; }
        public object ResetIncrementalValue { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
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
                TimeSpan ts = TimeSpan.FromTicks(WriteTicks);
                return (decimal)RowsTotal / Convert.ToDecimal(ts.TotalSeconds);
            }
        }

        public decimal ProcessingThroughput()
        {
            if (ProcessingTicks == 0)
                return 0;
            else
            {
                TimeSpan ts = TimeSpan.FromTicks(ProcessingTicks);
                return (decimal)(RowsReadPrimary + RowsReadReference) / Convert.ToDecimal(ts.TotalSeconds);
            }
        }

        public decimal ReadThroughput()
        {
            if (ReadTicks == 0)
                return 0;
            else
            {
                TimeSpan ts = TimeSpan.FromTicks(ReadTicks);
                return (decimal)(RowsReadPrimary + RowsReadReference) / Convert.ToDecimal(ts.TotalSeconds);
            }
        }

        public async Task<ReturnValue> SetRunStatus(ERunStatus newStatus, string message)
        {
            return await SetRunStatus(newStatus, new ReturnValue(false, message, null));
        }
        
        public async Task<ReturnValue> SetRunStatus(ERunStatus newStatus, ReturnValue returnValue = null)
        {
            RunStatus = newStatus;
            if(returnValue != null)
            {
                Message += Environment.NewLine + returnValue.ExceptionDetails;
            }

            if (RunStatus == ERunStatus.Abended || RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.FinishedErrors)
            {
                EndTime = DateTime.Now;
                OnFinish?.Invoke(this);
            }

            if (_auditConnection != null)
            {
                LastUpdateTime = DateTime.Now;

                var updateResult = await _auditConnection.UpdateAudit(this);
                if (!updateResult.Success)
                {
                    RunStatus = ERunStatus.Abended;
                    return updateResult;
                }
            }

            OnStatusUpdate?.Invoke(this);

            return new ReturnValue(true);
        }

        public bool IsRunning
        {
            get
            {
                return RunStatus == ERunStatus.Running || RunStatus == ERunStatus.RunningErrors || RunStatus == ERunStatus.Initialised || RunStatus == ERunStatus.Started;
            }
        }

        public bool IsFinished
        {
            get
            {
                return RunStatus == ERunStatus.Abended || RunStatus == ERunStatus.Cancelled || RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.FinishedErrors;
            }
        }

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

        public int PercentageComplete
        {
            get
            {
                if (RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.Abended) return 100;
                if (RunStatus == ERunStatus.Initialised) return 0;
                if (LastRowTotal == 0) return 50;
                int value = Convert.ToInt32(100 * ((double)RowsTotal / LastRowTotal));
                if (value > 100) return 100; else return value;
            }
        }


    }
}
