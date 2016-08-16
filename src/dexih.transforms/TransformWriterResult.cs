using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class TransformWriterResult
    {
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult transformWriterResult);
        public delegate void StatusUpdate(TransformWriterResult transformWriterResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        #endregion

        public TransformWriterResult(Int64 subscriptionKey, Int64 auditKey, string auditType, Int64 referenceKey, string referenceName, Int64 sourceTableKey, string sourceTableName, Int64 targetTableKey, string targetTableName, Connection auditConnection, TransformWriterResult lastSuccessfulResult)
        {
            SubscriptionKey = subscriptionKey;
            AuditKey = auditKey;
            AuditType = auditType;
            ReferenceKey = referenceKey;
            ReferenceName = referenceName;
            SourceTableKey = sourceTableKey;
            SourceTableName = sourceTableName;
            TargetTableKey = targetTableKey;
            TargetTableName = targetTableName;
            AuditConnection = auditConnection;
            LastRowTotal = lastSuccessfulResult == null ? 0 : lastSuccessfulResult.RowsTotal;
            LastMaxIncrementalValue = lastSuccessfulResult == null ? null : lastSuccessfulResult.MaxIncrementalValue;

            InitializeTime = DateTime.Now;
            LastUpdateTime = InitializeTime;
            RunStatus = ERunStatus.Initialised;
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ERunStatus
        {
            Initialised,
            Started,
            Running,
            RunningErrors,
            Finished,
            FinishedErrors,
            Abended,
            Cancelled,
            NotRunning
        }

        private Connection AuditConnection;

        public Int64 AuditKey { get; set; }
        public string AuditType { get; set; }
        public Int64 ReferenceKey { get; set; }
        public string ReferenceName { get; set; }
        public Int64 SourceTableKey { get; set; }
        public string SourceTableName { get; set; }
        public Int64 TargetTableKey { get; set; }
        public string TargetTableName { get; set; }

        public Int64 SubscriptionKey { get; set; }

        public long LastRowTotal { get; set; }
        public object LastMaxIncrementalValue { get; set; }

        public Int32 RowsPerProgressEvent { get; set; } = 1000;

        public Int64 RowsTotal { get; set; }
        public Int64 RowsCreated { get; set; }
        public Int64 RowsUpdated { get; set; }
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
        public DateTime StartTime { get; set; } = Convert.ToDateTime("1900-01-01");
        public DateTime EndTime { get; set; } = Convert.ToDateTime("1900-01-01");
        public DateTime LastUpdateTime { get; set; }
        public string PerformanceSummary { get; set; }
        private CancellationTokenSource CancelTokenSource { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public ERunStatus RunStatus { get; set; }

        public TimeSpan? TimeTaken()
        {
            if (EndTime == Convert.ToDateTime("1900-01-01") || StartTime == Convert.ToDateTime("1900-01-01"))
                return null;
            else
                return EndTime.Subtract((DateTime)StartTime);
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
        public async Task<ReturnValue> SetRunStatus(ERunStatus newStatus, string message = null)
        {
            RunStatus = newStatus;
            if (message != null)
            {
                if (Message == null)
                    Message = message;
                else
                    Message += Environment.NewLine + message;
            }

            if (AuditConnection != null)
            {
                LastUpdateTime = DateTime.Now;

                var updateResult = await AuditConnection.UpdateAudit(this);
                if (!updateResult.Success)
                {
                    RunStatus = ERunStatus.Abended;
                    return updateResult;
                }
            }

            if (RunStatus == ERunStatus.Abended || RunStatus == ERunStatus.Finished || RunStatus == ERunStatus.FinishedErrors)
                EndTime = DateTime.Now;

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
