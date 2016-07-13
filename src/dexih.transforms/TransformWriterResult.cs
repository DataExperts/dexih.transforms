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

        public TransformWriterResult()
        {

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

        public decimal? ReadThroughput { get; set; }
        public decimal? WriteThroughput { get; set; }
        public decimal? ProcessingThroughput { get; set; }

        public object MaxIncrementalValue { get; set; }


        public string Message { get; set; }
        public DateTime InitialiseTime { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public DateTime LastUpdate { get; set; }
        public string PerformanceSummary { get; set; }
        private CancellationTokenSource CancelTokenSource { get; set; }

        private ERunStatus _RunStatus;

        [JsonConverter(typeof(StringEnumConverter))]
        public ERunStatus RunStatus {
            get {
                return _RunStatus;
            }
            set {
                LastUpdate = DateTime.Now;
                if (_RunStatus != value)
                {
                    _RunStatus = value;
                    OnStatusUpdate?.Invoke(this);
                }
            }
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
                LastUpdate = DateTime.Now;
                OnProgressUpdate?.Invoke(this);
            }
        }
    }
}
