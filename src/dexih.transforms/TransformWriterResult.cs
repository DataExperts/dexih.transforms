﻿using dexih.functions;
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
using ProtoBuf;

namespace dexih.transforms
{
    /// <summary>
    /// Stores auditing information captured when using the TransformWriter.
    /// </summary>
    [PocoTable(Name = "DexihResults")]
    [ProtoContract]
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
        [ProtoMember(1)]
        public TransformWriterOptions TransformWriterOptions { get; set; }


        [PocoColumn(DeltaType = TableColumn.EDeltaType.DbAutoIncrement, IsKey = true)]
        [ProtoMember(2)]
        public long AuditKey { get; set; }

        [PocoColumn(MaxLength = 30)]
        [ProtoMember(3)]
        public string AuditType { get; set; }

        [ProtoMember(4)]
        public long ReferenceKey { get; set; }

        [ProtoMember(5)]
        public long ParentAuditKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        [ProtoMember(6)]
        public string ReferenceName { get; set; }

        [ProtoMember(7)]
        public long SourceTableKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        [ProtoMember(8)]
        public string SourceTableName { get; set; }

        [ProtoMember(9)]
        public long TargetTableKey { get; set; }

        [PocoColumn(MaxLength = 1024)]
        [ProtoMember(10)]
        public string TargetTableName { get; set; }

        [ProtoMember(11)]
        public long HubKey { get; set; }
        
        /// <summary>
        /// The reference to the connection use for auditing (such as profile data).
        /// </summary>
        [PocoColumn(Skip = true)]
        [ProtoMember(12)]
        public long AuditConnectionKey { get; set; }

        [PocoColumn(Skip = true)]
        [ProtoMember(13)]
        public long LastRowTotal { get; set; }

        [PocoColumn(Skip = true)]
        [ProtoMember(14)]
        public object LastMaxIncrementalValue { get; set; }

        [PocoColumn(Skip = true)]
        [ProtoMember(15)]
        public int RowsPerProgressEvent { get; set; } = 1000;

        [ProtoMember(16)]
        public long RowsTotal { get; set; }

        [ProtoMember(17)]
        public long RowsCreated { get; set; }

        [ProtoMember(18)]
        public long RowsUpdated { get; set; }

        [ProtoMember(19)]
        public long RowsDeleted { get; set; }

        [ProtoMember(20)]
        public long RowsPreserved { get; set; }

        [ProtoMember(21)]
        public long RowsIgnored { get; set; }

        [ProtoMember(22)]
        public long RowsRejected { get; set; }

        [ProtoMember(23)]
        public long RowsFiltered { get; set; }

        [ProtoMember(24)]
        public long RowsSorted { get; set; }

        [ProtoMember(25)]
        public long RowsReadPrimary { get; set; }

        [ProtoMember(26)]
        public long RowsReadReference { get; set; }

        [ProtoMember(27)]
        public long Passed { get; set; }

        [ProtoMember(28)]
        public long Failed { get; set; }

        [ProtoMember(29)]
        public long ReadTicks { get; set; }

        [ProtoMember(30)]
        public long WriteTicks { get; set; }

        [ProtoMember(31)]
        public long ProcessingTicks { get; set; }

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 255, AllowDbNull = true)]
        [ProtoMember(32)]
        public object MaxIncrementalValue { get; set; }

        [ProtoMember(33)]
        public long MaxSurrogateKey { get; set; }

        [PocoColumn(MaxLength = 4000, AllowDbNull = true)]
        [ProtoMember(34)]
        public string Message { get; set; }

        [PocoColumn(MaxLength = 255, DataType = ETypeCode.Text, AllowDbNull = true)]
        [ProtoMember(35)]
        public string ExceptionDetails { get; set; }

        [ProtoMember(36)]
        public DateTime InitializeTime { get; set; }

        [ProtoMember(37)]
        public DateTime? ScheduledTime { get; set; }

        [ProtoMember(38)]
        public DateTime? StartTime { get; set; }

        [ProtoMember(39)]
        public DateTime? EndTime { get; set; }

        [ProtoMember(40)]
        public DateTime? LastUpdateTime { get; set; }

        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        [ProtoMember(41)]
        public ETriggerMethod TriggerMethod { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        [ProtoMember(42)]
        public string TriggerInfo { get; set; }

        [PocoColumn(DataType = ETypeCode.Text, AllowDbNull = true)]
        [ProtoMember(43)]
        public List<TransformPerformance> PerformanceSummary { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        [ProtoMember(44)]
        public string ProfileTableName { get; set; }

        [PocoColumn(MaxLength = 1024, AllowDbNull = true)]
        [ProtoMember(45)]
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
        [ProtoMember(46)]
        public bool IsCurrent { get; set; }

        [ProtoMember(47)]
        public bool IsPrevious { get; set; }

        [ProtoMember(48)]
        public bool IsPreviousSuccess { get; set; }

        [PocoColumn(Skip = true)]
        [ProtoIgnore]
        public Connection AuditConnection { get; set; }

        [PocoColumn(Skip = true)]
        [ProtoMember(50)]
        public List<TransformWriterResult> ChildResults { get; set; } = new List<TransformWriterResult>();

        // [JsonConverter(typeof(StringEnumConverter))]
        [PocoColumn(DataType = ETypeCode.String, MaxLength = 20)]
        [ProtoMember(51)]
        public ERunStatus RunStatus { get; set; }

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
        
        public async Task Schedule(DateTime startTime, CancellationToken cancellationToken = default)
        {
            ResetStatistics();
            ScheduledTime = startTime;
            AuditKey = 0;
            RunStatus = ERunStatus.Scheduled;
            await AuditConnection.InitializeAudit(this, cancellationToken);
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
            _progressTimer?.Dispose();
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
