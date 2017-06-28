using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using System.Threading;
using dexih.transforms;
using dexih.functions;
using Newtonsoft.Json.Linq;
using System.Diagnostics;
using System.Data.Common;
using static dexih.functions.DataType;
using static dexih.transforms.Transform;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Net.Http;

namespace dexih.transforms
{
    public abstract class Connection
    {

        #region Enums

        public enum EConnectionState
        {
            Broken = 0,
            Open = 1,
            Closed = 2,
            Fetching = 3,
            Connecting = 4,
            Executing = 5
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ECategory
        {
            SqlDatabase = 0,
            NoSqlDatabase = 1,
            File = 2,
            WebService = 3,
			Hub = 4
        }

        #endregion

        #region Properties

        public string Name { get; set; }
        public string Server { get; set; }
        public bool Ntauth { get; set; }
        public string Username { get; set; } = "";
        public string Password { get; set; } = "";
        public string DefaultDatabase { get; set; }
        public string Filename { get; set; }
        public EConnectionState State { get; set; }

        public bool UseConnectionString { get; set; }
        public string ConnectionString { get; set; }


        #endregion

        #region Abstracts

        //Abstract Properties
        public abstract string ServerHelp { get; } //help text for what the server means for this description
        public abstract string DefaultDatabaseHelp { get; } //help text for what the default database means for this description

        public abstract string DatabaseTypeName { get; }
        public abstract ECategory DatabaseCategory { get; }
        public abstract bool AllowNtAuth { get; }
        public abstract bool AllowUserPass { get; }

        public abstract bool CanBulkLoad { get; }
        public abstract bool CanSort { get; }
        public abstract bool CanFilter { get; }
        public abstract bool CanAggregate { get; }

        //Functions required for managed connection
        public abstract Task<ReturnValue> CreateTable(Table table, bool dropTable = false);
        //public abstract Task<ReturnValue> TestConnection();
        public abstract Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken);
        public abstract Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        /// <param name="queries"></param>
        /// <param name="cancelToken"></param>
        /// <returns>Item1 = elapsed time, Item2 = autoincrement value</returns>
        public abstract Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken);

        /// <summary>
        /// Runs a bulk insert operation for the connection.  
        /// </summary>
        /// <param name="table"></param>
        /// <param name="sourceData"></param>
        /// <param name="cancelToken"></param>
        /// <returns>ReturnValue with the value = elapsed timer ticks taken to write the record.</returns>
        public abstract Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken);
        public abstract Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken);
        public abstract Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null);
        public abstract Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken);
        public abstract Task<ReturnValue<bool>> TableExists(Table table);

        /// <summary>
        /// If database connection supports direct DbDataReader.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query = null);

        //Functions required for datapoint.
        public abstract Task<ReturnValue> CreateDatabase(string databaseName);
        public abstract Task<ReturnValue<List<string>>> GetDatabaseList();
        public abstract Task<ReturnValue<List<Table>>> GetTableList();

        /// <summary>
        /// Interrogates the underlying data to get the Table structure.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="Properties"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue<Table>> GetSourceTableInfo(Table table);

        /// <summary>
        /// Adds any database specific mandatory column to the table object.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="position"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue> AddMandatoryColumns(Table table, int position);

        public Stopwatch WriteDataTimer = new Stopwatch();


        #endregion

        #region Audit

        public virtual Table AuditTable
        {
            get
            {
                Table auditTable = new Table("DexihHistory");
                AddMandatoryColumns(auditTable, 0).Wait();

                auditTable.Columns.Add(new TableColumn("AuditKey", ETypeCode.Int64, TableColumn.EDeltaType.AutoIncrement));
                auditTable.Columns.Add(new TableColumn("HubKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("AuditType", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 20 });
                auditTable.Columns.Add(new TableColumn("ReferenceKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("ParentAuditKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("ReferenceName", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 1024 });
                auditTable.Columns.Add(new TableColumn("SourceTableKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("SourceTableName", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 1024 });
                auditTable.Columns.Add(new TableColumn("TargetTableKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("TargetTableName", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 1024 });
                auditTable.Columns.Add(new TableColumn("RowsTotal", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsCreated", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsUpdated", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsDeleted", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsPreserved", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsIgnored", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsRejected", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsSorted", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsFiltered", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsReadPrimary", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("RowsReadReference", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("ReadTicks", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("WriteTicks", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("ProcessingTicks", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("MaxIncrementalValue", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 255, AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("MaxSurrogateKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField) { MaxLength = 255, AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("InitializeTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("ScheduledTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("StartTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("EndTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("LastUpdateTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("RunStatus", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 20 });
                auditTable.Columns.Add(new TableColumn("TriggerMethod", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 20 });
                auditTable.Columns.Add(new TableColumn("TriggerInfo", ETypeCode.String, TableColumn.EDeltaType.TrackingField) );
                auditTable.Columns.Add(new TableColumn("Message", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("IsCurrent", ETypeCode.Boolean, TableColumn.EDeltaType.TrackingField) { AllowDbNull = false });
                auditTable.Columns.Add(new TableColumn("IsPrevious", ETypeCode.Boolean, TableColumn.EDeltaType.TrackingField) { AllowDbNull = false });
                auditTable.Columns.Add(new TableColumn("IsPreviousSuccess", ETypeCode.Boolean, TableColumn.EDeltaType.TrackingField) { AllowDbNull = false });
                return auditTable;
            }
        }

        public virtual async Task<ReturnValue<TransformWriterResult>> InitializeAudit(long subScriptionKey, string auditType, Int64 referenceKey, Int64 parentAuditKey, string referenceName, Int64 sourceTableKey, string sourceTableName, Int64 targetTableKey, string targetTableName, TransformWriterResult.ETriggerMethod triggerMethod, string triggerInfo)
        {
            var auditTable = AuditTable;

            var tableExistsResult = await TableExists(auditTable);

            TransformWriterResult previousResult = null;

            if (!tableExistsResult.Success)
                return new ReturnValue<TransformWriterResult>(tableExistsResult);

            //create the audit table if it does not exist.
            if (tableExistsResult.Value == false)
            {
                //create the table if is doesn't already exist.
                var createAuditResult = await CreateTable(auditTable, false);
                if (!createAuditResult.Success)
                    return new ReturnValue<TransformWriterResult>(createAuditResult);
            }
            else
            {
                //get the last audit result for this reference to collect previous run information
                var lastAuditResult = await GetPreviousResult(subScriptionKey, referenceKey, CancellationToken.None);
                if (!lastAuditResult.Success)
                    return new ReturnValue<TransformWriterResult>(lastAuditResult);
                previousResult = lastAuditResult.Value;
            }

            var writerResult = new TransformWriterResult(subScriptionKey, 0, auditType, referenceKey, parentAuditKey, referenceName, sourceTableKey, sourceTableName, targetTableKey, targetTableName, this, previousResult, triggerMethod, triggerInfo);

            //note AuditKey not included in query columns as it is an AutoGenerate type.
            var queryColumns = new List<QueryColumn>
                {
                    new QueryColumn(new TableColumn("HubKey", ETypeCode.Int64),  writerResult.HubKey),
                    new QueryColumn(new TableColumn("AuditType", ETypeCode.String),  writerResult.AuditType),
                    new QueryColumn(new TableColumn("ReferenceKey", ETypeCode.Int64), writerResult.ReferenceKey),
                    new QueryColumn(new TableColumn("ParentAuditKey", ETypeCode.Int64), writerResult.ParentAuditKey),
                    new QueryColumn(new TableColumn("ReferenceName", ETypeCode.String), writerResult.ReferenceName),
                    new QueryColumn(new TableColumn("SourceTableKey", ETypeCode.Int64), writerResult.SourceTableKey),
                    new QueryColumn(new TableColumn("SourceTableName", ETypeCode.String), writerResult.SourceTableName),
                    new QueryColumn(new TableColumn("TargetTableKey", ETypeCode.Int64), writerResult.TargetTableKey),
                    new QueryColumn(new TableColumn("TargetTableName", ETypeCode.String), writerResult.TargetTableName),
                    new QueryColumn(new TableColumn("RowsTotal", ETypeCode.Int64), writerResult.RowsTotal),
                    new QueryColumn(new TableColumn("RowsCreated", ETypeCode.Int64), writerResult.RowsCreated),
                    new QueryColumn(new TableColumn("RowsUpdated", ETypeCode.Int64), writerResult.RowsUpdated),
                    new QueryColumn(new TableColumn("RowsDeleted", ETypeCode.Int64), writerResult.RowsDeleted),
                    new QueryColumn(new TableColumn("RowsPreserved", ETypeCode.Int64), writerResult.RowsPreserved),
                    new QueryColumn(new TableColumn("RowsIgnored", ETypeCode.Int64), writerResult.RowsIgnored),
                    new QueryColumn(new TableColumn("RowsRejected", ETypeCode.Int64), writerResult.RowsRejected),
                    new QueryColumn(new TableColumn("RowsSorted", ETypeCode.Int64), writerResult.RowsSorted),
                    new QueryColumn(new TableColumn("RowsFiltered", ETypeCode.Int64), writerResult.RowsFiltered),
                    new QueryColumn(new TableColumn("RowsReadPrimary", ETypeCode.Int64), writerResult.RowsReadPrimary),
                    new QueryColumn(new TableColumn("RowsReadReference", ETypeCode.Int64), writerResult.RowsReadReference),
                    new QueryColumn(new TableColumn("ReadTicks", ETypeCode.Int64), writerResult.ReadTicks),
                    new QueryColumn(new TableColumn("WriteTicks", ETypeCode.Int64), writerResult.WriteTicks),
                    new QueryColumn(new TableColumn("ProcessingTicks", ETypeCode.Int64), writerResult.ProcessingTicks),
                    new QueryColumn(new TableColumn("MaxIncrementalValue", ETypeCode.String), writerResult.MaxIncrementalValue),
                    new QueryColumn(new TableColumn("MaxSurrogateKey", ETypeCode.Int64), writerResult.MaxSurrogateKey),
                    new QueryColumn(new TableColumn("InitializeTime", ETypeCode.DateTime), writerResult.InitializeTime),
                    new QueryColumn(new TableColumn("ScheduledTime", ETypeCode.DateTime), writerResult.ScheduledTime),
                    new QueryColumn(new TableColumn("StartTime", ETypeCode.DateTime), writerResult.StartTime),
                    new QueryColumn(new TableColumn("EndTime", ETypeCode.DateTime), writerResult.EndTime),
                    new QueryColumn(new TableColumn("LastUpdateTime", ETypeCode.DateTime), writerResult.LastUpdateTime),
                    new QueryColumn(new TableColumn("RunStatus", ETypeCode.String), writerResult.RunStatus.ToString()),
                    new QueryColumn(new TableColumn("TriggerMethod", ETypeCode.String), writerResult.TriggerMethod.ToString()),
                    new QueryColumn(new TableColumn("TriggerInfo", ETypeCode.String), writerResult.TriggerInfo),
                    new QueryColumn(new TableColumn("Message", ETypeCode.DateTime), writerResult.Message),
                    new QueryColumn(new TableColumn("IsCurrent", ETypeCode.Boolean), true),
                    new QueryColumn(new TableColumn("IsPrevious", ETypeCode.Boolean), false),
                    new QueryColumn(new TableColumn("IsPreviousSuccess", ETypeCode.Boolean), false)

            };

            var partitionKeyOrdinal = auditTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzurePartitionKey);
            if (partitionKeyOrdinal > 0)
                queryColumns.Add(new QueryColumn(new TableColumn( auditTable.Columns[partitionKeyOrdinal].ColumnName, ETypeCode.String), "AuditRow"));

            var insertQuery = new InsertQuery(auditTable.TableName, queryColumns);
            var insertResult = await ExecuteInsert(auditTable, new List<InsertQuery>() { insertQuery }, CancellationToken.None);

            writerResult.AuditKey = insertResult.Value.Item2;

            if (!insertResult.Success)
                return new ReturnValue<TransformWriterResult>(insertResult);

            return new ReturnValue<TransformWriterResult>(true, writerResult);
        }

        public virtual async Task<ReturnValue> UpdateAudit(TransformWriterResult writerResult)
        {
            bool isCurrent = true;
            bool isPrevious = false;
            bool isPreviousSuccess = false;

            //when the runstatuss is finished or finished with errors, set the previous success record to false.
            if (writerResult.RunStatus == TransformWriterResult.ERunStatus.Finished || writerResult.RunStatus == TransformWriterResult.ERunStatus.FinishedErrors)
            {
                var updateLatestColumn = new List<QueryColumn>() {
                    new QueryColumn(new TableColumn("IsCurrent", ETypeCode.Boolean), false),
                    new QueryColumn(new TableColumn("IsPreviousSuccess", ETypeCode.Boolean), false)
                };

                var updateLatestFilters = new List<Filter>() {
                    new Filter(new TableColumn("HubKey", ETypeCode.Int64), Filter.ECompare.IsEqual, writerResult.HubKey),
                    new Filter(new TableColumn("ReferenceKey", ETypeCode.Int64), Filter.ECompare.IsEqual, writerResult.ReferenceKey),
                    new Filter(new TableColumn("IsPreviousSuccess", ETypeCode.Boolean), Filter.ECompare.IsEqual, true),
                };

                var updateIsLatest = new UpdateQuery(AuditTable.TableName, updateLatestColumn, updateLatestFilters);
                var updateLatestResult = await ExecuteUpdate(AuditTable, new List<UpdateQuery>() { updateIsLatest }, CancellationToken.None);

                isPreviousSuccess = true;
            }

            //when finished, mark the previous result to false.
            if (writerResult.IsFinished)
            {
                var updateLatestColumn = new List<QueryColumn>() {
                    new QueryColumn(new TableColumn("IsCurrent", ETypeCode.Boolean), false),
                    new QueryColumn(new TableColumn("IsPrevious", ETypeCode.Boolean), false)
                };

                var updateLatestFilters = new List<Filter>() {
                    new Filter(new TableColumn("HubKey", ETypeCode.Int64), Filter.ECompare.IsEqual, writerResult.HubKey),
                    new Filter(new TableColumn("ReferenceKey", ETypeCode.Int64), Filter.ECompare.IsEqual, writerResult.ReferenceKey),
                    new Filter(new TableColumn("IsPrevious", ETypeCode.Boolean), Filter.ECompare.IsEqual, true),
                };

                var updateIsLatest = new UpdateQuery(AuditTable.TableName, updateLatestColumn, updateLatestFilters);
                var updateLatestResult = await ExecuteUpdate(AuditTable, new List<UpdateQuery>() { updateIsLatest }, CancellationToken.None);

                isCurrent = false;
                isPrevious = true;
            }


            var updateColumns = new List<QueryColumn>()
            {
                    new QueryColumn(new TableColumn("AuditType", ETypeCode.String),  writerResult.AuditType),
                    new QueryColumn(new TableColumn("HubKey", ETypeCode.Int64),  writerResult.HubKey),
                    new QueryColumn(new TableColumn("ReferenceKey", ETypeCode.Int64), writerResult.ReferenceKey),
                    new QueryColumn(new TableColumn("ParentAuditKey", ETypeCode.Int64), writerResult.ParentAuditKey),
                    new QueryColumn(new TableColumn("ReferenceName", ETypeCode.String), writerResult.ReferenceName),
                    new QueryColumn(new TableColumn("SourceTableKey", ETypeCode.Int64), writerResult.SourceTableKey),
                    new QueryColumn(new TableColumn("SourceTableName", ETypeCode.String), writerResult.SourceTableName),
                    new QueryColumn(new TableColumn("TargetTableKey", ETypeCode.Int64), writerResult.TargetTableKey),
                    new QueryColumn(new TableColumn("TargetTableName", ETypeCode.String), writerResult.TargetTableName),
                    new QueryColumn(new TableColumn("RowsTotal", ETypeCode.Int64), writerResult.RowsTotal),
                    new QueryColumn(new TableColumn("RowsCreated", ETypeCode.Int64), writerResult.RowsCreated),
                    new QueryColumn(new TableColumn("RowsUpdated", ETypeCode.Int64), writerResult.RowsUpdated),
                    new QueryColumn(new TableColumn("RowsDeleted", ETypeCode.Int64), writerResult.RowsDeleted),
                    new QueryColumn(new TableColumn("RowsPreserved", ETypeCode.Int64), writerResult.RowsPreserved),
                    new QueryColumn(new TableColumn("RowsIgnored", ETypeCode.Int64), writerResult.RowsIgnored),
                    new QueryColumn(new TableColumn("RowsRejected", ETypeCode.Int64), writerResult.RowsRejected),
                    new QueryColumn(new TableColumn("RowsSorted", ETypeCode.Int64), writerResult.RowsSorted),
                    new QueryColumn(new TableColumn("RowsFiltered", ETypeCode.Int64), writerResult.RowsFiltered),
                    new QueryColumn(new TableColumn("RowsReadPrimary", ETypeCode.Int64), writerResult.RowsReadPrimary),
                    new QueryColumn(new TableColumn("RowsReadReference", ETypeCode.Int64), writerResult.RowsReadReference),
                    new QueryColumn(new TableColumn("ReadTicks", ETypeCode.Int64), writerResult.ReadTicks),
                    new QueryColumn(new TableColumn("WriteTicks", ETypeCode.Int64), writerResult.WriteTicks),
                    new QueryColumn(new TableColumn("ProcessingTicks", ETypeCode.Int64), writerResult.ProcessingTicks),
                    new QueryColumn(new TableColumn("MaxIncrementalValue", ETypeCode.String), writerResult.MaxIncrementalValue),
                    new QueryColumn(new TableColumn("MaxSurrogateKey", ETypeCode.Int64), writerResult.MaxSurrogateKey),
                    new QueryColumn(new TableColumn("InitializeTime", ETypeCode.DateTime), writerResult.InitializeTime),
                    new QueryColumn(new TableColumn("ScheduledTime", ETypeCode.DateTime), writerResult.ScheduledTime),
                    new QueryColumn(new TableColumn("StartTime", ETypeCode.DateTime), writerResult.StartTime),
                    new QueryColumn(new TableColumn("EndTime", ETypeCode.DateTime), writerResult.EndTime),
                    new QueryColumn(new TableColumn("LastUpdateTime", ETypeCode.DateTime), writerResult.LastUpdateTime),
                    new QueryColumn(new TableColumn("RunStatus", ETypeCode.String), writerResult.RunStatus.ToString()),
                    new QueryColumn(new TableColumn("TriggerMethod", ETypeCode.String), writerResult.TriggerMethod.ToString()),
                    new QueryColumn(new TableColumn("TriggerInfo", ETypeCode.String), writerResult.TriggerInfo.ToString()),
                    new QueryColumn(new TableColumn("Message", ETypeCode.String), writerResult.Message),
                    new QueryColumn(new TableColumn("IsCurrent", ETypeCode.Boolean), isCurrent),
                    new QueryColumn(new TableColumn("IsPrevious", ETypeCode.Boolean), isPrevious),
                    new QueryColumn(new TableColumn("IsPreviousSuccess", ETypeCode.Boolean), isPreviousSuccess)
            };

            var updateFilters = new List<Filter>() { new Filter(new TableColumn("AuditKey", ETypeCode.Int64), Filter.ECompare.IsEqual, writerResult.AuditKey) };

            var updateQuery = new UpdateQuery(AuditTable.TableName, updateColumns, updateFilters);
            var updateResult = await ExecuteUpdate(AuditTable, new List<UpdateQuery>() { updateQuery }, CancellationToken.None);

            return updateResult;
        }


        public virtual async Task<ReturnValue<TransformWriterResult>> GetPreviousResult(long hubKey, long referenceKey, CancellationToken cancellationToken)
        {
            var results = await GetTransformWriterResults(hubKey, new long[] {referenceKey } , null, null, true, false, false, null, -1, 0, null, cancellationToken);
            if (!results.Success)
                return new ReturnValue<TransformWriterResult>(results);

            if (results.Value.Count > 0)
                return new ReturnValue<TransformWriterResult>(true, results.Value[0]);
            else
                return new ReturnValue<TransformWriterResult>(true, null);
        }

        public virtual async Task<ReturnValue<TransformWriterResult>> GetPreviousSuccessResult(long hubKey, long referenceKey, CancellationToken cancellationToken)
        {
            var results = await GetTransformWriterResults(hubKey, new long[] { referenceKey }, null, null, false, true, false, null, -1, 0, null, cancellationToken);
            if (!results.Success)
                return new ReturnValue<TransformWriterResult>(results);

            if (results.Value.Count > 0)
                return new ReturnValue<TransformWriterResult>(true, results.Value[0]);
            else
                return new ReturnValue<TransformWriterResult>(true, null);
        }

        public virtual async Task<ReturnValue<TransformWriterResult>> GetCurrentResult(long hubKey, long referenceKey, CancellationToken cancellationToken)
        {
            var results = await GetTransformWriterResults(hubKey, new long[] { referenceKey }, null, null, false, false, true, null, -1, 0, null, cancellationToken);
            if (!results.Success)
                return new ReturnValue<TransformWriterResult>(results);

            if (results.Value.Count > 0)
                return new ReturnValue<TransformWriterResult>(true, results.Value[0]);
            else
                return new ReturnValue<TransformWriterResult>(true, null);
        }

        public virtual async Task<ReturnValue<List<TransformWriterResult>>> GetPreviousResults(long hubKey, long[] referenceKeys, CancellationToken cancellationToken)
        {
            return await GetTransformWriterResults(hubKey, referenceKeys, null, null, true, false, false, null, -1, 0, null, cancellationToken);
        }

        public virtual async Task<ReturnValue<List<TransformWriterResult>>> GetPreviousSuccessResults(long hubKey, long[] referenceKeys, CancellationToken cancellationToken)
        {
            return await GetTransformWriterResults(hubKey, referenceKeys, null, null, false, true, false, null, -1, 0, null, cancellationToken);
        }

        public virtual async Task<ReturnValue<List<TransformWriterResult>>> GetCurrentResults(long hubKey, long[] referenceKeys, CancellationToken cancellationToken)
        {
            return await GetTransformWriterResults(hubKey, referenceKeys, null, null, false, false, true, null, -1, 0, null, cancellationToken);
        }

        public virtual async Task<ReturnValue<List<TransformWriterResult>>> GetTransformWriterResults(long? hubKey, long[] referenceKeys, long? auditKey, TransformWriterResult.ERunStatus? runStatus, bool previousResult, bool previousSuccessResult, bool currentResult, DateTime? startTime, int rows, int maxMilliseconds, long? parentAuditKey, CancellationToken cancellationToken)
        {
            Transform reader = null;
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();

                reader = GetTransformReader(AuditTable);

                var filters = new List<Filter>();
                if(hubKey != null) filters.Add(new Filter(new TableColumn("HubKey", ETypeCode.Int64), Filter.ECompare.IsEqual, hubKey));
                if (referenceKeys != null && referenceKeys.Length > 0) filters.Add(new Filter(new TableColumn("ReferenceKey", ETypeCode.Int64), Filter.ECompare.IsIn, referenceKeys));
                if (auditKey != null) filters.Add(new Filter(new TableColumn("AuditKey", ETypeCode.Int64), Filter.ECompare.IsEqual, auditKey));
                if (runStatus != null) filters.Add(new Filter(new TableColumn("RunStatus", ETypeCode.String), Filter.ECompare.IsEqual, runStatus.ToString()));
                if (startTime != null) filters.Add(new Filter(new TableColumn("StartTime", ETypeCode.DateTime), Filter.ECompare.GreaterThanEqual, startTime));
                if (currentResult) filters.Add(new Filter(new TableColumn("IsCurrent", ETypeCode.Boolean), Filter.ECompare.IsEqual, true));
                if (previousResult) filters.Add(new Filter(new TableColumn("IsPrevious", ETypeCode.Boolean), Filter.ECompare.IsEqual, true));
                if (previousSuccessResult) filters.Add(new Filter(new TableColumn("IsPreviousSuccess", ETypeCode.Boolean), Filter.ECompare.IsEqual, true));
                if (parentAuditKey != null) filters.Add(new Filter(new TableColumn("ParentAuditKey", ETypeCode.Int64), Filter.ECompare.IsEqual, parentAuditKey));

                var sorts = new List<Sort>() { new Sort(new TableColumn("AuditKey", ETypeCode.Int64), Sort.EDirection.Descending) };
                var query = new SelectQuery() { Filters = filters, Sorts = sorts, Rows = rows };

                //add a sort transform to ensure sort order.
                reader = new TransformSort(reader, sorts);

                ReturnValue returnValue = await reader.Open(0, query);
                if (returnValue.Success == false)
                    return new ReturnValue<List<TransformWriterResult>>(returnValue.Success, returnValue.Message, returnValue.Exception, null);

                var writerResults = new List<TransformWriterResult>();
                int count = 0;

                while ((count < query.Rows || query.Rows == -1) &&
                    cancellationToken.IsCancellationRequested == false &&
                    await reader.ReadAsync(cancellationToken)
                    )
                {
                    TransformWriterResult result = new TransformWriterResult(
                        (long)TryParse(ETypeCode.Int64, reader["HubKey"]).Value,
                        (long)TryParse(ETypeCode.Int64, reader["AuditKey"]).Value,
                        (string)reader["AuditType"],
                        (long)TryParse(ETypeCode.Int64, reader["ReferenceKey"]).Value,
                        (long)TryParse(ETypeCode.Int64, reader["ParentAuditKey"]).Value,
                        (string)reader["ReferenceName"],
                        (long)TryParse(ETypeCode.Int64, reader["SourceTableKey"]).Value,
                        (string)reader["SourceTableName"],
                        (long)TryParse(ETypeCode.Int64, reader["TargetTableKey"]).Value,
                        (string)reader["TargetTableName"], null, null,
                        (TransformWriterResult.ETriggerMethod)Enum.Parse(typeof(TransformWriterResult.ETriggerMethod), (string)reader["TriggerMethod"]),
                        (string)(reader["TriggerInfo"] is DBNull ? null : reader["TriggerInfo"])
                        )
                    {
                        RowsTotal = (long)TryParse(ETypeCode.Int64, reader["RowsTotal"]).Value,
                        RowsCreated = (long)TryParse(ETypeCode.Int64, reader["RowsCreated"]).Value,
                        RowsUpdated = (long)TryParse(ETypeCode.Int64, reader["RowsUpdated"]).Value,
                        RowsDeleted = (long)TryParse(ETypeCode.Int64, reader["RowsDeleted"]).Value,
                        RowsPreserved = (long)TryParse(ETypeCode.Int64, reader["RowsPreserved"]).Value,
                        RowsIgnored = (long)TryParse(ETypeCode.Int64, reader["RowsIgnored"]).Value,
                        RowsRejected = (long)TryParse(ETypeCode.Int64, reader["RowsRejected"]).Value,
                        RowsFiltered = (long)TryParse(ETypeCode.Int64, reader["RowsFiltered"]).Value,
                        RowsSorted = (long)TryParse(ETypeCode.Int64, reader["RowsSorted"]).Value,
                        RowsReadPrimary = (long)TryParse(ETypeCode.Int64, reader["RowsReadPrimary"]).Value,
                        RowsReadReference = (long)TryParse(ETypeCode.Int64, reader["RowsReadReference"]).Value,
                        ReadTicks = (long)TryParse(ETypeCode.Int64, reader["ReadTicks"]).Value,
                        WriteTicks = (long)TryParse(ETypeCode.Int64, reader["WriteTicks"]).Value,
                        ProcessingTicks = (long)TryParse(ETypeCode.Int64, reader["ProcessingTicks"]).Value,
                        MaxIncrementalValue = reader["MaxIncrementalValue"],
                        MaxSurrogateKey = (long)TryParse(ETypeCode.Int64, reader["MaxSurrogateKey"]).Value,
                        InitializeTime = (DateTime)TryParse(ETypeCode.DateTime, reader["InitializeTime"]).Value,
                        ScheduledTime = reader["ScheduledTime"] is DBNull ? (DateTime?)null : (DateTime?)TryParse(ETypeCode.DateTime, reader["ScheduledTime"]).Value,
                        StartTime = reader["StartTime"] is DBNull ? null : (DateTime?)TryParse(ETypeCode.DateTime, reader["StartTime"]).Value,
                        EndTime = reader["EndTime"] is DBNull ? null : (DateTime?)TryParse(ETypeCode.DateTime, reader["EndTime"]).Value,
                        LastUpdateTime = reader["LastUpdateTime"] is DBNull ? (DateTime?)null : (DateTime?)TryParse(ETypeCode.DateTime, reader["LastUpdateTime"]).Value,
                        RunStatus = (TransformWriterResult.ERunStatus)Enum.Parse(typeof(TransformWriterResult.ERunStatus), (string)reader["RunStatus"]),
                        Message = (string)(reader["Message"] is DBNull ? null : reader["Message"])
                    };

                    writerResults.Add(result);

                    count++;
                    if (maxMilliseconds > 0 && watch.ElapsedMilliseconds > maxMilliseconds)
                        break;
                }

                watch.Stop();
                reader.Dispose();

                return new ReturnValue<List<TransformWriterResult>>(true, writerResults);
            }
            catch(Exception ex)
            {
                if(reader != null) reader.Dispose();
                return new ReturnValue<List<TransformWriterResult>>(false, "Get Transform Writer Results failed with error: " + ex.Message, ex);
            }
        }

        #endregion

        public virtual bool IsValidDatabaseName(string name)
        {
            return true;
        }

        public virtual bool IsValidTableName(string name)
        {
            return true;
        }

        public virtual bool IsValidColumnName(string name)
        {
            return true;
        }


        /// <summary>
        /// Gets the next surrogatekey.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="surrogateKeyColumn"></param>
        /// <param name="AuditKey">Included as Azure storage tables use the AuditKey to generate a new surrogate key</param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue<long>> GetIncrementalKey(Table table, TableColumn surrogateKeyColumn, CancellationToken cancelToken)
        {
            try
            {
                var query = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn(surrogateKeyColumn, SelectColumn.EAggregate.Max) },
                    Table = table.TableName
                };

                long surrogateKeyValue;
                var executeResult = await ExecuteScalar(table, query, cancelToken);
                if (!executeResult.Success)
                    return new ReturnValue<long>(executeResult);

                if (executeResult.Value == null || executeResult.Value is DBNull)
                    surrogateKeyValue = 0;
                else
                {
                    var convertResult = DataType.TryParse(ETypeCode.Int64, executeResult.Value);
                    if (!convertResult.Success)
                        return new ReturnValue<long>(convertResult);
                    surrogateKeyValue = (long)convertResult.Value;
                }

                return new ReturnValue<long>(true, surrogateKeyValue);
            }
            catch(Exception ex)
            {
                return new ReturnValue<long>(false, ex.Message, ex);
            }
        }

        /// <summary>
        /// This is called to update any reference tables that need to store the surrogatekey, which is returned by the GetIncrementalKey.  
        /// For sql databases, this does not thing as as select max(key) is called to get key, however nosql tables have no max() function.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="surrogateKeyColumn"></param>
        /// <param name="value"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> UpdateIncrementalKey(Table table, string surrogateKeyColumn, long value, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        /// <summary>
        /// Function runs when a data write comments.  This is used to put headers on csv files.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> DataWriterStart(Table table)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        /// <summary>
        /// Function runs when a data write finishes.  This is used to close file streams.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> DataWriterFinish(Table table)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public async Task<ReturnValue<Table>> GetPreview(Table table, SelectQuery query, int maxMilliseconds, CancellationToken cancellationToken)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            Transform reader = GetTransformReader(table);
            ReturnValue returnValue = await reader.Open(0, query);
            if (returnValue.Success == false)
                return new ReturnValue<Table>(returnValue.Success, returnValue.Message, returnValue.Exception, null);

            reader.SetCacheMethod(Transform.ECacheMethod.OnDemandCache);
			reader.SetEncryptionMethod(Transform.EEncryptionMethod.MaskSecureFields, "");

            int count = 0;
            while ((count < query.Rows || query.Rows == -1 ) &&
                cancellationToken.IsCancellationRequested == false && 
                await reader.ReadAsync(cancellationToken) 
                )
            {
                count++;
                if (maxMilliseconds > 0 && watch.ElapsedMilliseconds > maxMilliseconds)
                    break;
            }

            watch.Stop();
            reader.Dispose();

            return new ReturnValue<Table>(true, reader.CacheTable);
        }

        public async Task<ReturnValue<Table>> GetPreview(Table table, SelectQuery query, int maxMilliseconds, CancellationToken cancellationToken, Transform referenceTransform, List<JoinPair> referenceJoins)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            Transform reader = GetTransformReader(table, referenceTransform);
            reader.JoinPairs = referenceJoins;
            ReturnValue returnValue = await reader.Open(0, query);
            if (returnValue.Success == false)
                return new ReturnValue<Table>(returnValue.Success, returnValue.Message, returnValue.Exception, null);

            reader.SetCacheMethod(Transform.ECacheMethod.OnDemandCache);
				reader.SetEncryptionMethod(Transform.EEncryptionMethod.MaskSecureFields, "");

            int count = 0;
            while ((count < query.Rows || query.Rows == -1) &&
                cancellationToken.IsCancellationRequested == false &&
                await reader.ReadAsync(cancellationToken)
                )
            {
                count++;
                if (maxMilliseconds > 0 && watch.ElapsedMilliseconds > maxMilliseconds)
                    break;
            }

            watch.Stop();
            reader.Dispose();

            return new ReturnValue<Table>(true, reader.CacheTable);
        }


        /// <summary>
        /// This compares the physical table with the table structure to ensure that it can be used.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> CompareTable(Table table)
        {
            var physicalTableResult = await GetSourceTableInfo(table);
            if (!physicalTableResult.Success)
                return physicalTableResult;

            var physicalTable = physicalTableResult.Value;

            foreach(var col in table.Columns)
            {
                var compareCol = physicalTable.Columns.SingleOrDefault(c => c.ColumnName == col.ColumnName);

                if (compareCol == null)
                    return new ReturnValue(false, "The physical table " + table.TableName + " does contain the column " + col.ColumnName + ".  Reimport the table or recreate the table to fix.", null);

            }

            return new ReturnValue(true);

        }

    }
}

