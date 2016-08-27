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

        public enum ECategory
        {
            SqlDatabase = 0,
            NoSqlDatabase = 1,
            File = 2,
            WebService = 3,
            Application = 4
        }

        #endregion

        #region Properties

        public string Name { get; set; }
        public string ServerName { get; set; }
        public bool NtAuthentication { get; set; }
        public string UserName { get; set; } = "";
        public string Password { get; set; } = "";
        public string DefaultDatabase { get; set; }
        public string FileName { get; set; }
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
        public abstract Task<ReturnValue<long>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken);

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
        public abstract Task<ReturnValue> CreateDatabase(string DatabaseName);
        public abstract Task<ReturnValue<List<string>>> GetDatabaseList();
        public abstract Task<ReturnValue<List<string>>> GetTableList();

        /// <summary>
        /// Interrogates the underlying data to get the Table structure.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="Properties"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, string> Properties);

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
                Table auditTable = new Table("DexihAudit");
                AddMandatoryColumns(auditTable, 0).Wait();

                auditTable.Columns.Add(new TableColumn("AuditKey", ETypeCode.Int64, TableColumn.EDeltaType.SurrogateKey));
                auditTable.Columns.Add(new TableColumn("HubKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
                auditTable.Columns.Add(new TableColumn("AuditType", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 20 });
                auditTable.Columns.Add(new TableColumn("ReferenceKey", ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));
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
                auditTable.Columns.Add(new TableColumn("StartTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("EndTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("LastUpdateTime", ETypeCode.DateTime, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });
                auditTable.Columns.Add(new TableColumn("RunStatus", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { MaxLength = 20 });
                auditTable.Columns.Add(new TableColumn("Message", ETypeCode.String, TableColumn.EDeltaType.TrackingField) { AllowDbNull = true });

                return auditTable;
            }
        }

        public virtual async Task<ReturnValue<TransformWriterResult>> InitializeAudit(long subScriptionKey, string auditType, Int64 referenceKey, string referenceName, Int64 sourceTableKey, string sourceTableName, Int64 targetTableKey, string targetTableName)
        {
            var auditTable = AuditTable;

            var tableExistsResult = await TableExists(auditTable);

            long auditKey;
            TransformWriterResult lastResult = null;

            if (!tableExistsResult.Success)
                return new ReturnValue<TransformWriterResult>(tableExistsResult);

            //create the audit table if it does not exist.
            if (tableExistsResult.Value == false)
            {
                //create the table if is doesn't already exist.
                var createAuditResult = await CreateTable(auditTable, false);
                if (!createAuditResult.Success)
                    return new ReturnValue<TransformWriterResult>(createAuditResult);

                auditKey = 1;
            }
            else
            {
                //get the last successful audit result, and add + 1 for new key.
                var lastAuditResult = await GetTransformWriterResults(null, null, null, null, false, null, 1, 0, CancellationToken.None);
                if (!lastAuditResult.Success)
                    return new ReturnValue<TransformWriterResult>(lastAuditResult);
                var lastAudit = lastAuditResult.Value;

                if(lastAudit.Count == 0)
                {
                    auditKey = 1;
                }
                else
                {
                    auditKey = lastAudit[0].AuditKey + 1;

                    //get the last audit result for this reference to collect previous run information
                    lastAuditResult = await GetTransformWriterResults(subScriptionKey, new long[] { referenceKey }, null, TransformWriterResult.ERunStatus.Finished, false, null, 1, 0, CancellationToken.None);
                    if (!lastAuditResult.Success)
                        return new ReturnValue<TransformWriterResult>(lastAuditResult);
                    if(lastAuditResult.Value.Count > 0)
                        lastResult = lastAuditResult.Value[0];
                }
            }

            var writerResult = new TransformWriterResult(subScriptionKey, auditKey, auditType, referenceKey, referenceName, sourceTableKey, sourceTableName, targetTableKey, targetTableName, this, lastResult);

            var queryColumns = new List<QueryColumn>
                {
                    new QueryColumn("AuditKey", ETypeCode.Int64, writerResult.AuditKey),
                    new QueryColumn("HubKey", ETypeCode.Int64,  writerResult.HubKey),
                    new QueryColumn("AuditType", ETypeCode.String,  writerResult.AuditType),
                    new QueryColumn("ReferenceKey", ETypeCode.Int64, writerResult.ReferenceKey),
                    new QueryColumn("ReferenceName", ETypeCode.String, writerResult.ReferenceName),
                    new QueryColumn("SourceTableKey", ETypeCode.Int64, writerResult.SourceTableKey),
                    new QueryColumn("SourceTableName", ETypeCode.String, writerResult.SourceTableName),
                    new QueryColumn("TargetTableKey", ETypeCode.Int64, writerResult.TargetTableKey),
                    new QueryColumn("TargetTableName", ETypeCode.String, writerResult.TargetTableName),
                    new QueryColumn("RowsTotal", ETypeCode.Int64, writerResult.RowsTotal),
                    new QueryColumn("RowsCreated", ETypeCode.Int64, writerResult.RowsCreated),
                    new QueryColumn("RowsUpdated", ETypeCode.Int64, writerResult.RowsUpdated),
                    new QueryColumn("RowsDeleted", ETypeCode.Int64, writerResult.RowsDeleted),
                    new QueryColumn("RowsPreserved", ETypeCode.Int64, writerResult.RowsPreserved),
                    new QueryColumn("RowsIgnored", ETypeCode.Int64, writerResult.RowsIgnored),
                    new QueryColumn("RowsRejected", ETypeCode.Int64, writerResult.RowsRejected),
                    new QueryColumn("RowsSorted", ETypeCode.Int64, writerResult.RowsSorted),
                    new QueryColumn("RowsFiltered", ETypeCode.Int64, writerResult.RowsFiltered),
                    new QueryColumn("RowsReadPrimary", ETypeCode.Int64, writerResult.RowsReadPrimary),
                    new QueryColumn("RowsReadReference", ETypeCode.Int64, writerResult.RowsReadReference),
                    new QueryColumn("ReadTicks", ETypeCode.Int64, writerResult.ReadTicks),
                    new QueryColumn("WriteTicks", ETypeCode.Int64, writerResult.WriteTicks),
                    new QueryColumn("ProcessingTicks", ETypeCode.Int64, writerResult.ProcessingTicks),
                    new QueryColumn("MaxIncrementalValue", ETypeCode.String, writerResult.MaxIncrementalValue),
                    new QueryColumn("MaxSurrogateKey", ETypeCode.String, writerResult.MaxSurrogateKey),
                    new QueryColumn("InitializeTime", ETypeCode.DateTime, writerResult.InitializeTime),
                    new QueryColumn("StartTime", ETypeCode.DateTime, writerResult.StartTime),
                    new QueryColumn("EndTime", ETypeCode.DateTime, writerResult.EndTime),
                    new QueryColumn("LastUpdateTime", ETypeCode.DateTime, writerResult.LastUpdateTime),
                    new QueryColumn("RunStatus", ETypeCode.DateTime, writerResult.RunStatus.ToString()),
                    new QueryColumn("Message", ETypeCode.DateTime, writerResult.Message)
            };

            //add connection specific values
            var rowKeyOrdinal = auditTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzureRowKey);
            if (rowKeyOrdinal > 0)
                queryColumns.Add(new QueryColumn(auditTable.Columns[rowKeyOrdinal].ColumnName, ETypeCode.String, writerResult.AuditKey));

            var partitionKeyOrdinal = auditTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzurePartitionKey);
            if (partitionKeyOrdinal > 0)
                queryColumns.Add(new QueryColumn(auditTable.Columns[partitionKeyOrdinal].ColumnName, ETypeCode.String, "AuditRow"));

            var insertQuery = new InsertQuery(auditTable.TableName, queryColumns);
            var insertResult = await ExecuteInsert(auditTable, new List<InsertQuery>() { insertQuery }, CancellationToken.None);

            if (!insertResult.Success)
                return new ReturnValue<TransformWriterResult>(insertResult);

            return new ReturnValue<TransformWriterResult>(true, writerResult);
        }

        public virtual async Task<ReturnValue> UpdateAudit(TransformWriterResult writerResult)
        {
            var updateColumns = new List<QueryColumn>()
            {
                    new QueryColumn("AuditType", ETypeCode.String,  writerResult.AuditType),
                    new QueryColumn("HubKey", ETypeCode.Int64,  writerResult.HubKey),
                    new QueryColumn("ReferenceKey", ETypeCode.Int64, writerResult.ReferenceKey),
                    new QueryColumn("ReferenceName", ETypeCode.String, writerResult.ReferenceName),
                    new QueryColumn("SourceTableKey", ETypeCode.Int64, writerResult.SourceTableKey),
                    new QueryColumn("SourceTableName", ETypeCode.String, writerResult.SourceTableName),
                    new QueryColumn("TargetTableKey", ETypeCode.Int64, writerResult.TargetTableKey),
                    new QueryColumn("TargetTableName", ETypeCode.String, writerResult.TargetTableName),
                    new QueryColumn("RowsTotal", ETypeCode.Int64, writerResult.RowsTotal),
                    new QueryColumn("RowsCreated", ETypeCode.Int64, writerResult.RowsCreated),
                    new QueryColumn("RowsUpdated", ETypeCode.Int64, writerResult.RowsUpdated),
                    new QueryColumn("RowsDeleted", ETypeCode.Int64, writerResult.RowsDeleted),
                    new QueryColumn("RowsPreserved", ETypeCode.Int64, writerResult.RowsPreserved),
                    new QueryColumn("RowsIgnored", ETypeCode.Int64, writerResult.RowsIgnored),
                    new QueryColumn("RowsRejected", ETypeCode.Int64, writerResult.RowsRejected),
                    new QueryColumn("RowsSorted", ETypeCode.Int64, writerResult.RowsSorted),
                    new QueryColumn("RowsFiltered", ETypeCode.Int64, writerResult.RowsFiltered),
                    new QueryColumn("RowsReadPrimary", ETypeCode.Int64, writerResult.RowsReadPrimary),
                    new QueryColumn("RowsReadReference", ETypeCode.Int64, writerResult.RowsReadReference),
                    new QueryColumn("ReadTicks", ETypeCode.Int64, writerResult.ReadTicks),
                    new QueryColumn("WriteTicks", ETypeCode.Int64, writerResult.WriteTicks),
                    new QueryColumn("ProcessingTicks", ETypeCode.Int64, writerResult.ProcessingTicks),
                    new QueryColumn("MaxIncrementalValue", ETypeCode.String, writerResult.MaxIncrementalValue),
                    new QueryColumn("MaxSurrogateKey", ETypeCode.String, writerResult.MaxSurrogateKey),
                    new QueryColumn("InitializeTime", ETypeCode.DateTime, writerResult.InitializeTime),
                    new QueryColumn("StartTime", ETypeCode.DateTime, writerResult.StartTime),
                    new QueryColumn("EndTime", ETypeCode.DateTime, writerResult.EndTime),
                    new QueryColumn("LastUpdateTime", ETypeCode.DateTime, writerResult.LastUpdateTime),
                    new QueryColumn("RunStatus", ETypeCode.String, writerResult.RunStatus.ToString()),
                    new QueryColumn("Message", ETypeCode.String, writerResult.Message)
            };

            var updateFilters = new List<Filter>() { new Filter("AuditKey", Filter.ECompare.IsEqual, writerResult.AuditKey) };

            var updateQuery = new UpdateQuery(AuditTable.TableName, updateColumns, updateFilters);

            var updateResult = await ExecuteUpdate(AuditTable, new List<UpdateQuery>() { updateQuery }, CancellationToken.None);

            return updateResult;
        }

        public virtual async Task<ReturnValue<List<TransformWriterResult>>> GetTransformWriterResults(long? hubKey, long[] referenceKeys, long? auditKey, TransformWriterResult.ERunStatus? runStatus, bool lastResultOnly, DateTime? startTime, int rows, int maxMilliseconds, CancellationToken cancellationToken)
        {
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();

                Transform reader = GetTransformReader(AuditTable);

                var filters = new List<Filter>();
                if(hubKey != null) filters.Add(new Filter("HubKey", Filter.ECompare.IsEqual, hubKey));
                if (referenceKeys != null && referenceKeys.Length > 0) filters.Add(new Filter("ReferenceKey", Filter.ECompare.IsIn, referenceKeys));
                if (auditKey != null) filters.Add(new Filter("AuditKey", Filter.ECompare.IsEqual, auditKey));
                if (runStatus != null) filters.Add(new Filter("RunStatus", Filter.ECompare.IsEqual, runStatus.ToString()));
                if (startTime != null) filters.Add(new Filter("StartTime", Filter.ECompare.GreaterThanEqual, startTime));

                var sorts = new List<Sort>() { new Sort("AuditKey", Sort.EDirection.Descending) };
                var query = new SelectQuery() { Filters = filters, Sorts = sorts, Rows = rows };

                //add a sort transform to ensure sort order.
                reader = new TransformSort(reader, sorts);

                //if lastResult only, create a group by to get most recent rows.
                if(lastResultOnly)
                {
                    var groupFields = new List<ColumnPair>() { new ColumnPair("AuditKey") };
                    var aggregates = new List<Function>();
                    foreach(var column in AuditTable.Columns)
                    {
                        if (column.ColumnName != "AuditKey")
                            aggregates.Add(StandardFunctions.GetFunctionReference("First", new string[] { column.ColumnName }, column.ColumnName, null));
                    }
                    reader = new TransformGroup(reader, groupFields, aggregates, false);
                }

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
                        (string)reader["ReferenceName"],
                        (long)TryParse(ETypeCode.Int64, reader["SourceTableKey"]).Value,
                        (string)reader["SourceTableName"], 
                        (long)TryParse(ETypeCode.Int64, reader["TargetTableKey"]).Value,
                        (string)reader["TargetTableName"], null, null
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
                        StartTime = (DateTime)TryParse(ETypeCode.DateTime, reader["StartTime"]).Value,
                        EndTime = (DateTime)TryParse(ETypeCode.DateTime, reader["EndTime"]).Value,
                        LastUpdateTime = (DateTime)TryParse(ETypeCode.DateTime, reader["LastUpdateTime"]).Value
                    };
                    await result.SetRunStatus((TransformWriterResult.ERunStatus)Enum.Parse(typeof(TransformWriterResult.ERunStatus), (string)reader["RunStatus"]), (string)(reader["Message"] is DBNull ? null : reader["Message"]));

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
            reader.SetEncryptionMethod(Transform.EEncryptionMethod.BlankSecureFields, "", "<hidden field>");

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
        public async Task<ReturnValue> CompareTable(Table table)
        {
            var physicalTableResult = await GetSourceTableInfo(table.TableName, null);
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

