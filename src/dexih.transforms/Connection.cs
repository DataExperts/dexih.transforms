using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions;
using System.Diagnostics;
using System.Data.Common;
using System.IO;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;
using dexih.transforms.Exceptions;
using dexih.transforms.Poco;
using Dexih.Utils.DataType;

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
        public enum EConnectionCategory
        {
            SqlDatabase, // sql server, mysql, postgre etc.
            NoSqlDatabase, // Azure and others
            DatabaseFile, // coverts Excel, Sqlite where database is a simple file.
            File, // flat files
            WebService,
			Hub
        }

        #endregion

        #region Properties

        public string Name { get; set; }
        public virtual string Server { get; set; }
        public bool UseWindowsAuth { get; set; }
        public string Username { get; set; } = "";
        public string Password { get; set; } = "";
        public string DefaultDatabase { get; set; }
        public string Filename { get; set; }
        public EConnectionState State { get; set; }

        public bool UseConnectionString { get; set; }
        public string ConnectionString { get; set; }

        #endregion

        #region Abstracts

        public ConnectionAttribute Attributes => GetType().GetCustomAttribute<ConnectionAttribute>();
        
//        //Abstract Properties
        public abstract bool CanBulkLoad { get; }
        public abstract bool CanSort { get; }
        public abstract bool CanFilter { get; }
        public abstract bool CanUpdate { get; }
        public abstract bool CanDelete { get; }
        public abstract bool CanAggregate { get; }

        /// <summary>
        /// The connection can directly insert binary (byte[])
        /// </summary>
        public abstract bool CanUseBinary { get; }
        
        /// <summary>
        /// The connection can directly insert arrays
        /// </summary>
        public abstract bool CanUseArray { get; }
        
        /// <summary>
        /// The connection can directly insert json
        /// </summary>
        public abstract bool CanUseJson { get; }
        
        /// <summary>
        /// The connection can directly insert char[]
        /// </summary>
        public abstract bool CanUseCharArray { get; }

        /// <summary>
        /// The connection can directly insert xml structure.
        /// </summary>
        public abstract bool CanUseXml { get; }

        /// <summary>
        /// The connection can directly used Guids
        /// </summary>
        public virtual bool CanUseGuid { get; } = false;
        
        /// <summary>
        /// The connection has native support for boolean.  If false, conversion will be an in 0 - false, 1- true.
        /// </summary>
        public virtual bool CanUseBoolean { get; } = true;

        /// <summary>
        /// Allows for columns which are automatically incremented by the database
        /// </summary>
        public abstract  bool CanUseDbAutoIncrement { get; }

        /// <summary>
        /// The connection can natively insert timespan.
        /// </summary>
        public virtual bool CanUseTimeSpan { get; } = true;

        /// <summary>
        /// The connection can natively accept unsigned numeric types.
        /// </summary>
        public virtual bool CanUseUnsigned { get; } = true;
        
        /// <summary>
        /// The connection can natively accept a signed byte.
        /// </summary>
        public virtual bool CanUseSByte { get; } = true;

        public abstract bool CanUseSql { get; }

        public virtual bool CanUseTransaction { get; } = false;

        public abstract bool DynamicTableCreation { get; } //connection allows any data columns to created dynamically (vs a preset table structure).

        public virtual Task<int> StartTransaction()
        {
            throw new ConnectionException($"The current connection {Name} does not support transactions.");
        }

        public virtual void CommitTransaction(int transactionReference)
        {
            throw new ConnectionException($"The current connection {Name} does not support transactions.");
        }

        public virtual void RollbackTransaction(int transactionReference)
        {
            throw new ConnectionException($"The current connection {Name} does not support transactions.");
        }

        public FilePermissions FilePermissions { get; set; }
        
        //Functions required for managed connection
        public abstract Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken);

        public Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
            return ExecuteUpdate(table, queries, -1, cancellationToken);
        }

        public Task ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancellationToken)
        {
            return ExecuteDelete(table, queries, -1, cancellationToken);
        }

        public Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
            return ExecuteInsert(table, queries, -1, cancellationToken);
        }

        public Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            return TruncateTable(table, -1, cancellationToken);
        }

        
        public abstract Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken);
        public abstract Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="table"></param>
        /// <param name="queries"></param>
        /// <param name="transactionReference"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The last autoincrement value</returns>
        public abstract Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken);

        public abstract Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken);

        /// <summary>
        /// Runs a bulk insert operation for the connection.  
        /// </summary>
        /// <param name="table"></param>
        /// <param name="sourceData"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>ReturnValue with the value = elapsed timer ticks taken to write the record.</returns>
        public abstract Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken);
        public abstract Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken);
        public abstract Transform GetTransformReader(Table table, bool previewMode = false);
        public abstract Task<bool> TableExists(Table table, CancellationToken cancellationToken);

        /// <summary>
        /// If database connection supports direct DbDataReader.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="connection"></param>
        /// <param name="query"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public abstract Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken);

        //Functions required for data point.
        public abstract Task CreateDatabase(string databaseName, CancellationToken cancellationToken);
        public abstract Task<List<string>> GetDatabaseList(CancellationToken cancellationToken);
        public abstract Task<List<Table>> GetTableList(CancellationToken cancellationToken);

        /// <summary>
        /// Interrogates the underlying data to get the Table structure.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public abstract Task<Table> GetSourceTableInfo(Table table, CancellationToken cancellationToken);

        public async Task<Table> GetSourceTableInfo(string TableName, CancellationToken cancellationToken)
        {
            var table = new Table(TableName);
            var initResult = await InitializeTable(table, 0);
            if(initResult == null)
            {
                return null;
            }
            return await GetSourceTableInfo(initResult, cancellationToken);
        }

        /// <summary>
        /// Adds any database specific mandatory columns to the table object and returns the initialized version.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="position"></param>
        /// <returns></returns>
        public abstract Task<Table> InitializeTable(Table table, int position);

        public Stopwatch WriteDataTimer = new Stopwatch();

        #endregion
        
        #region DataType ranges

        public virtual object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            return GetDataTypeMaxValue(typeCode, length);
        }

        public virtual object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
        {
            return GetDataTypeMinValue(typeCode, length);
        }

        
        #endregion

        #region Audit

//        /// <summary>
//        /// Propulates the writerResult with a initial values, and writes the status to the database table.
//        /// </summary>
//        /// <param name="hubKey"></param>
//        /// <param name="auditConnectionKey"></param>
//        /// <param name="auditType"></param>
//        /// <param name="referenceKey"></param>
//        /// <param name="parentAuditKey"></param>
//        /// <param name="referenceName"></param>
//        /// <param name="sourceTableKey"></param>
//        /// <param name="sourceTableName"></param>
//        /// <param name="targetTableKey"></param>
//        /// <param name="targetTableName"></param>
//        /// <param name="triggerMethod"></param>
//        /// <param name="triggerInfo"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public virtual async Task<TransformWriterResult> InitializeAudit(long hubKey, long auditConnectionKey, string auditType, long referenceKey, long parentAuditKey, string referenceName, long sourceTableKey, string sourceTableName, long targetTableKey, string targetTableName, TransformWriterOptions transformWriterOptions, CancellationToken cancellationToken)
//        {
//            var writerResult = new TransformWriterResult();
//            
//            var picoTable = new PocoTable<TransformWriterResult>();
//
//            TransformWriterResult previousResult = null;
//
//            //create the audit table if it does not exist.
//            var tableExistsResult = await picoTable.TableExists(this, cancellationToken);
//            if (tableExistsResult == false)
//            {
//                //create the table if it doesn't already exist.
//                await picoTable.CreateTable(this, false, cancellationToken);
//            }
//            else
//            {
//                //get the last audit result for this reference to collect previous run information
//                previousResult = await GetPreviousResult(hubKey, auditConnectionKey, referenceKey, CancellationToken.None);
//            }
//
//            writerResult.SetProperties(hubKey, auditConnectionKey, 0, auditType, referenceKey, parentAuditKey, referenceName, sourceTableKey, sourceTableName, targetTableKey, targetTableName, this, previousResult, transformWriterOptions);
//            await picoTable.ExecuteInsert(this, writerResult, cancellationToken);
//
//            return writerResult;
//        }

        public async Task<TransformWriterResult> InitializeAudit(CancellationToken cancellationToken)
        {
            var writerResult = new TransformWriterResult(this);
            await InitializeAudit(writerResult, cancellationToken);
            return writerResult;
        }
        
        public async Task InitializeAudit(TransformWriterResult writerResult, CancellationToken cancellationToken)
        {
            var pocoTable = new PocoTable<TransformWriterResult>();
            if(!await pocoTable.TableExists(this, cancellationToken))
            {
                await pocoTable.CreateTable(this, false, cancellationToken);
                
            }
            else
            {
                var previousSuccess = await GetPreviousSuccessResult(writerResult.HubKey, writerResult.AuditConnectionKey,
                    writerResult.ReferenceKey, cancellationToken);
            
                writerResult.LastRowTotal = previousSuccess?.RowsTotal ?? 0;
                writerResult.LastMaxIncrementalValue = previousSuccess?.MaxIncrementalValue;
            }

            await pocoTable.ExecuteInsert(this, writerResult, cancellationToken);
        }

        public virtual async Task UpdateAudit(TransformWriterResult writerResult, CancellationToken cancellationToken )
        {
            var picoTable = new PocoTable<TransformWriterResult>();

            writerResult.IsCurrent = true;
            writerResult.IsPrevious = false;
            writerResult.IsPreviousSuccess = false;

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

                var updateIsLatest = new UpdateQuery(picoTable.Table.Name, updateLatestColumn, updateLatestFilters);
                await ExecuteUpdate(picoTable.Table, new List<UpdateQuery>() { updateIsLatest }, CancellationToken.None);

                writerResult.IsPreviousSuccess = true;
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

                var updateIsLatest = new UpdateQuery(picoTable.Table.Name, updateLatestColumn, updateLatestFilters);
                await ExecuteUpdate(picoTable.Table, new List<UpdateQuery>() { updateIsLatest }, CancellationToken.None);

                writerResult.IsCurrent = false;
                writerResult.IsPrevious = true;
            }

            await picoTable.ExecuteUpdate(this, writerResult, cancellationToken);

        }


        public virtual async Task<TransformWriterResult> GetPreviousResult(long hubKey, long connectionKey, long referenceKey, CancellationToken cancellationToken)
        {
            var results = await GetTransformWriterResults(hubKey, connectionKey, new long[] { referenceKey }, null, null, null, true, false, false, null, -1, null, false, cancellationToken);
            if (results == null || results.Count == 0)
            {
                return null;
            }
            return results[0];
        }

        public virtual async Task<TransformWriterResult> GetPreviousSuccessResult(long hubKey, long connectionKey, long referenceKey, CancellationToken cancellationToken)
        {
            var results = await GetTransformWriterResults(hubKey, connectionKey, new long[] { referenceKey }, null, null, null, false, true, false, null, -1, null, false, cancellationToken);
            if (results == null || results.Count == 0)
            {
                return null;
            }
            return results[0];
        }

        public virtual async Task<TransformWriterResult> GetCurrentResult(long hubKey, long connectionKey, long referenceKey, CancellationToken cancellationToken)
        {
            var results = await GetTransformWriterResults(hubKey, connectionKey, new long[] { referenceKey }, null, null, null, false, false, true, null, -1, null, false, cancellationToken);
            if (results == null || results.Count == 0)
            {
                return null;
            }
            return results[0];
        }

        public virtual async Task<List<TransformWriterResult>> GetPreviousResults(long hubKey, long connectionKey, long[] referenceKeys, CancellationToken cancellationToken)
        {
            return await GetTransformWriterResults(hubKey, connectionKey, referenceKeys, null, null, null, true, false, false, null, -1, null, false, cancellationToken);
        }

        public virtual async Task<List<TransformWriterResult>> GetPreviousSuccessResults(long hubKey, long connectionKey, long[] referenceKeys, CancellationToken cancellationToken)
        {
            return await GetTransformWriterResults(hubKey, connectionKey, referenceKeys, null, null, null, false, true, false, null, -1, null, false, cancellationToken);
        }

        public virtual async Task<List<TransformWriterResult>> GetCurrentResults(long hubKey, long connectionKey, long[] referenceKeys, CancellationToken cancellationToken)
        {
            return await GetTransformWriterResults(hubKey, connectionKey, referenceKeys, null, null, null, false, false, true, null, -1, null, false, cancellationToken);
        }

        public virtual async Task<List<TransformWriterResult>> GetTransformWriterResults(long? hubKey, long connectionKey, long[] referenceKeys, string auditType, long? auditKey, TransformWriterResult.ERunStatus? runStatus, bool previousResult, bool previousSuccessResult, bool currentResult, DateTime? startTime, int rows, long? parentAuditKey, bool childItems, CancellationToken cancellationToken)
        {
            Transform reader = null;
            var watch = new Stopwatch();
            watch.Start();

            var picoTable = new PocoTable<TransformWriterResult>();
            reader = GetTransformReader(picoTable.Table);

            var filters = new List<Filter>();
            if(hubKey != null) filters.Add(new Filter(new TableColumn("HubKey", ETypeCode.Int64), Filter.ECompare.IsEqual, hubKey));
            if (referenceKeys != null && referenceKeys.Length > 0) filters.Add(new Filter(new TableColumn("ReferenceKey", ETypeCode.Int64), Filter.ECompare.IsIn, referenceKeys));
            if (auditType != null) filters.Add(new Filter(new TableColumn("AuditType", ETypeCode.String), Filter.ECompare.IsEqual, auditType));
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

            var returnValue = await reader.Open(0, query, cancellationToken);
            if (!returnValue)
            {
                throw new ConnectionException($"Failed to get the transform writer results on table {picoTable.Table} at {Name}.");
            }

            var pocoReader = new PocoLoader<TransformWriterResult>();
            var writerResults = await pocoReader.ToListAsync(reader, rows, cancellationToken);

            foreach(var result in writerResults)
            {
                result.AuditConnectionKey = connectionKey;
                
                if(childItems)
                {
                    result.ChildResults = await GetTransformWriterResults(hubKey, connectionKey, null, null, null, null, previousResult, previousSuccessResult, currentResult, null, rows, result.AuditKey, false, cancellationToken);
                }

                if (cancellationToken.IsCancellationRequested)
                    break;
            }

            watch.Stop();
            reader.Dispose();

            return writerResults;
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

        public async Task<Transform> GetTransformReader(string tableName, CancellationToken cancellationToken = default)
        {
            var table = await GetSourceTableInfo(tableName, cancellationToken);
            var transform = GetTransformReader(table, true);
            return transform;
        }


        /// <summary>
        /// Gets the next surrogatekey.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="surrogateKeyColumn"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual async Task<long> GetNextKey(Table table, TableColumn surrogateKeyColumn, CancellationToken cancellationToken = default)
        {
            if(DynamicTableCreation)
            {
                return 0;
            }

            var query = new SelectQuery()
            {
                Columns = new List<SelectColumn> { new SelectColumn(surrogateKeyColumn, SelectColumn.EAggregate.Max) },
                Table = table.Name
            };

            long surrogateKeyValue;
            var executeResult = await ExecuteScalar(table, query, cancellationToken);

            if (executeResult == null || executeResult is DBNull)
                surrogateKeyValue = 0;
            else
            {
                try
                {
                    var convertResult = Operations.Parse<long>(executeResult);
                    surrogateKeyValue = convertResult;
                } 
                catch(Exception ex)
                {
                    throw new ConnectionException($"Failed to get the surrogate key from {table.Name} on {Name} as the value is not a valid numeric.  {ex.Message}", ex);
                }
            }

            return surrogateKeyValue;
        }

        /// <summary>
        /// This is called to update any reference tables that need to store the surrogatekey, which is returned by the GetIncrementalKey.  
        /// For sql databases, this does not thing as as select max(key) is called to get key, however nosql tables have no max() function.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="surrogateKeyColumn"></param>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual Task UpdateIncrementalKey(Table table, string surrogateKeyColumn, object value, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Function runs when a data write comments.  This is used to put headers on csv files.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual Task DataWriterStart(Table table)
        {
            return Task.CompletedTask;
        }

        public object ConvertForWrite(TableColumn column, object value)
        {
            return ConvertForWrite(column.Name, column.DataType, column.Rank, column.AllowDbNull, value);
        }

        /// <summary>
        /// Converts a value to a datatype that can be written to the data source.
        /// This includes transforming json/xml/arrays into strings where necessary.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="typeCode"></param>
        /// <param name="rank"></param>
        /// <param name="allowDbNull"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public virtual object ConvertForWrite(string name, ETypeCode typeCode, int rank, bool allowDbNull, object value)
        {
            if (value == null || value == DBNull.Value)
            {
                if (allowDbNull)
                {
                    return DBNull.Value;
                }
                else
                {
                    throw new ConnectionException($"The {name} item has a value null which could not be inserted as the column does not allow nulls.");
                }
            }

            if (rank > 0 && !CanUseArray)
            {
                return Operations.Parse<string>(value);
            }
            
            switch (typeCode)
            {
                case ETypeCode.Binary:
                    if(!CanUseBinary) return Operations.Parse<string>(value);
                    goto default;
                case ETypeCode.Boolean:
                    if (!CanUseBoolean)
                    {
                        if (value is bool b)
                        {
                            return b ? 1 : 0;
                        }

                        var b1 = Operations.Parse<bool>(value);
                        return b1 ? 1 : 0;
                    }
                    goto default;
                case ETypeCode.Json:
                    if(!CanUseJson) return Operations.Parse<string>(value);
                    goto default;
                case ETypeCode.Xml:
                    if(!CanUseXml) return Operations.Parse<string>(value);
                    goto default;
                case ETypeCode.CharArray:
                    if(!CanUseCharArray) return Operations.Parse<string>(value);
                    goto default;
                case ETypeCode.Guid:
                    if(!CanUseGuid) return Operations.Parse<string>(value);
                    goto default;
                case ETypeCode.UInt16:
                    if (!CanUseUnsigned) return Operations.Parse<int>(value);
                    goto default;
                case ETypeCode.UInt32:
                    if (!CanUseUnsigned) return Operations.Parse<long>(value);
                    goto default;
                case ETypeCode.UInt64:
                    if (!CanUseUnsigned) return Operations.Parse<long>(value);
                    goto default;
                case ETypeCode.SByte:
                    if (!CanUseSByte) return Operations.Parse<short>(value);
                    goto default;
                case ETypeCode.Time:
                    if (!CanUseTimeSpan) return Operations.Parse<string>(value);
                    goto default;
                case ETypeCode.Node:
                    if (value is DbDataReader reader)
                    {
                        var streamJson = new StreamJson("json", reader);

                        // convert stream to string
                        var streamReader = new StreamReader(streamJson);
                        return streamReader.ReadToEnd();
                    }

                    return null;
                default:
                    return Operations.Parse(typeCode, rank, value);
            }
        }

        /// <summary>
        /// Converts a value to the required data type after being read from the data source.
        /// This includes transforming strings containing arrays/json/xml into native structures.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public virtual object ConvertForRead(TableColumn column, object value)
        {
            if ((column.Rank > 0 && !CanUseArray) ||
                (column.DataType == ETypeCode.CharArray && !CanUseCharArray) ||
                (column.DataType == ETypeCode.Binary && !CanUseBinary) ||
                (column.DataType == ETypeCode.Json && !CanUseJson) ||
                (column.DataType == ETypeCode.Xml && !CanUseXml) ||
                column.DataType == ETypeCode.Guid) // GUID's get parameterized as binary.  So need to explicitly convert to string.
            {
                return Operations.Parse(column.DataType, value);
            }
            
            return value;

        }

        /// <summary>
        /// Function runs when a data write finishes.  This is used to close file streams.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual Task DataWriterFinish(Table table)
        {
            return Task.CompletedTask;
        }

        public async Task<Table> GetPreview(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
            try
            {
                var watch = new Stopwatch();
                watch.Start();

                var rows = query?.Rows ?? -1;

                using (var reader = GetTransformReader(table, true))
                {
                    var returnValue = await reader.Open(0, query, cancellationToken);
                    if (!returnValue)
                    {
                        throw new ConnectionException($"The reader failed to open for table {table.Name} on {Name}");
                    }

                    reader.SetCacheMethod(Transform.ECacheMethod.OnDemandCache);
                    reader.SetEncryptionMethod(Transform.EEncryptionMethod.MaskSecureFields, "");

                    var count = 0;
                    while (
                        (count < rows || rows < 0) &&
                           cancellationToken.IsCancellationRequested == false &&
                           await reader.ReadAsync(cancellationToken)
                    )
                    {
                        count++;
                    }

                    watch.Stop();
                    return reader.CacheTable;
                }

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The preview failed to for table {table.Name} on {Name}", ex);
            }
        }

        /// <summary>
        /// Returns a hashset table containing all the values in a table column.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ConnectionException"></exception>
        public async Task<HashSet<object>> GetColumnValues(Table table, TableColumn column, CancellationToken cancellationToken)
        {
            var query = new SelectQuery()
            {
                Columns = new List<SelectColumn>() {new SelectColumn(column)},
                Groups = new List<TableColumn>() {column},
            };
            
            using (var reader = GetTransformReader(table, true))
            {
                var returnValue = await reader.Open(0, query, cancellationToken);
                
                if (!returnValue)
                {
                    throw new ConnectionException($"The reader failed to open for table {table.Name} on {Name}");
                }

                var values = new HashSet<object>();
                while (
                    cancellationToken.IsCancellationRequested == false &&
                    await reader.ReadAsync(cancellationToken)
                )
                {
                    var value = reader[0];
                    if(!values.Contains(value)) values.Add(value);
                }

                return values;
            }
        }


        /// <summary>
        /// This compares the physical table with the table structure to ensure that it can be used.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>true if table matches, throw an exception is it does not match</returns>
        public virtual async Task<bool> CompareTable(Table table, CancellationToken cancellationToken = default)
        {
            var physicalTable = await GetSourceTableInfo(table, cancellationToken);
            if (physicalTable == null)
            {
                throw new ConnectionException($"The compare table failed to get the source table information for table {table.Name} at {Name}.");
            }

            foreach(var col in table.Columns)
            {
                var compareCol = physicalTable.Columns.SingleOrDefault(c => c.Name == col.Name);

                if (compareCol == null)
                {
                    throw new ConnectionException($"The source table {table.Name} does not contain the column {col.Name}.  Reimport the table or recreate the table with the missing column to fix.");
                }
            }

            return true;
        }

    }
}

