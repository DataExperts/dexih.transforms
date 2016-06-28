using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using dexih.functions;
using System.IO;
using System.Net;
using System.Data.Common;
using dexih.transforms;
using static dexih.functions.DataType;
using System.Text.RegularExpressions;
using System.Threading;

namespace dexih.connections
{
    public class ConnectionAzure : Connection
    {

        public override string ServerHelp => "Server Name";
        public override string DefaultDatabaseHelp => "Database";
        public override bool AllowNtAuth => false;
        public override bool AllowUserPass => true;
        public override string DatabaseTypeName => "Azure Storage Tables";
        public override ECategory DatabaseCategory => ECategory.NoSqlDatabase;


        public override bool CanBulkLoad => true;
        public override bool CanSort => false;

        public override bool CanFilter => true;
        public override bool CanAggregate => false;


        public override bool IsValidDatabaseName(string name)
        {
            return Regex.IsMatch(name, "^[A-Za-z][A-Za-z0-9]{2,62}$");
        }

        public override bool IsValidTableName(string name)
        {
            return Regex.IsMatch(name, "^[A-Za-z][A-Za-z0-9]{2,62}$");
        }

        public override bool IsValidColumnName(string name)
        {
            return Regex.IsMatch(name, "^(?:((?!\\d)\\w+(?:\\.(?!\\d)\\w+)*)\\.)?((?!\\d)\\w+)$");
        }


        public override async Task<ReturnValue<int>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                string targetTableName = table.TableName;

                List<Task> tasks = new List<Task>();

                //create buffers of data and write in parallel.
                int bufferSize = 0;
                List<object[]> buffer = new List<object[]>();

                var sk = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);

                while (reader.Read())
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<int>(false, "Insert rows cancelled.", null);

                    if (bufferSize > 99)
                    {
                        tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancelToken));
                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue<int>(false, "Update rows cancelled.", null);

                        bufferSize = 0;
                        buffer = new List<object[]>();
                    }

                    object[] row = new object[table.Columns.Count];

                    for (int i = 0; i < table.Columns.Count; i++)
                    {
                        if(i< reader.FieldCount)
                            row[i] = reader[i];
                        else
                        {
                            //if the reader does not have the azure fields, then just add defaults.
                            if (table.Columns[i].DeltaType == TableColumn.EDeltaType.AzurePartitionKey)
                                row[i] = "default";
                            else if (table.Columns[i].DeltaType == TableColumn.EDeltaType.AzureRowKey)
                            {
                                if (sk != null)
                                    row[i] = reader[sk.ColumnName];
                                else
                                    row[i] = Guid.NewGuid().ToString();
                            }
                        }
                    }

                    buffer.Add(row);
                    bufferSize++;
                }
                tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancelToken));
                if (cancelToken.IsCancellationRequested)
                    return new ReturnValue<int>(false, "Update rows cancelled.", null);

                bufferSize = 0;
                buffer = new List<object[]>();

                await Task.WhenAll(tasks);

                return new ReturnValue<int>(true, 0);
            }
            catch(StorageException ex)
            {
                string message = "Error writing to Azure Storage table: " + table.TableName + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".";
                return new ReturnValue<int>(false, message, ex);
            }
            catch (Exception ex)
            {
                string message = "Error writing to Azure Storage table: " + table.TableName + ".  Error Message: " + ex.Message;
                return new ReturnValue<int>(false, message, ex);
            }
        }

        public async Task WriteDataBuffer(Table table, List<object[]> buffer, string targetTableName, CancellationToken cancelToken)
        {
            CloudTableClient connection = GetCloudTableClient();
            CloudTable cloudTable = connection.GetTableReference(targetTableName);

            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            int partitionKey = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzurePartitionKey);
            int rowKey = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzureRowKey);
            int surrogateKey = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SurrogateKey);

            foreach (object[] row in buffer)
            {
                Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
                for (int i = 0; i < table.Columns.Count; i++)
                    if (table.Columns[i].DeltaType != TableColumn.EDeltaType.AzureRowKey && table.Columns[i].DeltaType != TableColumn.EDeltaType.AzurePartitionKey && table.Columns[i].DeltaType != TableColumn.EDeltaType.TimeStamp)
                    {
                        object value = row[i];
                        if (value == DBNull.Value) value = null;
                        properties.Add(table.Columns[i].ColumnName, NewEntityProperty(table.Columns[i].DataType, value));
                    }

                var partionKeyValue = partitionKey >= 0 ? row[partitionKey] : "default";
                var rowKeyValue = rowKey >= 0 ? row[rowKey] : surrogateKey >= 0 ? row[surrogateKey] : Guid.NewGuid().ToString();
                DynamicTableEntity entity = new DynamicTableEntity(partionKeyValue.ToString(), rowKeyValue.ToString(), "*", properties);

                batchOperation.Insert(entity);
            }
            await cloudTable.ExecuteBatchAsync(batchOperation, null, null, cancelToken);
        }



        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="dropTable"></param>
        /// <returns></returns>
        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable = false)
        {
            try
            {
                if (!IsValidTableName(table.TableName))
                    return new ReturnValue(false, "The table " + table.TableName + " could not be created as it does not meet Azure table naming standards.", null);

                foreach(var col in table.Columns)
                {
                    if (!IsValidColumnName(col.ColumnName))
                        return new ReturnValue(false, "The table " + table.TableName + " could not be created as the column + " + col.ColumnName + " does not meet Azure table naming standards.", null);
                }

                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.TableName);
                if (dropTable)
                    await cTable.DeleteIfExistsAsync();

                //bool result = await Retry.Do(async () => await cTable.CreateIfNotExistsAsync(), TimeSpan.FromSeconds(10), 6);

                bool isCreated = false;
                for (int i = 0; i< 10; i++)
                {
                    try
                    {
                        isCreated = await GetCloudTableClient().GetTableReference(table.TableName).CreateIfNotExistsAsync();
                        if (isCreated)
                            break;
                        await Task.Delay(5000);
                    }
                    catch
                    {
                        await Task.Delay(5000);
                        continue;
                    }
                }


                return new ReturnValue(isCreated);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when creating an azure table.  This could be due to the previous Azure table still being deleted due to delayed garbage collection.  The message is: " + ex.Message, ex);
            }
        }

        public CloudTableClient GetCloudTableClient()
        {
            CloudStorageAccount storageAccount;

            if(UseConnectionString)
                storageAccount = CloudStorageAccount.Parse(ConnectionString);
            // Retrieve the storage account from the connection string.
            else if (string.IsNullOrEmpty(UserName)) //no username, then use the development settings.
                storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
            else
                storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=" + UserName + ";AccountKey=" + Password + ";TableEndpoint=" + ServerName );

            //ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(storageAccount.TableEndpoint);
            //tableServicePoint.UseNagleAlgorithm = false;
            //tableServicePoint.ConnectionLimit = 10000;

            // Create the table client.
            return storageAccount.CreateCloudTableClient();
        }

        public override async Task<ReturnValue> CreateDatabase(string DatabaseName)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            List<string> list = await Task.Run(() => new List<string> {"Default"} );
            return new ReturnValue<List<string>>(true, list);
        }

        public override async Task<ReturnValue<List<string>>> GetTableList()
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                TableContinuationToken continuationToken = null;
                List<string> list = new List<string>();
                do
                {
                    var table = await connection.ListTablesSegmentedAsync(continuationToken);
                    continuationToken = table.ContinuationToken;
                    list.AddRange(table.Results.Select(c=>c.Name));

                } while (continuationToken != null);

                return new ReturnValue<List<string>>(true, list);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error was encountered when getting a list of Azure tables: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties = null)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();


                //The new datatable that will contain the table schema
                Table table = new Table(tableName);
                table.LogicalName = tableName;
                table.Description = "";

                CloudTable cloudTable = connection.GetTableReference(tableName);
                var query = new TableQuery().Take(1);

                TableContinuationToken continuationToken = null;
                List<DynamicTableEntity> list = new List<DynamicTableEntity>();
                do
                {
                    var result = await cloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                    continuationToken = result.ContinuationToken;
                    list.AddRange(result.Results);

                } while (continuationToken != null);

                if (list.Count > 0)
                {
                    var dynamicTableEntity = list[0];
                    foreach (var property in dynamicTableEntity.Properties)
                    {
                        //add the basic properties                            
                        TableColumn col = new TableColumn()
                        {
                            ColumnName = property.Key,
                            LogicalName = property.Key,
                            IsInput = false,
                            ColumnGetType = property.Value.GetType(),
                            Description = "",
                            AllowDbNull = true,
                            IsUnique = false
                        };

                        table.Columns.Add(col);
                    }
                }
                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error was encountered when getting azure table information: " + ex.Message, ex, null);
            }
        }

 
        public string ConvertOperator(Filter.ECompare Operator)
        {
            switch( Operator)
            {
                case Filter.ECompare.IsEqual: 
                    return "eq";
                case Filter.ECompare.GreaterThan:
                    return "gt";
                case Filter.ECompare.GreaterThanEqual:
                    return "ge";
                case Filter.ECompare.LessThan:
                    return "lt";
                case Filter.ECompare.LessThanEqual:
                    return "le";
                case Filter.ECompare.NotEqual:
                    return "ne";
                default:
                    throw new Exception("ConvertOperator failed");
            }
        }

        public string BuildFilterString(List<Filter> filters)
        {
            if (filters == null || filters.Count == 0)
                return "";
            else
            {
                string combinedFilterString = "";
                foreach (var filter in filters)
                {
                    string filterString;
                    switch (filter.CompareDataType)
                    {
                        case ETypeCode.String:
                        case ETypeCode.Guid:
                        case ETypeCode.Unknown:
                            filterString = TableQuery.GenerateFilterCondition(filter.Column1, ConvertOperator(filter.Operator), (string)filter.Value2);
                            break;
                        case ETypeCode.Boolean:
                            filterString = TableQuery.GenerateFilterConditionForBool(filter.Column1, ConvertOperator(filter.Operator), (bool) filter.Value2);
                            break;
                        case ETypeCode.Int16:
                        case ETypeCode.Int32:
                        case ETypeCode.UInt16:
                        case ETypeCode.UInt32:
                            filterString = TableQuery.GenerateFilterConditionForInt(filter.Column1, ConvertOperator(filter.Operator), (int)filter.Value2);
                            break;
                        case ETypeCode.UInt64:
                        case ETypeCode.Int64:
                            filterString = TableQuery.GenerateFilterConditionForLong(filter.Column1, ConvertOperator(filter.Operator), (long) filter.Value2);
                            break;
                        case ETypeCode.DateTime:
                            filterString = TableQuery.GenerateFilterConditionForDate(filter.Column1, ConvertOperator(filter.Operator), (DateTime)filter.Value2);
                            break;
                        case ETypeCode.Time:
                            filterString = TableQuery.GenerateFilterCondition(filter.Column1, ConvertOperator(filter.Operator), filter.Value2.ToString());
                            break;
                        case ETypeCode.Double:
                        case ETypeCode.Decimal:
                            filterString = TableQuery.GenerateFilterConditionForDouble(filter.Column1, ConvertOperator(filter.Operator), (double)filter.Value2);
                            break;
                        default:
                            throw new Exception("The data type: " + filter.CompareDataType.ToString() + " is not supported by Azure table storage.");
                    }
                    if (combinedFilterString == "")
                        combinedFilterString = filterString;
                    else if(filterString != "")
                        combinedFilterString = TableQuery.CombineFilters(combinedFilterString, filter.AndOr.ToString().ToLower(), filterString);
                }
                return combinedFilterString;
            }
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                bool result;

                CloudTable cTable = connection.GetTableReference(table.TableName);
                await cTable.DeleteIfExistsAsync(null, null,cancelToken);
                result = Retry.Do(() => cTable.CreateIfNotExistsAsync(null, null,cancelToken).Result, TimeSpan.FromSeconds(10), 6);

                return new ReturnValue(result);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The truncate table failed.  This may be due to Azure garbage collection processes being too slow.  The error was: " + ex.Message, ex);
            }
        }


        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            await Task.Run(() =>
            {
                if (table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.AzurePartitionKey).Count() == 0)
                {
                    //partion key uses the AuditKey which allows bulk load, and can be used as an incremental checker.
                    table.Columns.Add(new TableColumn()
                    {
                        ColumnName = "partitionKey",
                        DataType = ETypeCode.String,
                        MaxLength = 0,
                        Precision = 0,
                        AllowDbNull = false,
                        LogicalName = table.TableName + " parition key.",
                        Description = "The Azure partition key and UpdateAuditKey for this table.",
                        IsUnique = true,
                        DeltaType = TableColumn.EDeltaType.AzurePartitionKey,
                        IsIncrementalUpdate = true,
                        IsMandatory = true
                    });
                }

                if (table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.AzureRowKey).Count() == 0)
                {
                    //add the special columns for managed tables.
                    table.Columns.Add(new TableColumn()
                    {
                        ColumnName = "rowKey",
                        DataType = ETypeCode.String,
                        MaxLength = 0,
                        Precision = 0,
                        AllowDbNull = false,
                        LogicalName = table.TableName + " surrogate key",
                        Description = "The azure rowKey and the natural key for this table.",
                        IsUnique = true,
                        DeltaType = TableColumn.EDeltaType.AzureRowKey,
                        IsMandatory = true
                    });
                }

                if (table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TimeStamp).Count() == 0)
                {

                    //add the special columns for managed tables.
                    table.Columns.Add(new TableColumn()
                    {
                        ColumnName = "Timestamp",
                        DataType = ETypeCode.DateTime,
                        MaxLength = 0,
                        Precision = 0,
                        AllowDbNull = false,
                        LogicalName = table.TableName + " timestamp.",
                        Description = "The Azure Timestamp for the managed table.",
                        IsUnique = true,
                        DeltaType = TableColumn.EDeltaType.TimeStamp,
                        IsMandatory = true
                    });
                }
            });

            return new ReturnValue(true, "", null);
        }

        private EntityProperty NewEntityProperty(ETypeCode typeCode, object value)
        {
            var returnValue = DataType.TryParse(typeCode, value);
            if (!returnValue.Success)
                throw new Exception(returnValue.Message);

            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return new EntityProperty((Byte)returnValue.Value);
                case ETypeCode.SByte:
                    return new EntityProperty((SByte)returnValue.Value);
                case ETypeCode.UInt16:
                    return new EntityProperty((UInt16)returnValue.Value);
                case ETypeCode.UInt32:
                    return new EntityProperty((UInt32)returnValue.Value);
                case ETypeCode.UInt64:
                    return new EntityProperty((UInt64)returnValue.Value);
                case ETypeCode.Int16:
                    return new EntityProperty((Int16)returnValue.Value);
                case ETypeCode.Int32:
                    return new EntityProperty((Int32)returnValue.Value);
                case ETypeCode.Int64:
                    return new EntityProperty((Int64)returnValue.Value);
                case ETypeCode.Double:
                    return new EntityProperty((Double)returnValue.Value);
                case ETypeCode.Single:
                    return new EntityProperty((Single)returnValue.Value);
                case ETypeCode.String:
                    return new EntityProperty((String)returnValue.Value);
                case ETypeCode.Boolean:
                    return new EntityProperty((Boolean)returnValue.Value);
                case ETypeCode.DateTime:
                    return new EntityProperty((DateTime)returnValue.Value);
                case ETypeCode.Guid:
                    return new EntityProperty((Guid)returnValue.Value);
                case ETypeCode.Decimal:
                case ETypeCode.Unknown:
                    return new EntityProperty(value.ToString()); //decimal not supported, so convert to string
                case ETypeCode.Time:
                    return new EntityProperty(((TimeSpan)value).Ticks); //timespan not supported, so use ticks.
                default:
                    throw new Exception("Cannot create new azure entity as the data type: " + typeCode.ToString() + " is not suppored.");
            }

        }

        public object ConvertEntityProperty(ETypeCode typeCode, object value)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                case ETypeCode.SByte:
                case ETypeCode.UInt16:
                case ETypeCode.UInt32:
                case ETypeCode.UInt64:
                case ETypeCode.Int16:
                case ETypeCode.Int32:
                case ETypeCode.Int64:
                case ETypeCode.Double:
                case ETypeCode.Single:
                case ETypeCode.String:
                case ETypeCode.Boolean:
                case ETypeCode.DateTime:
                case ETypeCode.Guid:
                    return value;
                case ETypeCode.Decimal:
                    return Convert.ToDecimal(value);
                case ETypeCode.Time:
                    return new TimeSpan((long)value);
                case ETypeCode.Unknown:
                    return value.ToString();
                default:
                    return value;
            }
        }

 
        public override async Task<ReturnValue<int>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.TableName);

                int rowsInserted = 0;
                int rowcount = 0;

                List<Task> batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                TableBatchOperation batchOperation = new TableBatchOperation();

                var partitionKey = table.GetDeltaColumn(TableColumn.EDeltaType.AzurePartitionKey);
                var rowKey = table.GetDeltaColumn(TableColumn.EDeltaType.AzureRowKey);
                var timeStamp = table.GetDeltaColumn(TableColumn.EDeltaType.TimeStamp);

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<int>(false, "Insert rows cancelled.", null);

                    Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
                    foreach (var field in query.InsertColumns)
                        if (field.Column != "rowKey" && field.Column != "partitionKey" && field.Column != "Timestamp")
                            properties.Add(field.Column, NewEntityProperty(table[field.Column].DataType, field.Value));

                    string partitionKeyValue = null;
                    if (partitionKey != null)
                        partitionKeyValue = query.InsertColumns.SingleOrDefault(c => c.Column == partitionKey.ColumnName)?.Value.ToString();

                    if (string.IsNullOrEmpty(partitionKeyValue)) partitionKeyValue = "default";

                    string rowKeyValue = null;
                    if(rowKey != null)
                        rowKeyValue = query.InsertColumns.SingleOrDefault(c => c.Column ==partitionKey.ColumnName)?.Value.ToString();

                    if (string.IsNullOrEmpty(rowKeyValue))
                    {
                        var sk = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey)?.ColumnName;

                        if(sk == null)
                            rowKeyValue = Guid.NewGuid().ToString();
                        else
                            rowKeyValue = query.InsertColumns.Single(c => c.Column == sk).Value.ToString();
                    }


                    DynamicTableEntity entity = new DynamicTableEntity(partitionKeyValue, rowKeyValue, "*", properties);

                    batchOperation.Insert(entity);

                    rowcount++;
                    rowsInserted++;

                    if (rowcount > 99)
                    {
                        rowcount = 0;
                        batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation, null, null, cancelToken));

                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue<int>(false, "Update rows cancelled.", null);

                        batchOperation = new TableBatchOperation();
                    }
                }

                if (batchOperation.Count > 0)
                {
                    batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation));
                }

                await Task.WhenAll(batchTasks.ToArray());

                return new ReturnValue<int>(true, "", null, 1);
            }
            catch (Exception ex)
            {
                return new ReturnValue<int>(false, "The Azure insert query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override async Task<ReturnValue<int>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.TableName);

                int rowsUpdated = 0;
                int rowcount = 0;

                List<Task> batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                TableBatchOperation batchOperation = new TableBatchOperation();

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    //Read the key fields from the table
                    TableQuery tableQuery = new TableQuery();

                    //select all columns
                    tableQuery.SelectColumns = (new[] { "partitionKey", "rowKey" }.Concat(table.Columns.Where(c=>c.ColumnName != "partitionKey" || c.ColumnName != "rowKey").Select(c=>c.ColumnName)).ToList());
                    tableQuery.FilterString = BuildFilterString(query.Filters);

                    //run the update 
                    TableContinuationToken continuationToken = null;
                    do
                    {
                        var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancelToken);
                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue<int>(false, "Update rows cancelled.", null);

                        continuationToken = result.ContinuationToken;

                        foreach(var entity in result.Results)
                        {

                            foreach (var column in query.UpdateColumns)
                            {
                                entity.Properties[column.Column] = NewEntityProperty(table[column.Column].DataType, column.Value);
                            }

                            batchOperation.Replace(entity);

                            rowcount++;
                            rowsUpdated++;

                            if(rowcount > 99)
                            {
                                rowcount = 0;
                                batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation));
                                batchOperation = new TableBatchOperation();
                            }
                        }

                    } while (continuationToken != null);
                }

                if (batchOperation.Count > 0)
                {
                    batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation));
                }

                await Task.WhenAll(batchTasks.ToArray());

                return new ReturnValue<int>(true, rowsUpdated);
            }
            catch(Exception ex)
            {
                return new ReturnValue<int>(false, "The Azure update query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override async Task<ReturnValue<int>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.TableName);

                int rowsDeleted = 0;
                int rowcount = 0;

                List<Task> batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                TableBatchOperation batchOperation = new TableBatchOperation();

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<int>(false, "Delete rows cancelled.", null);

                    //Read the key fields from the table
                    TableQuery tableQuery = new TableQuery();
                    tableQuery.SelectColumns = new[] { "partitionKey", "rowKey" };
                    tableQuery.FilterString = BuildFilterString(query.Filters);
                    //TableResult = TableReference.ExecuteQuery(TableQuery);

                    TableContinuationToken continuationToken = null;
                    do
                    {
                        var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancelToken);
                        continuationToken = result.ContinuationToken;

                        foreach (var entity in result.Results)
                        {
                            batchOperation.Delete(entity);
                            rowcount++;
                            rowsDeleted++;

                            if (rowcount > 99)
                            {
                                await cTable.ExecuteBatchAsync(batchOperation);
                                batchOperation = new TableBatchOperation();
                                rowcount = 0;
                            }
                        }

                    } while (continuationToken != null);

                }

                if (batchOperation.Count > 0)
                {
                    await cTable.ExecuteBatchAsync(batchOperation);
                }

                return new ReturnValue<int>(true, "", null, rowsDeleted); 
            }
            catch (Exception ex)
            {
                return new ReturnValue<int>(false, "The Azure update query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.TableName);

                //Read the key fields from the table
                TableQuery tableQuery = new TableQuery();
                tableQuery.SelectColumns = query.Columns.Select(c=>c.Column).ToArray();
                tableQuery.FilterString = BuildFilterString(query.Filters);
                tableQuery.Take(1);

                TableContinuationToken continuationToken = null;
                var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancelToken);

                if (cancelToken.IsCancellationRequested)
                    return new ReturnValue<object>(false, "Execute scalar cancelled.", null);

                continuationToken = result.ContinuationToken;

                object value;
                //get the result value
                if (result.Results.Count == 0)
                    value = null;
                else
                    value = result.Results[0].Properties[query.Columns[0].Column].PropertyAsObject;

                //convert it back to a .net type.
                value = ConvertEntityProperty(table[query.Columns[0].Column].DataType, value);

                return new ReturnValue<object>(true, value);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object>(false, "The Azure select query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, SelectQuery query = null)
        {
            throw new NotImplementedException("The execute reader is not available for Azure table connections.");
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null)
        {
            var reader = new ReaderAzure(this, table);
            return reader;
        }
    }
}
