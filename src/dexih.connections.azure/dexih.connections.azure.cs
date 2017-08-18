using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using dexih.functions;
using System.Data.Common;
using dexih.transforms;
using static dexih.functions.DataType;
using System.Text.RegularExpressions;
using System.Threading;
using System.Diagnostics;

namespace dexih.connections.azure
{
    public class ConnectionAzureTable : Connection
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
        public override bool CanUseBinary => true;
        public override bool CanUseSql => false;

        public override object GetDataTypeMinValue(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(1800, 01, 02, 0, 0, 0, 0);
                default:
                    return DataType.GetDataTypeMinValue(typeCode);
            }

        }

        public override object GetDataTypeMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }

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
            return Regex.IsMatch(name, "^[A-Za-z][A-Za-z0-9]{2,254}$");
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.Name);

                var exists = await cTable.ExistsAsync(null, null, cancelToken);

                return new ReturnValue<bool>(true, exists);
            }
            catch (Exception ex)
            {
                return new ReturnValue<bool>(false, "Error testing table exists: " + ex.Message, ex);
            }
        }


        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                string targetTableName = table.Name;
                var timer = Stopwatch.StartNew();

                List<Task> tasks = new List<Task>();

                //create buffers of data and write in parallel.
                int bufferSize = 0;
                List<object[]> buffer = new List<object[]>();

                var sk = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);

                while (await reader.ReadAsync(cancelToken))
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Insert rows cancelled.", null, timer.ElapsedTicks);

                    if (bufferSize > 99)
                    {
                        tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancelToken));
                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue<long>(false, "Update rows cancelled.", null, timer.ElapsedTicks);

                        bufferSize = 0;
                        buffer = new List<object[]>();
                    }

                    object[] row = new object[table.Columns.Count];

                    for (int i = 0; i < table.Columns.Count; i++)
                    {
                        if (i < reader.FieldCount)
                            row[i] = reader[i];
                        else
                        {
                            //if the reader does not have the azure fields, then just add defaults.
                            if (table.Columns[i].DeltaType == TableColumn.EDeltaType.AzurePartitionKey)
                                row[i] = "default";
                            else if (table.Columns[i].DeltaType == TableColumn.EDeltaType.AzureRowKey)
                            {
                                if (sk != null)
                                    row[i] = reader[sk.Name];
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
                    return new ReturnValue<long>(false, "Update rows cancelled.", null, timer.ElapsedTicks);

                bufferSize = 0;
                buffer = new List<object[]>();

                await Task.WhenAll(tasks);

                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }
            catch (StorageException ex)
            {
                string message = "Error writing to Azure Storage table: " + table.Name + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".";
                return new ReturnValue<long>(false, message, ex);
            }
            catch (Exception ex)
            {
                string message = "Error writing to Azure Storage table: " + table.Name + ".  Error Message: " + ex.Message;
                return new ReturnValue<long>(false, message, ex);
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
                        properties.Add(table.Columns[i].Name, NewEntityProperty(table.Columns[i].Datatype, value));
                    }

                var partionKeyValue = partitionKey >= 0 ? row[partitionKey] : "default";
                var rowKeyValue = rowKey >= 0 ? row[rowKey] : surrogateKey >= 0 ? ((long)row[surrogateKey]).ToString("D20") : Guid.NewGuid().ToString();
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
        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            try
            {
                if (!IsValidTableName(table.Name))
                    return new ReturnValue(false, "The table " + table.Name + " could not be created as it does not meet Azure table naming standards.", null);

                foreach (var col in table.Columns)
                {
                    if (!IsValidColumnName(col.Name))
                        return new ReturnValue(false, "The table " + table.Name + " could not be created as the column " + col.Name + " does not meet Azure table naming standards.", null);
                }

                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.Name);
                if (dropTable)
                    await cTable.DeleteIfExistsAsync();

                var exists = await cTable.ExistsAsync();
                if (exists)
                    return new ReturnValue(true);

                //bool result = await Retry.Do(async () => await cTable.CreateIfNotExistsAsync(), TimeSpan.FromSeconds(10), 6);

                bool isCreated = false;
                for (int i = 0; i < 10; i++)
                {
                    try
                    {
                        isCreated = await GetCloudTableClient().GetTableReference(table.Name).CreateIfNotExistsAsync();
                        if (isCreated)
                            break;
                        await Task.Delay(5000, cancelToken);
                    }
                    catch
                    {
                        await Task.Delay(5000, cancelToken);
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

            if (UseConnectionString)
                storageAccount = CloudStorageAccount.Parse(ConnectionString);
            // Retrieve the storage account from the connection string.
            else if (string.IsNullOrEmpty(Username)) //no username, then use the development settings.
                storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
            else
                storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=" + Username + ";AccountKey=" + Password + ";TableEndpoint=" + Server);

            //ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(storageAccount.TableEndpoint);
            //tableServicePoint.UseNagleAlgorithm = false;
            //tableServicePoint.ConnectionLimit = 10000;

            // Create the table client.
            return storageAccount.CreateCloudTableClient();
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
        {
            try
            {
                var testConnect = GetCloudTableClient();
                List<string> list = await Task.Run(() => new List<string> { "Default" });
                return new ReturnValue<List<string>>(true, list);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error was encountered when getting a list databases: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                TableContinuationToken continuationToken = null;
                var list = new List<Table>();
                do
                {
                    var table = await connection.ListTablesSegmentedAsync(continuationToken);
                    continuationToken = table.ContinuationToken;
					list.AddRange(table.Results.Select(c => new Table(c.Name)));

                } while (continuationToken != null);

                return new ReturnValue<List<Table>>(true, list);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<Table>>(false, "The following error was encountered when getting a list of Azure tables: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();


                //The new datatable that will contain the table schema
                Table table = new Table(originalTable.Name);
                table.LogicalName = originalTable.Name;
                table.Description = "";

                CloudTable cloudTable = connection.GetTableReference(table.Name);
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
                            Name = property.Key,
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

        /// <summary>
        /// Azure can always return true for CompareTable, as the columns are not created in the same way relational tables are.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public override async Task<ReturnValue> CompareTable(Table table, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue(true), cancelToken);
        }

        /// <summary>
        /// Azure does not have a max function, so use a different method to generate a surrogate key.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="surrogateKeyColumn"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public override async Task<ReturnValue<long>> GetIncrementalKey(Table table, TableColumn surrogateKeyColumn, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference("DexihKeys");

                long incrementalKey = 0;
                Guid lockGuid = Guid.NewGuid();

                if (!await cTable.ExistsAsync())
                {
                    await cTable.CreateAsync();
                }

                DynamicTableEntity entity = null;

                do
                {
                    //get the last key value if it exists.
                    var tableResult = await cTable.ExecuteAsync(TableOperation.Retrieve(table.Name, surrogateKeyColumn.Name, new List<string>() { "IncrementalValue", "LockGuid" }));
                    if (tableResult.Result == null)
                    {
                        entity = new DynamicTableEntity(table.Name, surrogateKeyColumn.Name);
                        entity.Properties.Add("IncrementalValue", new EntityProperty((long)1));
                        entity.Properties.Add("LockGuid", new EntityProperty(lockGuid));
                        incrementalKey = 1;
                    }
                    else
                    {
                        entity = tableResult.Result as DynamicTableEntity;
                        incrementalKey = entity.Properties["IncrementalValue"].Int64Value.Value;
                        incrementalKey++;
                        entity.Properties["IncrementalValue"] = new EntityProperty(incrementalKey);
                        entity.Properties["LockGuid"] = new EntityProperty(lockGuid);
                    }

                    //update the record with the new incrementalvalue and the guid.
                    await cTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));

                    tableResult = await cTable.ExecuteAsync(TableOperation.Retrieve(table.Name, surrogateKeyColumn.Name, new List<string>() { "IncrementalValue", "LockGuid" }));
                    entity = tableResult.Result as DynamicTableEntity;

                } while (entity.Properties["LockGuid"].GuidValue.Value != lockGuid);

                return new ReturnValue<long>(true, incrementalKey);
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> UpdateIncrementalKey(Table table, string surrogateKeyColumn, long value, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference("DexihKeys");

                if (!await cTable.ExistsAsync())
                {
                    await cTable.CreateAsync();
                }

                DynamicTableEntity entity = null;
                entity = new DynamicTableEntity(table.Name, surrogateKeyColumn);
                entity.Properties.Add("IncrementalValue", new EntityProperty(value));
                entity.Properties.Add("LockGuid", new EntityProperty(Guid.NewGuid()));

                //update the record with the new incrementalvalue and the guid.
                await cTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, ex.Message, ex);
            }
        }


        public string ConvertOperator(Filter.ECompare Operator)
        {
            switch (Operator)
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
                    var filterOperator = filter.Operator;

                    string filterString;

                    if (filter.Value2.GetType().IsArray)
                    {
                        List<object> array = new List<object>();
                        foreach (object value in (Array)filter.Value2)
                        {
                            var valueparse = TryParse(filter.CompareDataType, value);
                            if (!valueparse.Success)
                                throw new Exception("The filter value " + value.ToString() + " could not be convered to a " + filter.CompareDataType.ToString());
                            array.Add(valueparse.Value);
                        }
                        filterString = " (" + string.Join(" or ", array.Select(c => GenerateFilterCondition(filter.Column1.Name, Filter.ECompare.IsEqual, filter.CompareDataType, c))) + ")";
                    }
                    else
                    {
                        var value2Parse = TryParse(filter.CompareDataType, filter.Value2);
                        if (!value2Parse.Success)
                            throw new Exception("The filter value " + filter.Value2.ToString() + " could not be convered to a " + filter.CompareDataType.ToString());
                        var value2 = value2Parse.Value;

                        filterString = GenerateFilterCondition(filter.Column1.Name, filter.Operator, filter.CompareDataType, value2);
                    }

                    if (combinedFilterString == "")
                        combinedFilterString = filterString;
                    else if (filterString != "")
                        combinedFilterString = TableQuery.CombineFilters(combinedFilterString, filter.AndOr.ToString().ToLower(), filterString);
                }
                return combinedFilterString;
            }
        }

        private string GenerateFilterCondition(string column, Filter.ECompare filterOperator, ETypeCode compareDataType, object value)
        {
            string filterString;

            switch (compareDataType)
            {
                case ETypeCode.String:
                case ETypeCode.Guid:
                case ETypeCode.Unknown:
                    filterString = TableQuery.GenerateFilterCondition(column, ConvertOperator(filterOperator), (string)value);
                    break;
                case ETypeCode.Boolean:
                    filterString = TableQuery.GenerateFilterConditionForBool(column, ConvertOperator(filterOperator), (bool)value);
                    break;
                case ETypeCode.Int16:
                case ETypeCode.Int32:
                case ETypeCode.UInt16:
                case ETypeCode.UInt32:
                    filterString = TableQuery.GenerateFilterConditionForInt(column, ConvertOperator(filterOperator), (int)value);
                    break;
                case ETypeCode.UInt64:
                case ETypeCode.Int64:
                    filterString = TableQuery.GenerateFilterConditionForLong(column, ConvertOperator(filterOperator), (long)value);
                    break;
                case ETypeCode.DateTime:
                    filterString = TableQuery.GenerateFilterConditionForDate(column, ConvertOperator(filterOperator), (DateTime)value);
                    break;
                case ETypeCode.Time:
                    filterString = TableQuery.GenerateFilterCondition(column, ConvertOperator(filterOperator), value.ToString());
                    break;
                case ETypeCode.Double:
                case ETypeCode.Decimal:
                    filterString = TableQuery.GenerateFilterConditionForDouble(column, ConvertOperator(filterOperator), (double)value);
                    break;
                default:
                    throw new Exception("The data type: " + compareDataType.ToString() + " is not supported by Azure table storage.");
            }

            return filterString;
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                return await CreateTable(table, true, cancelToken);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The truncate table failed.  This may be due to Azure garbage collection processes being too slow.  The error was: " + ex.Message, ex);
            }
        }


        public override async Task<ReturnValue<Table>> InitializeTable(Table table, int position)
        {
            await Task.Run(() =>
            {
                if (!table.Columns.Any(c => c.DeltaType == TableColumn.EDeltaType.AzurePartitionKey))
                {
                    //partion key uses the AuditKey which allows bulk load, and can be used as an incremental checker.
                    table.Columns.Add(new TableColumn()
                    {
                        Name = "PartitionKey",
                        Datatype = ETypeCode.String,
                        MaxLength = 0,
                        Precision = 0,
                        AllowDbNull = false,
                        LogicalName = table.Name + " partition key.",
                        Description = "The Azure partition key and UpdateAuditKey for this table.",
                        IsUnique = true,
                        DeltaType = TableColumn.EDeltaType.AzurePartitionKey,
                        IsIncrementalUpdate = true,
                        IsMandatory = true
                    });
                }

                if (!table.Columns.Any(c => c.DeltaType == TableColumn.EDeltaType.AzureRowKey))
                {
                    //add the special columns for managed tables.
                    table.Columns.Add(new TableColumn()
                    {
                        Name = "RowKey",
                        Datatype = ETypeCode.String,
                        MaxLength = 0,
                        Precision = 0,
                        AllowDbNull = false,
                        LogicalName = table.Name + " surrogate key",
                        Description = "The azure rowKey and the natural key for this table.",
                        IsUnique = true,
                        DeltaType = TableColumn.EDeltaType.AzureRowKey,
                        IsMandatory = true
                    });
                }

                if (!table.Columns.Any(c => c.DeltaType == TableColumn.EDeltaType.TimeStamp))
                {

                    //add the special columns for managed tables.
                    table.Columns.Add(new TableColumn()
                    {
                        Name = "Timestamp",
                        Datatype = ETypeCode.DateTime,
                        MaxLength = 0,
                        Precision = 0,
                        AllowDbNull = false,
                        LogicalName = table.Name + " timestamp.",
                        Description = "The Azure Timestamp for the managed table.",
                        IsUnique = true,
                        DeltaType = TableColumn.EDeltaType.TimeStamp,
                        IsMandatory = true
                    });
                }
            });

            return new ReturnValue<Table>(true, table);
        }

        private EntityProperty NewEntityProperty(ETypeCode typeCode, object value)
        {
            var returnValue = DataType.TryParse(typeCode, value);
            if (!returnValue.Success)
                throw new Exception(returnValue.Message);

            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return new EntityProperty((Byte?)returnValue.Value);
                case ETypeCode.SByte:
                    return new EntityProperty((SByte?)returnValue.Value);
                case ETypeCode.UInt16:
                    return new EntityProperty((UInt16?)returnValue.Value);
                case ETypeCode.UInt32:
                    return new EntityProperty((UInt32?)returnValue.Value);
                case ETypeCode.UInt64:
                    return new EntityProperty(Convert.ToInt64(returnValue.Value));
                case ETypeCode.Int16:
                    return new EntityProperty((Int16?)returnValue.Value);
                case ETypeCode.Int32:
                    return new EntityProperty((Int32?)returnValue.Value);
                case ETypeCode.Int64:
                    return new EntityProperty((Int64?)returnValue.Value);
                case ETypeCode.Double:
                    return new EntityProperty((Double?)returnValue.Value);
                case ETypeCode.Single:
                    return new EntityProperty((Single?)returnValue.Value);
                case ETypeCode.String:
                    return new EntityProperty((String)returnValue.Value);
                case ETypeCode.Boolean:
                    return new EntityProperty((Boolean?)returnValue.Value);
                case ETypeCode.DateTime:
                    return new EntityProperty((DateTime?)returnValue.Value);
                case ETypeCode.Guid:
                    return new EntityProperty((Guid?)returnValue.Value);
                case ETypeCode.Decimal:
                case ETypeCode.Unknown:
                    return new EntityProperty(value.ToString()); //decimal not supported, so convert to string
                case ETypeCode.Time:
                    return new EntityProperty(((TimeSpan)value).Ticks); //timespan not supported, so use ticks.
                case ETypeCode.Binary:
                    return new EntityProperty((byte[])value);
                default:
                    throw new Exception("Cannot create new azure entity as the data type: " + typeCode.ToString() + " is not suppored.");
            }

        }

        public object ConvertEntityProperty(ETypeCode typeCode, object value)
        {
            if (value == null)
                return null;

            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return Convert.ToByte(value);
                case ETypeCode.SByte:
                    return Convert.ToSByte(value);
                case ETypeCode.UInt16:
                    return Convert.ToUInt16(value);
                case ETypeCode.UInt32:
                    return Convert.ToUInt32(value);
                case ETypeCode.UInt64:
                    return Convert.ToUInt64(value);
                case ETypeCode.Int16:
                    return Convert.ToInt16(value);
                case ETypeCode.Int32:
                    return Convert.ToInt32(value);
                case ETypeCode.Int64:
                    return Convert.ToInt64(value);
                case ETypeCode.Double:
                    return Convert.ToDouble(value);
                case ETypeCode.Single:
                    return Convert.ToSingle(value);
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


        public override async Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.Name);

                int rowsInserted = 0;
                int rowcount = 0;
                Stopwatch timer = Stopwatch.StartNew();

                List<Task> batchTasks = new List<Task>();

                var autoIncrement = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                long lastAutoIncrement = 0;

                //start a batch operation to update the rows.
                TableBatchOperation batchOperation = new TableBatchOperation();

                var partitionKey = table.GetDeltaColumn(TableColumn.EDeltaType.AzurePartitionKey);
                var rowKey = table.GetDeltaColumn(TableColumn.EDeltaType.AzureRowKey);
                var timeStamp = table.GetDeltaColumn(TableColumn.EDeltaType.TimeStamp);

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<Tuple<long, long>>(false, "Insert rows cancelled.", null, Tuple.Create(timer.ElapsedTicks, (long)0));

                    Dictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();
                    foreach (var field in query.InsertColumns)
                    {
                        if (!(field.Column.Name == "RowKey" || field.Column.Name == "PartitionKey" || field.Column.Name == "Timestamp"))
                            properties.Add(field.Column.Name, NewEntityProperty(table.Columns[field.Column].Datatype, field.Value));
                    }

                    if (autoIncrement != null)
                    {
                        var autoIncrementResult = await GetIncrementalKey(table, autoIncrement, CancellationToken.None);
                        lastAutoIncrement = autoIncrementResult.Value;

                        properties.Add(autoIncrement.Name, NewEntityProperty(ETypeCode.Int64, lastAutoIncrement));
                    }

                    string partitionKeyValue = null;
                    if (partitionKey != null)
                        partitionKeyValue = query.InsertColumns.SingleOrDefault(c => c.Column.Name == partitionKey.Name)?.Value.ToString();

                    if (string.IsNullOrEmpty(partitionKeyValue)) partitionKeyValue = "default";

                    string rowKeyValue = null;
                    if (rowKey != null)
                        rowKeyValue = query.InsertColumns.SingleOrDefault(c => c.Column.Name == rowKey.Name)?.Value.ToString();

                    if (string.IsNullOrEmpty(rowKeyValue))
                    {
                        var sk = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey)?.Name;

                        if (sk == null)
                            if (autoIncrement == null)
                                rowKeyValue = Guid.NewGuid().ToString();
                            else
                                rowKeyValue = lastAutoIncrement.ToString("D20");
                        else
                            rowKeyValue = ((long)query.InsertColumns.Single(c => c.Column.Name == sk).Value).ToString("D20");
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
                            return new ReturnValue<Tuple<long, long>>(false, "Update rows cancelled.", null, Tuple.Create(timer.ElapsedTicks, (long)0));

                        batchOperation = new TableBatchOperation();
                    }
                }

                if (batchOperation.Count > 0)
                {
                    batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation));
                }

                await Task.WhenAll(batchTasks.ToArray());

                return new ReturnValue<Tuple<long, long>>(true, Tuple.Create(timer.ElapsedTicks, (long)lastAutoIncrement));
            }
            catch (Exception ex)
            {
                return new ReturnValue<Tuple<long, long>>(false, "The Azure insert query for " + table.Name + " could not be run due to the following error: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.Name);

                int rowsUpdated = 0;
                int rowcount = 0;
                Stopwatch timer = Stopwatch.StartNew();

                List<Task> batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                TableBatchOperation batchOperation = new TableBatchOperation();

                var surrogateKeyColumn = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    //Read the key fields from the table
                    TableQuery tableQuery = new TableQuery();

                    //select all columns
                    tableQuery.SelectColumns = (new[] { "PartitionKey", "RowKey" }.Concat(table.Columns.Where(c => c.Name != "PartitionKey" && c.Name != "RowKey").Select(c => c.Name)).ToList());

                    //the rowkey is the same as the surrogate key, so add this to the filter string if the surrogate key is used.
                    if (surrogateKeyColumn != null)
                    {
                        int filtercount = query.Filters.Count;
                        for (int i = 0; i < filtercount; i++)
                        {
                            if (query.Filters[i].Column1.Name == surrogateKeyColumn.Name)
                            {
                                var rowKeyValue = ((long)query.Filters[i].Value2).ToString("D20");

                                query.Filters.Add(new Filter()
                                {
                                    Column1 = new TableColumn("RowKey", ETypeCode.String),
                                    Column2 = query.Filters[i].Column2,
                                    Value1 = query.Filters[i].Value1,
                                    Value2 = rowKeyValue,
                                    AndOr = query.Filters[i].AndOr,
                                    CompareDataType = ETypeCode.String,
                                    Operator = query.Filters[i].Operator
                                });
                            }
                        }
                    }

                    tableQuery.FilterString = BuildFilterString(query.Filters);

                    //run the update 
                    TableContinuationToken continuationToken = null;
                    do
                    {
                        var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancelToken);
                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue<long>(false, "Update rows cancelled.", null, timer.ElapsedTicks);

                        continuationToken = result.ContinuationToken;

                        foreach (var entity in result.Results)
                        {

                            foreach (var column in query.UpdateColumns)
                            {
                                switch (column.Column.Name)
                                {
                                    case "RowKey":
                                        entity.RowKey = column.Value.ToString();
                                        break;
                                    case "PartitionKey":
                                        entity.PartitionKey = column.Value.ToString();
                                        break;
                                    default:
                                        entity.Properties[column.Column.Name] = NewEntityProperty(table[column.Column.Name].Datatype, column.Value);
                                        break;
                                }
                            }

                            batchOperation.Replace(entity);

                            rowcount++;
                            rowsUpdated++;

                            if (rowcount > 99)
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

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, "The Azure update query for " + table.Name + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override async Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.Name);

                Stopwatch timer = Stopwatch.StartNew();
                int rowsDeleted = 0;
                int rowcount = 0;

                List<Task> batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                TableBatchOperation batchOperation = new TableBatchOperation();

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Delete rows cancelled.", null, timer.ElapsedTicks);

                    //Read the key fields from the table
                    TableQuery tableQuery = new TableQuery();
                    tableQuery.SelectColumns = new[] { "PartitionKey", "RowKey" };
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

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, "The Azure update query for " + table.Name + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient connection = GetCloudTableClient();
                CloudTable cTable = connection.GetTableReference(table.Name);

                //Read the key fields from the table
                TableQuery tableQuery = new TableQuery();
                tableQuery.SelectColumns = query.Columns.Select(c => c.Column.Name).ToArray();
                tableQuery.FilterString = BuildFilterString(query.Filters);
                tableQuery.Take(1);

                TableContinuationToken continuationToken = null;
                try
                {
                    var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancelToken);

                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<object>(false, "Execute scalar cancelled.", null);

                    continuationToken = result.ContinuationToken;

                    object value;
                    //get the result value
                    if (result.Results.Count == 0)
                        value = null;
                    else
                    {
                        switch (query.Columns[0].Column.Name)
                        {
                            case "RowKey":
                                value = result.Results[0].RowKey;
                                break;
                            case "PartitionKey":
                                value = result.Results[0].PartitionKey;
                                break;
                            default:
                                value = result.Results[0].Properties[query.Columns[0].Column.Name].PropertyAsObject;
                                break;
                        }
                    }

                    //convert it back to a .net type.
                    value = ConvertEntityProperty(table.Columns[query.Columns[0].Column].Datatype, value);
                    return new ReturnValue<object>(true, value);

                }
                catch (StorageException ex)
                {
                    string message = "Error running a command against table: " + table.Name + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".";
                    return new ReturnValue<object>(false, message, ex);
                }


            }
            catch (Exception ex)
            {
                return new ReturnValue<object>(false, "The Azure select query for " + table.Name + " could not be run due to the following error: " + ex.Message, ex, -1);
            }
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException("The execute reader is not available for Azure table connections.");
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderAzure(this, table);
            return reader;
        }

    }
}
