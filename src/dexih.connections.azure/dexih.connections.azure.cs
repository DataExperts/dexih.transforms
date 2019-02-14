using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using dexih.functions;
using System.Data.Common;
using dexih.transforms;
using System.Text.RegularExpressions;
using System.Threading;
using static Dexih.Utils.DataType.DataType;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.connections.azure
{
    [Connection(
        ConnectionCategory = EConnectionCategory.NoSqlDatabase,
        Name = "Azure Storage Tables", 
        Description = "A NoSQL key-value store which supports massive semi-structured datasets",
        DatabaseDescription = "Database Name",
        ServerDescription = "Azure End Point",
        AllowsConnectionString = true,
        AllowsSql = false,
        AllowsFlatFiles = false,
        AllowsManagedConnection = true,
        AllowsSourceConnection = true,
        AllowsTargetConnection = true,
        AllowsUserPassword = true,
        AllowsWindowsAuth = false,
        RequiresDatabase = true,
        RequiresLocalStorage = false
    )]
    public class ConnectionAzureTable : Connection
    {
        public override bool CanBulkLoad => true;
        public override bool CanSort => false;
        public override bool CanFilter => true;
        public override bool CanDelete => true;
        public override bool CanUpdate => true;
        public override bool CanAggregate => false;
        public override bool CanUseBinary => true;
        public override bool CanUseArray => false;
        public override bool CanUseJson => false;
        public override bool CanUseXml => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseSql => false;
        public override bool CanUseAutoIncrement => false;
        public override bool DynamicTableCreation => true;
        
        public override bool CanUseGuid => true;

        /// <summary>
        /// Name of the table used to store surrogate keys.
        /// </summary>
        public string SurrogateKeyTable => "DexihKeys";
        
        /// <summary>
        /// Name of the column in the surrogate key table to store latest incremental value.
        /// </summary>
        public string IncrementalValueName => "IncrementalValue";
        
        /// <summary>
        /// Name of the property which is a guid and used to lock rows when updating.
        /// </summary>
        public string LockGuidName => "LockGuid";

        public string AzurePartitionKeyDefaultValue => "default";
        


        public override object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(1800, 01, 02, 0, 0, 0, 0, DateTimeKind.Utc);
                case ETypeCode.Double:
                    return -1E+100;
                case ETypeCode.Single:
                    return -1E+37F;
                default:
                    return GetDataTypeMinValue(typeCode, length);
            }
        }

        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return DateTime.MaxValue.ToUniversalTime();
                case ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
                case ETypeCode.Double:
                    return 1E+100;
                case ETypeCode.Single:
                    return 1E+37F;

                default:
                    return GetDataTypeMaxValue(typeCode, length);
            }
        }
        
        public object ConvertParameterType(object value)
        {
            if (value == null)
                return DBNull.Value;
            
            return value;
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

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var exists = cTable.ExistsAsync(null, null, cancellationToken);

                return exists;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Could not check if table exists.  {ex.Message}");
            }
        }


        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
        {
            try
            {
                var targetTableName = table.Name;

                var tasks = new List<Task>();

                //create buffers of data and write in parallel.
                var bufferSize = 0;
                var buffer = new List<object[]>();

                var sk = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);

                while (await reader.ReadAsync(cancellationToken))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException($"Bulk insert operation was cancelled.");
                    }

                    if (bufferSize > 99)
                    {
                        tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancellationToken));
                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new ConnectionException($"Bulk insert operation was cancelled.");
                        }

                        bufferSize = 0;
                        buffer = new List<object[]>();
                    }

                    var row = new object[table.Columns.Count];

                    for (var i = 0; i < table.Columns.Count; i++)
                    {
                        if (i < reader.FieldCount)
                        {
                            row[i] = reader[i];
                        }
                        else
                        {
                            //if the reader does not have the azure fields, then just add defaults.
                            if (table.Columns[i].DeltaType == TableColumn.EDeltaType.AzurePartitionKey)
                                row[i] = AzurePartitionKeyDefaultValue;
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
                tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancellationToken));
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new ConnectionException($"Bulk insert operation was cancelled.");
                }

                await Task.WhenAll(tasks);
            }
            catch (StorageException ex)
            {
                throw new ConnectionException($"Error writing to Azure Storage table: " + table.Name + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".", ex);
            }
            catch (Exception ex)
            {
                throw new ConnectionException("Error writing to Azure Storage table: " + table.Name + ".  Error: " + ex.Message, ex);
            }
        }

        private Task WriteDataBuffer(Table table, IEnumerable<object[]> buffer, string targetTableName, CancellationToken cancellationToken)
        {
            var connection = GetCloudTableClient();
            var cloudTable = connection.GetTableReference(targetTableName);

            // Create the batch operation.
            var batchOperation = new TableBatchOperation();

            var partitionKey = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzurePartitionKey);
            var rowKey = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzureRowKey);
            var surrogateKey = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SurrogateKey);

            foreach (var row in buffer)
            {
                var properties = new Dictionary<string, EntityProperty>();
                for (var i = 0; i < table.Columns.Count; i++)
                    if (table.Columns[i].DeltaType != TableColumn.EDeltaType.AzureRowKey && table.Columns[i].DeltaType != TableColumn.EDeltaType.AzurePartitionKey && table.Columns[i].DeltaType != TableColumn.EDeltaType.TimeStamp)
                    {
                        var value = row[i];
                        if (value == DBNull.Value) value = null;
                        properties.Add(table.Columns[i].Name, NewEntityProperty(table.Columns[i].DataType, value, table.Columns[i].Rank));
                    }

                var partitionKeyValue = partitionKey >= 0 ? row[partitionKey] : AzurePartitionKeyDefaultValue;
                var rowKeyValue = rowKey >= 0 ? row[rowKey] : surrogateKey >= 0 ? ((long)row[surrogateKey]).ToString("D20") : Guid.NewGuid().ToString();
                var entity = new DynamicTableEntity(partitionKeyValue.ToString(), rowKeyValue.ToString(), "*", properties);

                batchOperation.Insert(entity);
            }
            return cloudTable.ExecuteBatchAsync(batchOperation, null, null, cancellationToken);
        }


        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="dropTable"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
        {
            try
            {
                if (!IsValidTableName(table.Name))
                {
                    throw new ConnectionException("The table " + table.Name + " could not be created as it does not meet Azure table naming standards.");
                }

                foreach (var col in table.Columns)
                {
                    if (!IsValidColumnName(col.Name))
                    {
                        throw new ConnectionException("The table " + table.Name + " could not be created as the column " + col.Name + " does not meet Azure table naming standards.");
                    }
                }

                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);
                if (dropTable)
                    await cTable.DeleteIfExistsAsync();

                var exists = await cTable.ExistsAsync();
                if (exists)
                {
                    return;
                }

                var isCreated = false;
                for (var i = 0; i < 10; i++)
                {
                    try
                    {
                        isCreated = await GetCloudTableClient().GetTableReference(table.Name).CreateIfNotExistsAsync();
                        if (isCreated)
                            break;
                        await Task.Delay(5000, cancellationToken);
                    }
                    catch
                    {
                        await Task.Delay(5000, cancellationToken);
                    }
                }

                if(!isCreated)
                {
                    throw new ConnectionException("Failed to create table after 10 attempts.");
                }

                return;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error creating Azure table {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets a connection refererence to the Azure server.
        /// </summary>
        /// <returns></returns>
        public CloudTableClient GetCloudTableClient()
        {
            CloudStorageAccount storageAccount;

            if (UseConnectionString)
                storageAccount = CloudStorageAccount.Parse(ConnectionString);
            else
                storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=" + Username + ";AccountKey=" + Password + ";TableEndpoint=" + Server);

            // Create the table client.
            return storageAccount.CreateCloudTableClient();
        }

        /// <summary>
        /// Azure does not have databases, so this is a dummy function.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Azure does not have databases, so this returns a dummy value.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
        {
            var list = new List<string> { "Default" };
            return Task.FromResult(list);
        }

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken)
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

                return list;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error getting Azure table list {ex.Message}", ex);
            }
        }

        public override async Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();


                //The new datatable that will contain the table schema
                var table = new Table(originalTable.Name)
                {
                    LogicalName = originalTable.Name,
                    Description = ""
                };

                var cloudTable = connection.GetTableReference(table.Name);
                var query = new TableQuery().Take(1);

                TableContinuationToken continuationToken = null;
                var list = new List<DynamicTableEntity>();
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
                        var col = new TableColumn()
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
                return table;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error getting Azure table information for table {originalTable.Name}.  {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Azure can always return true for CompareTable, as the columns are not created in the same way relational tables are.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task<bool> CompareTable(Table table, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        /// <summary>
        /// Note: Azure does not have a max function, so we used a key's table to store surrogate keys for each table.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="surrogateKeyColumn"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task<long> GetNextKey(Table table, TableColumn surrogateKeyColumn, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference("DexihKeys");

                long incrementalKey = 0;
                var lockGuid = Guid.NewGuid();

                if (!await cTable.ExistsAsync())
                {
                    await cTable.CreateAsync();
                }

                DynamicTableEntity entity = null;

                do
                {
                    //get the last key value if it exists.
                    var tableResult = await cTable.ExecuteAsync(TableOperation.Retrieve(table.Name, surrogateKeyColumn.Name, new List<string>() { IncrementalValueName, LockGuidName }));
                    if (tableResult.Result == null)
                    {
                        entity = new DynamicTableEntity(table.Name, surrogateKeyColumn.Name);
                        entity.Properties.Add(IncrementalValueName, new EntityProperty((long)1));
                        entity.Properties.Add(LockGuidName, new EntityProperty(lockGuid));
                        incrementalKey = 1;
                    }
                    else
                    {
                        entity = tableResult.Result as DynamicTableEntity;
                        incrementalKey = entity.Properties[IncrementalValueName].Int64Value.Value;
                        incrementalKey++;
                        entity.Properties[IncrementalValueName] = new EntityProperty(incrementalKey);
                        entity.Properties[LockGuidName] = new EntityProperty(lockGuid);
                    }

                    //update the record with the new incremental value and the guid.
                    await cTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));

                    tableResult = await cTable.ExecuteAsync(TableOperation.Retrieve(table.Name, surrogateKeyColumn.Name, new List<string>() { IncrementalValueName, LockGuidName }));
                    entity = tableResult.Result as DynamicTableEntity;

                } while (entity.Properties[LockGuidName].GuidValue.Value != lockGuid);

                return incrementalKey;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Azure Error getting incremental key for table {table.Name} {ex.Message}", ex);
            }
        }

        public override async Task UpdateIncrementalKey(Table table, string surrogateKeyColumn, object value, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference("DexihKeys");

                if (!await cTable.ExistsAsync())
                {
                    await cTable.CreateAsync();
                }

                DynamicTableEntity entity = null;
                entity = new DynamicTableEntity(table.Name, surrogateKeyColumn);
                switch (value)
                {
                    case short shortValue:
                        entity.Properties.Add(IncrementalValueName, new EntityProperty(shortValue));
                        break;
                    case int intValue:
                        entity.Properties.Add(IncrementalValueName, new EntityProperty(intValue));
                        break;
                    case long longValue:
                        entity.Properties.Add(IncrementalValueName, new EntityProperty(longValue));
                        break;
                    case ushort ushortValue:
                        entity.Properties.Add(IncrementalValueName, new EntityProperty(ushortValue));
                        break;
                    case uint uintValue:
                        entity.Properties.Add(IncrementalValueName, new EntityProperty(uintValue));
                        break;
                    case ulong ulongValue:
                        entity.Properties.Add(IncrementalValueName, new EntityProperty(ulongValue));
                        break;
                    default:
                        throw new ConnectionException($"The datatype {value.GetType()} is not supported for incremental columns.  Use an integer type instead.");
                }

                entity.Properties.Add(LockGuidName, new EntityProperty(Guid.NewGuid()));

                //update the record with the new incremental value and the guid.
                await cTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Azure Error updating incremental key for table {table.Name} {ex.Message}", ex);
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
                var combinedFilterString = "";

                foreach (var filter in filters)
                {
                    var filterOperator = filter.Operator;

                    string filterString;

                    if (filter.Value2.GetType().IsArray)
                    {
                        var array = new List<object>();
                        foreach (var value in (Array)filter.Value2)
                        {
                            try
                            {
                                var valueparse = Operations.Parse(filter.CompareDataType, value);
                                array.Add(valueparse);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The filter value could not be converted to a {filter.CompareDataType}.  {ex.Message}", ex, value);
                            }
                        }
                        filterString = " (" + string.Join(" or ", array.Select(c => GenerateFilterCondition(filter.Column1.Name, Filter.ECompare.IsEqual, filter.CompareDataType, c))) + ")";
                    }
                    else
                    {
                        object value2 = null;
                        try
                        {
                            value2 = Operations.Parse(filter.CompareDataType, filter.Value2);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"The filter value could not be convered to a {filter.CompareDataType}.  {ex.Message}", ex, filter.Value2);
                        }

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
				case ETypeCode.Text:
				case ETypeCode.Json:
                case ETypeCode.Node:
				case ETypeCode.Xml:
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

        public override Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                return CreateTable(table, true, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The truncate table failed for {table.Name}.  {ex.Message}", ex);
            }
        }


        public override Task<Table> InitializeTable(Table table, int position)
        {
            if (table.Columns.All(c => c.DeltaType != TableColumn.EDeltaType.AzurePartitionKey))
            {
                //partion key uses the AuditKey which allows bulk load, and can be used as an incremental checker.
                table.Columns.Add(new TableColumn()
                {
                    Name = "PartitionKey",
                    DataType = ETypeCode.String,
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

            if (table.Columns.All(c => c.DeltaType != TableColumn.EDeltaType.AzureRowKey))
            {
                //add the special columns for managed tables.
                table.Columns.Add(new TableColumn()
                {
                    Name = "RowKey",
                    DataType = ETypeCode.String,
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

            if (table.Columns.All(c => c.DeltaType != TableColumn.EDeltaType.TimeStamp))
            {

                //add the special columns for managed tables.
                table.Columns.Add(new TableColumn()
                {
                    Name = "Timestamp",
                    DataType = ETypeCode.DateTime,
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

            return Task.FromResult(table);
        }

        private EntityProperty NewEntityProperty(ETypeCode typeCode, object value, int rank)
        {
            var returnValue = ConvertForWrite(typeCode, rank, true, value);

            if (rank > 0) typeCode = ETypeCode.String;

            if (returnValue is DBNull) returnValue = null;

            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return new EntityProperty((byte?)returnValue);
                case ETypeCode.SByte:
                    return new EntityProperty((sbyte?)returnValue);
                case ETypeCode.UInt16:
                    return new EntityProperty((ushort?)returnValue);
                case ETypeCode.UInt32:
                    return new EntityProperty((uint?)returnValue);
                case ETypeCode.UInt64:
                    return new EntityProperty(Convert.ToInt64(returnValue));
                case ETypeCode.Int16:
                    return new EntityProperty((short?)returnValue);
                case ETypeCode.Enum:
                case ETypeCode.Int32:
                    return new EntityProperty((int?)returnValue);
                case ETypeCode.Int64:
                    return new EntityProperty((long?)returnValue);
                case ETypeCode.Double:
                    return new EntityProperty((double?)returnValue);
                case ETypeCode.Single:
                    return new EntityProperty((float?)returnValue);
                case ETypeCode.Object:
                case ETypeCode.CharArray:
                case ETypeCode.Char:
                case ETypeCode.String:
				case ETypeCode.Text:
				case ETypeCode.Json:
				case ETypeCode.Xml:
                case ETypeCode.Node:
                    return new EntityProperty((string)returnValue);
                case ETypeCode.Boolean:
                    return new EntityProperty((bool?)returnValue);
                case ETypeCode.DateTime:
                    return new EntityProperty((DateTime?)returnValue);
                case ETypeCode.Guid:
                    return new EntityProperty((Guid?)returnValue);
                case ETypeCode.Decimal:
                case ETypeCode.Unknown:
                    return new EntityProperty(returnValue?.ToString()); //decimal not supported, so convert to string
                case ETypeCode.Time:
                    return new EntityProperty(returnValue == null ? 0L : ((TimeSpan)value).Ticks); //timespan not supported, so use ticks.
                case ETypeCode.Binary:
                    return new EntityProperty((byte[])value);
                default:
                    throw new Exception("Cannot create new azure entity as the data type: " + typeCode.ToString() + " is not supported.");
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
                case ETypeCode.DateTime:
                case ETypeCode.String:
				case ETypeCode.Text:
				case ETypeCode.Json:
                case ETypeCode.Node:
				case ETypeCode.Xml:
                case ETypeCode.Boolean:
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


        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var rowsInserted = 0;
                var rowcount = 0;

                var batchTasks = new List<Task>();

                var autoIncrement = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                long lastAutoIncrement = 0;

                //start a batch operation to update the rows.
                var batchOperation = new TableBatchOperation();

                var partitionKey = table.GetDeltaColumn(TableColumn.EDeltaType.AzurePartitionKey);
                var rowKey = table.GetDeltaColumn(TableColumn.EDeltaType.AzureRowKey);
                var timeStamp = table.GetDeltaColumn(TableColumn.EDeltaType.TimeStamp);

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Insert rows was cancelled.");
                    }

                    var properties = new Dictionary<string, EntityProperty>();
                    foreach (var field in query.InsertColumns)
                    {
                        if (!(field.Column.Name == "RowKey" || field.Column.Name == "PartitionKey" || field.Column.Name == "Timestamp"))
                            properties.Add(field.Column.Name, NewEntityProperty(table.Columns[field.Column].DataType, field.Value, field.Column.Rank));
                    }

                    if (autoIncrement != null)
                    {
                        var autoIncrementResult = await GetNextKey(table, autoIncrement, CancellationToken.None);
                        lastAutoIncrement = autoIncrementResult;

                        properties.Add(autoIncrement.Name, NewEntityProperty(ETypeCode.Int64, lastAutoIncrement, autoIncrement.Rank));
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

                    var entity = new DynamicTableEntity(partitionKeyValue, rowKeyValue, "*", properties);

                    batchOperation.Insert(entity);

                    rowcount++;
                    rowsInserted++;

                    if (rowcount > 99)
                    {
                        rowcount = 0;
                        batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation, null, null, cancellationToken));

                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new ConnectionException("Insert rows was cancelled.");
                        }

                        batchOperation = new TableBatchOperation();
                    }
                }

                if (batchOperation.Count > 0)
                {
                    batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation));
                }

                await Task.WhenAll(batchTasks.ToArray());

                return (long)lastAutoIncrement;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The Azure insert query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var rowsUpdated = 0;
                var rowcount = 0;

                var batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                var batchOperation = new TableBatchOperation();

                var surrogateKeyColumn = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    //Read the key fields from the table
                    var tableQuery = new TableQuery
                    {
                        //select all columns
                        SelectColumns = (new[] { "PartitionKey", "RowKey" }.Concat(table.Columns.Where(c => c.Name != "PartitionKey" && c.Name != "RowKey").Select(c => c.Name)).ToList())
                    };

                    //the rowkey is the same as the surrogate key, so add this to the filter string if the surrogate key is used.
                    if (surrogateKeyColumn != null)
                    {
                        var filtercount = query.Filters.Count;
                        for (var i = 0; i < filtercount; i++)
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
                        var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancellationToken);
                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new ConnectionException("Update rows cancelled.");
                        }

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
                                        var col = table[column.Column.Name];
                                        entity.Properties[column.Column.Name] = NewEntityProperty(col.DataType, column.Value, col.Rank);
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

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The Azure update query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var rowsDeleted = 0;
                var rowcount = 0;

                var batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                var batchOperation = new TableBatchOperation();

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Delete rows cancelled.");
                    }

                    //Read the key fields from the table
                    var tableQuery = new TableQuery
                    {
                        SelectColumns = new[] { "PartitionKey", "RowKey" },
                        FilterString = BuildFilterString(query.Filters)
                    };
                    //TableResult = TableReference.ExecuteQuery(TableQuery);

                    TableContinuationToken continuationToken = null;
                    do
                    {
                        var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken, null, null, cancellationToken);
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

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The Azure delete query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                //Read the key fields from the table
                var tableQuery = new TableQuery
                {
                    SelectColumns = query.Columns.Select(c => c.Column.Name).ToArray(),
                    FilterString = BuildFilterString(query.Filters)
                };
                tableQuery.Take(1);

                try
                {
                    var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, null, null, null, cancellationToken);

                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Execute scalar cancelled.");
                    }

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
                    value = ConvertEntityProperty(table.Columns[query.Columns[0].Column].DataType, value);
                    return value;

                }
                catch (StorageException ex)
                {
                    var message = "Error running a command against table: " + table.Name + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".";
                    throw new ConnectionException(message, ex);
                }


            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The Azure execut scalar query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException("A native database reader is not available for Azure table connections.");
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderAzure(this, table);
            return reader;
        }

    }
}
