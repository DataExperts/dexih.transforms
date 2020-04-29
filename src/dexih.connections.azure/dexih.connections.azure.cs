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
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace dexih.connections.azure
{
    [Connection(
        ConnectionCategory = EConnectionCategory.NoSqlDatabase,
        Name = "Azure Storage Tables", 
        Description = "A NoSQL key-value store which supports massive semi-structured data-sets",
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
        public override bool CanGroup => false;
        public override bool CanUseBinary => true;
        public override bool CanUseArray => false;
        public override bool CanUseJson => false;
        public override bool CanUseXml => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseSql => false;
        public override bool CanUseDbAutoIncrement => true;
        public override bool DynamicTableCreation => true;
        
        public override bool CanUseGuid => true;

        /// <summary>
        /// Name of the table used to store surrogate keys.
        /// </summary>
        public string IncrementalKeyTable { get; set; } = "DexihKeys";
        
        /// <summary>
        /// Name of the column in the surrogate key table to store latest incremental value.
        /// </summary>
        public string IncrementalValueName => "IncrementalValue";
        
        /// <summary>
        /// Name of the property which is a guid and used to lock rows when updating.
        /// </summary>
        public string LockGuidName => "LockGuid";

        public string AzurePartitionKeyDefaultValue => "default";

        public override bool IsFilterSupported(Filter filter)
        {
            switch (filter.Operator)
            {
                case ECompare.Like:
                case ECompare.IsNull:
                case ECompare.IsNotNull:
                    return false;
            }

            switch (filter.CompareDataType)
            {
                case ETypeCode.Byte:
                case ETypeCode.Char:
                case ETypeCode.SByte:
                case ETypeCode.Enum:
                case ETypeCode.CharArray:
                case ETypeCode.Object:
                case ETypeCode.Geometry:
                    return false;
            }

            return true;
        }

        public override object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                case ETypeCode.Date:
                    return new DateTime(1800, 01, 02, 0, 0, 0, 0, DateTimeKind.Utc);
                case ETypeCode.Double:
                    return -1E+100;
                case ETypeCode.Single:
                    return -1E+37F;
                default:
                    return DataType.GetDataTypeMinValue(typeCode, length);
            }
        }

        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                case ETypeCode.Date:
                    return DateTime.MaxValue.ToUniversalTime();
                case ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
                case ETypeCode.Double:
                    return 1E+100;
                case ETypeCode.Single:
                    return 1E+37F;
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
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

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
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


        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
        {
            try
            {
                var targetTableName = table.Name;

                var tasks = new List<Task>();

                //create buffers of data and write in parallel.
                var bufferSize = 0;
                var buffer = new List<object[]>();

                var sk = table.GetAutoIncrementColumn();
                var skOrdinal = -1;
                if (sk != null)
                {
                    skOrdinal = reader.GetOrdinal(sk.Name);
                }

                var ordinals = new int[table.Columns.Count];

                long keyValue = -1;

                for (var i = 0; i < table.Columns.Count; i++)
                {
                    ordinals[i] = reader.GetOrdinal(table.Columns[i].Name);

                    if (table.Columns[i].DeltaType == EDeltaType.DbAutoIncrement)
                    {
                        keyValue = await GetMaxValue<long>(table, sk, cancellationToken);
                    }
                }

                while (await reader.ReadAsync(cancellationToken))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException($"Bulk insert operation was cancelled.");
                    }

                    if (bufferSize > 99)
                    {
                        tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancellationToken = default));
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
                        var ordinal = ordinals[i];
                        if (ordinal >= 0)
                        {
                            if (table.Columns[i].DeltaType == EDeltaType.DbAutoIncrement &&
                                (reader[ordinal] == null || reader[ordinal] is DBNull))
                            {
                                row[i] = ++keyValue;
                            }
                            else
                            {
                                row[i] = reader[ordinal];
                            }
                        }
                        else
                        {
                            switch (table.Columns[i].DeltaType)
                            {
                                case EDeltaType.PartitionKey:
                                    row[i] = AzurePartitionKeyDefaultValue;
                                    break;
                                case EDeltaType.RowKey:
                                    if (skOrdinal >= 0)
                                        row[i] = reader[skOrdinal];
                                    else
                                        row[i] = Guid.NewGuid().ToString();
                                    break;
                                case EDeltaType.DbAutoIncrement:
                                    row[i] = 0;
                                    break;
                            }
                        }
                    }

                    buffer.Add(row);
                    bufferSize++;
                }
                tasks.Add(WriteDataBuffer(table, buffer, targetTableName, cancellationToken = default));
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new ConnectionException($"Bulk insert operation was cancelled.");
                }

                if (keyValue > -1 && sk != null)
                {
                    await UpdateMaxValue(table, sk.Name, keyValue, cancellationToken);
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

        private Task WriteDataBuffer(Table table, IEnumerable<object[]> buffer, string targetTableName, CancellationToken cancellationToken = default)
        {
            var connection = GetCloudTableClient();
            var cloudTable = connection.GetTableReference(targetTableName);

            // Create the batch operation.
            var batchOperation = new TableBatchOperation();

            var partitionKey = table.GetOrdinal(EDeltaType.PartitionKey);
            var rowKey = table.GetOrdinal(EDeltaType.RowKey);
            var surrogateKey = table.GetAutoIncrementOrdinal();

            foreach (var row in buffer)
            {
                var properties = new Dictionary<string, EntityProperty>();
                for(var i = 0; i < table.Columns.Count; i++)
                {
                    var column = table.Columns[i];
                    if (column.DeltaType == EDeltaType.RowKey ||
                        column.DeltaType == EDeltaType.PartitionKey ||
                        column.DeltaType == EDeltaType.TimeStamp ) continue;

                    var value = row[i];
                    if (value == DBNull.Value) value = null;
                    properties.Add(column.Name, NewEntityProperty(column, value));
                }

                var partitionKeyValue = partitionKey >= 0 ? row[partitionKey] : AzurePartitionKeyDefaultValue;
                var rowKeyValue = rowKey >= 0 ? row[rowKey] : surrogateKey >= 0 ? ConvertKeyValue(row[surrogateKey]) : Guid.NewGuid().ToString();
                var entity = new DynamicTableEntity(partitionKeyValue.ToString(), rowKeyValue.ToString(), "*", properties);

                batchOperation.Insert(entity);
            }
            return cloudTable.ExecuteBatchAsync(batchOperation, null, null, cancellationToken);
        }

        private string ConvertKeyValue(object value)
        {
            switch (value)
            {
                case long longValue:
                    return longValue.ToString("D20");
                case int intValue:
                    return intValue.ToString("D20");
                default:
                    return value.ToString();
            }
        }

        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="dropTable"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
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

                // reset the auto incremental table, when rebuilding the table.
                var incremental = table.GetColumn(EDeltaType.DbAutoIncrement);
                if (incremental != null)
                {
                    await UpdateMaxValue(table, incremental.TableColumnName(), 0, cancellationToken);
                }
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
        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Azure does not have databases, so this returns a dummy value.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            var list = new List<string> { "Default" };
            return Task.FromResult(list);
        }

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
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

        public override async Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken = default)
        {
            try
            {
                var connection = GetCloudTableClient();


                //The new data table that will contain the table schema
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
        public override Task<bool> CompareTable(Table table, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        /// <inheritdoc />
        /// <summary>
        /// Note: Azure does not have a max function, so we used a key's table to store surrogate keys for each table.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task<T> GetMaxValue<T>(Table table, TableColumn column, CancellationToken cancellationToken = default)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(IncrementalKeyTable);

                T value;
                var lockGuid = Guid.NewGuid();

                if (!await cTable.ExistsAsync())
                {
                    await cTable.CreateAsync();
                }

                DynamicTableEntity entity;

                var tableResult = await cTable.ExecuteAsync(TableOperation.Retrieve(table.Name, column.Name, new List<string>() { IncrementalValueName, LockGuidName }));
                if (tableResult.Result == null)
                {
                    entity = new DynamicTableEntity(table.Name, column.Name);
                    entity.Properties.Add(IncrementalValueName, new EntityProperty((long)1));
                    entity.Properties.Add(LockGuidName, new EntityProperty(lockGuid));
                    value = default(T);
                }
                else
                {
                    entity = tableResult.Result as DynamicTableEntity;
                    value = GetEntityProperty<T>(entity.Properties[IncrementalValueName]); 
                    entity.Properties[IncrementalValueName] = NewEntityProperty<T>(value);
                    entity.Properties[LockGuidName] = new EntityProperty(lockGuid);
                }
                    
                tableResult = await cTable.ExecuteAsync(TableOperation.Retrieve(table.Name, column.Name, new List<string>() { IncrementalValueName, LockGuidName }));
                entity = tableResult.Result as DynamicTableEntity;

                return value;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Azure Error getting incremental key for table {table.Name} {ex.Message}", ex);
            }
        }

        public override async Task UpdateMaxValue<T>(Table table, string columnName, T value, CancellationToken cancellationToken = default)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(IncrementalKeyTable);

                if (!await cTable.ExistsAsync())
                {
                    await cTable.CreateAsync();
                }

                DynamicTableEntity entity = null;
                entity = new DynamicTableEntity(table.Name, columnName);
                entity.Properties.Add(IncrementalValueName, NewEntityProperty<T>(value));
                entity.Properties.Add(LockGuidName, new EntityProperty(Guid.NewGuid()));

                //update the record with the new incremental value and the guid.
                await cTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Azure Error updating incremental key for table {table.Name} {ex.Message}", ex);
            }
        }

        private EntityProperty NewEntityProperty<T>(T value)
        {
            switch (value)
            {
                case short shortValue:
                    return new EntityProperty(shortValue);
                case int intValue:
                    return new EntityProperty(intValue);
                case long longValue:
                    return new EntityProperty(longValue);
                case ushort ushortValue:
                    return new EntityProperty(ushortValue);
                case uint uintValue:
                    return new EntityProperty(uintValue);
                case ulong ulongValue:
                    return new EntityProperty(ulongValue);
                case DateTime dateTimeValue:
                    return new EntityProperty(dateTimeValue);
                default:
                    throw new ConnectionException($"The datatype {value.GetType()} is not supported for incremental columns.  Use an integer type instead.");
            }
        }

        private T GetEntityProperty<T>(EntityProperty prop)
        {
            return (T)Convert.ChangeType(prop.PropertyAsObject, typeof(T));
        }

        public string ConvertOperator(ECompare Operator)
        {
            switch (Operator)
            {
                case ECompare.IsEqual:
                    return "eq";
                case ECompare.GreaterThan:
                    return "gt";
                case ECompare.GreaterThanEqual:
                    return "ge";
                case ECompare.LessThan:
                    return "lt";
                case ECompare.LessThanEqual:
                    return "le";
                case ECompare.NotEqual:
                    return "ne";
                default:
                    throw new Exception($"The operator {Operator} is not supported by Azure storage tables.");
            }
        }
        
        public string BuildFilterString(Filters filters)
        {
            if (filters == null || filters.Count == 0)
                return "";
            else
            {
                var combinedFilterString = "";

                foreach (var filter in filters)
                {
                    string filterString;

                    if (filter.Value2.GetType().IsArray)
                    {
                        var array = new List<object>();
                        foreach (var value in (Array)filter.Value2)
                        {
                            try
                            {
                                var valueParsed = Operations.Parse(filter.CompareDataType, value);
                                array.Add(valueParsed);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The filter value could not be converted to a {filter.CompareDataType}.  {ex.Message}", ex, value);
                            }
                        }
                        filterString = " (" + string.Join(" or ", array.Select(c => GenerateFilterCondition(filter.Column1.Name, ECompare.IsEqual, filter.CompareDataType, c))) + ")";
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
                            throw new ConnectionException($"The filter value could not be converted to a {filter.CompareDataType}.  {ex.Message}", ex, filter.Value2);
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

        private string GenerateFilterCondition(string column, ECompare filterOperator, ETypeCode compareDataType, object value)
        {
            string filterString;

            var operation = ConvertOperator(filterOperator);

            if (operation == null)
            {
                return null;
            }

            switch (compareDataType)
            {
                case ETypeCode.String:
				case ETypeCode.Text:
				case ETypeCode.Json:
                case ETypeCode.Node:
				case ETypeCode.Xml:
                case ETypeCode.Guid:
                case ETypeCode.Unknown:
                    filterString = TableQuery.GenerateFilterCondition(column, operation, (string)value);
                    break;
                case ETypeCode.Boolean:
                    filterString = TableQuery.GenerateFilterConditionForBool(column, operation, (bool)value);
                    break;
                case ETypeCode.Int16:
                case ETypeCode.Int32:
                case ETypeCode.UInt16:
                case ETypeCode.UInt32:
                    filterString = TableQuery.GenerateFilterConditionForInt(column, operation, (int)value);
                    break;
                case ETypeCode.UInt64:
                case ETypeCode.Int64:
                    filterString = TableQuery.GenerateFilterConditionForLong(column, operation, (long)value);
                    break;
                case ETypeCode.DateTime:
                case ETypeCode.Date:
                    filterString = TableQuery.GenerateFilterConditionForDate(column, operation, (DateTime)value);
                    break;
                case ETypeCode.Time:
                    filterString = TableQuery.GenerateFilterCondition(column, operation, value.ToString());
                    break;
                case ETypeCode.Double:
                case ETypeCode.Decimal:
                case ETypeCode.Single:
                    filterString = TableQuery.GenerateFilterConditionForDouble(column, operation, (double)value);
                    break;
                case ETypeCode.Binary:
                    filterString = TableQuery.GenerateFilterConditionForBinary(column, operation, (byte[])value);
                    break;
                default:
                    throw new Exception("The data type: " + compareDataType + " is not supported by Azure table storage.");
            }

            return filterString;
        }

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                return CreateTable(table, true, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The truncate table failed for {table.Name}.  {ex.Message}", ex);
            }
        }


        public override Task<Table> InitializeTable(Table table, int position, CancellationToken cancellationToken)
        {
            if (table.Columns.All(c => c.DeltaType != EDeltaType.PartitionKey))
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
                    DeltaType = EDeltaType.PartitionKey,
                    IsIncrementalUpdate = true,
                    IsMandatory = true
                });
            }

            if (table.Columns.All(c => c.DeltaType != EDeltaType.RowKey))
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
                    DeltaType = EDeltaType.RowKey,
                    IsMandatory = true
                });
            }

            if (table.Columns.All(c => c.DeltaType != EDeltaType.TimeStamp))
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
                    DeltaType = EDeltaType.TimeStamp,
                    IsMandatory = true
                });
            }

            return Task.FromResult(table);
        }

        private EntityProperty NewEntityProperty(TableColumn column, object value)
        {
            var (typeCode, returnValue) = ConvertForWrite(column, value);

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
                    return new EntityProperty(returnValue == null ? (long?) null : Convert.ToInt64(returnValue));
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
                case ETypeCode.Date:
                    return new EntityProperty((DateTime?)returnValue);
                case ETypeCode.Guid:
                    return new EntityProperty((Guid?)returnValue);
                case ETypeCode.Decimal:
                case ETypeCode.Unknown:
                    return new EntityProperty(returnValue?.ToString()); //decimal not supported, so convert to string
                case ETypeCode.Time:
                    return new EntityProperty(returnValue == null ? (long?) null : ((TimeSpan)value).Ticks); //timespan not supported, so convert to string.
                case ETypeCode.Binary:
                    return new EntityProperty((byte[])value);
                case ETypeCode.Geometry:
                    if(value == null)
                    {
                        return new EntityProperty((byte[])null);
                    }
                    return new EntityProperty(((Geometry)value).AsBinary());
                default:
                    throw new Exception("Cannot create new azure entity as the data type: " + typeCode + " is not supported.");
            }

        }

        public object ConvertEntityProperty(ETypeCode typeCode, object value)
        {
            if (value == null || value is DBNull)
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
                case ETypeCode.Date:
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
                case ETypeCode.Geometry:
                    var bytes = Operations.Parse<byte[]>(value);
                    var binReader = new WKBReader();
                    return binReader.Read(bytes);                
                default:
                    return value;
            }
        }


        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var rowCount = 0;

                var batchTasks = new List<Task>();

                long keyValue = -1;

                var dbAutoIncrement = table.GetColumn(EDeltaType.DbAutoIncrement);
                if (dbAutoIncrement != null)
                {
                    keyValue = await GetMaxValue<long>(table, dbAutoIncrement, cancellationToken);
                }

                long identityValue = 0;

                //start a batch operation to update the rows.
                var batchOperation = new TableBatchOperation();

                var partitionKey = table.GetColumn(EDeltaType.PartitionKey);
                var rowKey = table.GetColumn(EDeltaType.RowKey);

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Insert rows was cancelled.");
                    }

                    var properties = new Dictionary<string, EntityProperty>();
                    for (var i = 0; i < query.InsertColumns.Count; i++)
                    {
                        var field = query.InsertColumns[i];

                        if (field.Column.Name == "RowKey" || field.Column.Name == "PartitionKey" ||
                            field.Column.Name == "Timestamp" || field.Column.DeltaType == EDeltaType.DbAutoIncrement)
                        {
                            continue;
                        }

                        if (field.Column.DeltaType == EDeltaType.AutoIncrement)
                        {
                            identityValue = Convert.ToInt64(field.Value);
                        }

                        properties.Add(field.Column.Name, NewEntityProperty(table.Columns[field.Column], field.Value));
                    }

                    if (dbAutoIncrement != null)
                    {
                        identityValue = ++keyValue;
                        properties.Add(dbAutoIncrement.Name, NewEntityProperty(table.Columns[dbAutoIncrement], identityValue));
                    }

                    string partitionKeyValue = null;
                    if (partitionKey != null)
                    {
                        partitionKeyValue = query.InsertColumns.SingleOrDefault(c => c.Column.Name == partitionKey.Name)
                            ?.Value.ToString();
                    }

                    if (string.IsNullOrEmpty(partitionKeyValue)) partitionKeyValue = "default";

                    string rowKeyValue = null;
                    if (rowKey != null)
                        rowKeyValue = query.InsertColumns.SingleOrDefault(c => c.Column.Name == rowKey.Name)?.Value.ToString();

                    if (string.IsNullOrEmpty(rowKeyValue))
                    {
                        if (dbAutoIncrement == null)
                            rowKeyValue = Guid.NewGuid().ToString();
                        else
                            rowKeyValue = identityValue.ToString("D20");
                    }

                    var entity = new DynamicTableEntity(partitionKeyValue, rowKeyValue, "*", properties);

                    batchOperation.Insert(entity);

                    rowCount++;

                    if (rowCount > 99)
                    {
                        rowCount = 0;
                        batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation, null, null, cancellationToken = default));

                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new ConnectionException("Insert rows was cancelled.");
                        }

                        batchOperation = new TableBatchOperation();
                    }
                }
                
                if (keyValue > -1 && dbAutoIncrement != null)
                {
                    await UpdateMaxValue(table, dbAutoIncrement.Name, keyValue, cancellationToken);
                }

                if (batchOperation.Count > 0)
                {
                    batchTasks.Add(cTable.ExecuteBatchAsync(batchOperation));
                }

                await Task.WhenAll(batchTasks.ToArray());

                return identityValue;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The Azure insert query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var rowcount = 0;

                var batchTasks = new List<Task>();

                //start a batch operation to update the rows.
                var batchOperation = new TableBatchOperation();

                var surrogateKeyColumn = table.GetAutoIncrementColumn();

                //loop through all the queries to retrieve the rows to be updated.
                foreach (var query in queries)
                {
                    //Read the key fields from the table
                    var tableQuery = new TableQuery
                    {
                        //select all columns
                        SelectColumns = new[] { "PartitionKey", "RowKey" }.Concat(table.Columns.Where(c => c.Name != "PartitionKey" && c.Name != "RowKey").Select(c => c.Name)).ToList()
                    };

                    //the rowkey is the same as the surrogate key, so add this to the filter string if the surrogate key is used.
                    if (surrogateKeyColumn != null)
                    {
                        var filterCount = query.Filters.Count;
                        for (var i = 0; i < filterCount; i++)
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
                                        entity.Properties[column.Column.Name] = NewEntityProperty(col, column.Value);
                                        break;
                                }
                            }

                            batchOperation.Replace(entity);

                            rowcount++;

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

        public override async Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var connection = GetCloudTableClient();
                var cTable = connection.GetTableReference(table.Name);

                var rowcount = 0;

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

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
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
                throw new ConnectionException($"The Azure execute scalar query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
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
