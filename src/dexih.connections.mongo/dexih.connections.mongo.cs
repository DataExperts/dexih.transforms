using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Text.Json;
using dexih.transforms;
using System.Text.RegularExpressions;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace dexih.connections.mongo
{
    [Connection(
        ConnectionCategory = EConnectionCategory.NoSqlDatabase,
        Name = "MongoDB", 
        Description = "MongoDB is a general purpose, document-based, distributed database built for modern application developers and for the cloud era.",
        DatabaseDescription = "Database",
        ServerDescription = "MongoDb Server:Port",
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
    public class ConnectionMongo : Connection
    {
        public override bool CanBulkLoad => true;
        public override bool CanSort => true;
        public override bool CanFilter => true;
        public override bool CanDelete => true;
        public override bool CanUpdate => true;
        public override bool CanAggregate => false;
        public override bool CanUseBinary => true;
        public override bool CanUseArray => true;
        public override bool CanUseJson => true;
        public override bool CanUseXml => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseSql => false;
        public override bool CanUseDbAutoIncrement => true;
        public override bool DynamicTableCreation => true;
        public override bool CanUseGuid => false;
        public override bool CanUseUnsigned => false;
        public override bool CanUseTimeSpan => false;

        public const string IncrementalKeyTable = "_incrementalKeys";

//        public override object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
//        {
//            switch (typeCode)
//            {
//                case ETypeCode.DateTime:
//                    return new DateTime(1800, 01, 02, 0, 0, 0, 0, DateTimeKind.Utc);
//                case ETypeCode.Double:
//                    return -1E+100;
//                case ETypeCode.Single:
//                    return -1E+37F;
//                default:
//                    return DataType.GetDataTypeMinValue(typeCode, length);
//            }
//        }

        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
//                case ETypeCode.DateTime:
//                    return DateTime.MaxValue.ToUniversalTime();
                case ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
//                case ETypeCode.Double:
//                    return 1E+100;
//                case ETypeCode.Single:
//                    return 1E+37F;

                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }
        
        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();

                var filter = new BsonDocument("name", table.Name);
                //filter by collection name
                var collections = await database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter }, cancellationToken);
                //check for existence
                return await collections.AnyAsync(cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Could not check if collection exists.  {ex.Message}");
            }
        }

        private BsonDocument CreateDocumentRow(Table table, object[] row)
        {
            var document = new BsonDocument();

            var elements = new List<BsonElement>();
            for(var i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                var dictionary = row.ToDictionary(d => column.Name, d => CreateBsonValue(d, column, column.Rank));
                document.AddRange(dictionary);
            }

            return document;
        }

        private BsonValue CreateBsonValue(object value, TableColumn column, int rank)
        {
            var (convertedType, convertedValue) = ConvertForWrite(column.Name, column.DataType, rank, column.AllowDbNull, value);

            if (convertedValue == null || convertedValue is DBNull)
            {
                return BsonNull.Value;
            }

            if (rank >= 1)
            {
                var type = value.GetType();
                IEnumerable array;
                if (type.GetArrayRank() == 2)
                {
                    array = Operations.ConvertToJaggedArray((Array) value);
                }
                else
                {
                    array = ((IEnumerable) value);
                }

                var enumerator = array.GetEnumerator();
                var bsonValues = new List<BsonValue>();
                while (enumerator.MoveNext())
                {
                    bsonValues.Add(CreateBsonValue(enumerator.Current, column, rank - 1));
                }
                return new BsonArray(bsonValues);
            }
            
            switch (convertedType)
            {
                case ETypeCode.Unknown:
                    return BsonValue.Create(convertedValue);
                case ETypeCode.Binary:
                    return new BsonBinaryData((byte[])convertedValue);
                case ETypeCode.Byte:
                    return new BsonInt32((byte)convertedValue);
                case ETypeCode.Char:
                    return new BsonInt32((char)convertedValue);
                case ETypeCode.SByte:
                    return new BsonInt32((sbyte)convertedValue);
                case ETypeCode.Int16:
                    return new BsonInt32((short)convertedValue);
                case ETypeCode.Int32:
                    return new BsonInt32((int)convertedValue);
                case ETypeCode.Int64:
                    return new BsonInt64((long)convertedValue);
                case ETypeCode.Decimal:
                    return new BsonDecimal128((decimal)convertedValue);
                case ETypeCode.Double:
                    return new BsonDouble((double)convertedValue);
                case ETypeCode.Single:
                    return new BsonDouble((Single)convertedValue);
                case ETypeCode.String:
                case ETypeCode.Text:
                    return new BsonString((string)convertedValue);
                case ETypeCode.Boolean:
                    return new BsonBoolean((bool) convertedValue);
                case ETypeCode.DateTime:
                    return new BsonDateTime(((DateTime) convertedValue).ToUniversalTime());
                case ETypeCode.Json:
                    var json = ((JsonElement) convertedValue);
                    if (json.ValueKind == JsonValueKind.Array)
                    {
                        return BsonSerializer.Deserialize<BsonArray>(json.GetRawText());
                    }
                    else
                    {
                        return BsonSerializer.Deserialize<BsonDocument>(json.GetRawText());
                    }
                case ETypeCode.Xml:
                    return new BsonString((string)convertedValue);
                case ETypeCode.Enum:
                    return new BsonInt32((int)convertedValue);
                case ETypeCode.CharArray:
                    return new BsonString((string)convertedValue);
                case ETypeCode.Object:
                    return new BsonString((string)convertedValue);
                case ETypeCode.Geometry:
                    return new BsonBinaryData((byte[]) convertedValue);
                default:
                    throw new ArgumentOutOfRangeException(nameof(convertedType), convertedType, null);
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
                int skOrdinal = -1;
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
                        keyValue = await GetLastKey(table, sk, cancellationToken);
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
                    await UpdateIncrementalKey(table, sk.Name, keyValue, cancellationToken);
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                throw new ConnectionException("Error writing to Mongo collection: " + table.Name + ".  Error: " + ex.Message, ex);
            }
        }

        private Task WriteDataBuffer(Table table, IEnumerable<object[]> buffer, string targetTableName, CancellationToken cancellationToken = default)
        {
            var database = GetMongoDatabase();
            var collection = database.GetCollection<BsonDocument>(targetTableName);
            
            var surrogateKey = table.GetAutoIncrementOrdinal();
            var data = new List<BsonDocument>();

            foreach (var row in buffer)
            {
                var properties = new BsonDocument();
                for(var i = 0; i < table.Columns.Count; i++)
                {
                    var column = table.Columns[i];
                    var value = row[i];
                    if (value == DBNull.Value) value = null;
                    
                    var element = new BsonElement(column.Name, CreateBsonValue(value, column, column.Rank));
                    properties.Add(element);
                }

                data.Add(properties);
            }

            return collection.InsertManyAsync(data, cancellationToken: cancellationToken);
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
                    throw new ConnectionException("The table " + table.Name + " could not be created as it does not meet mongo table naming standards.");
                }

                foreach (var col in table.Columns)
                {
                    if (!IsValidColumnName(col.Name))
                    {
                        throw new ConnectionException("The table " + table.Name + " could not be created as the column " + col.Name + " does not meet mongo table naming standards.");
                    }
                }

                var database = GetMongoDatabase();
                var tableExists = await TableExists(table, cancellationToken);
                if (dropTable && tableExists)
                {
                    await database.DropCollectionAsync(table.Name, cancellationToken);
                }
                else if (tableExists)
                {
                    return;
                }
                
                await database.CreateCollectionAsync(table.Name, cancellationToken: cancellationToken);
                var collection = database.GetCollection<BsonDocument>(table.Name);

                foreach (var column in table.Columns.Where(c =>
                    c.DeltaType == EDeltaType.DbAutoIncrement || c.DeltaType == EDeltaType.AutoIncrement))
                {
                    await collection.Indexes.CreateOneAsync(Builders<BsonDocument>.IndexKeys.Ascending(column.Name));    
                }

                IndexKeysDefinition<BsonDocument> naturalKey = null;
                foreach (var column in table.Columns.Where(c => c.DeltaType == EDeltaType.NaturalKey))
                {
                    if (naturalKey == null)
                    {
                        naturalKey = Builders<BsonDocument>.IndexKeys.Ascending(column.Name);
                    }
                    else
                    {
                        naturalKey.Ascending(column.Name);
                    }     
                }

                if (naturalKey != null)
                {
                    var validFromDate = table.GetColumn(EDeltaType.ValidFromDate);
                    if (validFromDate != null)
                    {
                        naturalKey.Ascending(validFromDate.Name);
                    }

                    await collection.Indexes.CreateOneAsync(naturalKey);
                }
                
                // reset the auto incremental table, when rebuilding the table.
                var incremental = table.GetColumn(EDeltaType.DbAutoIncrement);
                if (incremental != null)
                {
                    await UpdateIncrementalKey(table, incremental.TableColumnName(), 0, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error creating mongo table {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Gets a connection refererence to the Mongo server.
        /// </summary>
        /// <returns></returns>
        private MongoClient GetMongoClient()
        {
            MongoClient client;

            if (UseConnectionString)
            {
                client = new MongoClient(ConnectionString);
            }
            else
            {
                if (string.IsNullOrEmpty(Username))
                {
                    client = new MongoClient($"mongodb://{Server}");
                }
                else
                {
                    client = new MongoClient($"mongodb://{Username}:{Password}@{Server}");    
                }
                
            }

            // Create the table client.
            return client;
        }

        private IMongoDatabase GetMongoDatabase()
        {
            var client = GetMongoClient();
            return client.GetDatabase(DefaultDatabase);
        }

        public async Task<IAsyncCursor<BsonDocument>> GetCollection(string name, SelectQuery query, CancellationToken cancellationToken)
        {
            var database = GetMongoDatabase();
            var collection = database.GetCollection<BsonDocument>(name);

            IFindFluent<BsonDocument, BsonDocument> find;
            if (query?.Filters != null && query.Filters.Count > 0)
            {
                find = collection.Find(BuildFilterDefinition(query.Filters));
            }
            else
            {
                find = collection.Find(new BsonDocument());
            }

            if (query?.Columns?.Count > 0)
            {
                find = find.Project(BuildProjectionDefinition(query.Columns));
            }

            if (query?.Sorts?.Count > 0)
            {
                find = find.Sort(BuildSortDefinition(query.Sorts));
            }

            return await find.ToCursorAsync(cancellationToken);
        }

        /// <summary>
        /// mongo does not have databases, so this is a dummy function.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            var client = GetMongoClient();
            client.GetDatabase(databaseName);
            DefaultDatabase = databaseName;
            return Task.CompletedTask;
        }

        /// <summary>
        /// mongo does not have databases, so this returns a dummy value.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            var client = GetMongoClient();
            List<string> dbs = new List<string>();
            using (IAsyncCursor<string> cursor = await client.ListDatabaseNamesAsync(cancellationToken))
            {
                while (await cursor.MoveNextAsync(cancellationToken))
                {
                    dbs.AddRange(cursor.Current.Select(doc => (doc)));
                }
            }

            return dbs;
        }

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();

                var collectionList = await database.ListCollectionsAsync(cancellationToken: cancellationToken);

                var list = new List<Table>();

                foreach (var collection in await collectionList.ToListAsync<BsonDocument>(cancellationToken: cancellationToken))
                {
                    list.Add(new Table(collection["name"].AsString));
                }
                
                return list;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error getting mongo collection list {ex.Message}", ex);
            }
        }

        public override async Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();


                //The new data table that will contain the table schema
                var table = new Table(originalTable.Name)
                {
                    LogicalName = originalTable.Name,
                    Description = ""
                };

                var collection = database.GetCollection<BsonDocument>(table.Name);

                var document = await collection.AsQueryable().Sample(1).FirstOrDefaultAsync(cancellationToken: cancellationToken);

                if (document != null)
                {
                    foreach (var element in document.Elements)
                    {
                        var (type, rank) = ConvertBsonType(element.Value);
                        //add the basic properties                            
                        var col = new TableColumn()
                        {
                            Name = element.Name,
                            LogicalName = element.Name,
                            IsInput = false,
                            DataType = type,
                            Rank = rank,
                            Description = "",
                            AllowDbNull = true,
                            IsUnique = false
                        };

                        if (element.Value.BsonType == BsonType.ObjectId)
                        {
                            col.MaxLength = element.Value.BsonType == BsonType.ObjectId ? 14 : (int?) null;
                            col.DeltaType = EDeltaType.RowKey;

                        }
                        table.Columns.Add(col);
                    }
                }
                
                return table;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error getting mongo table information for table {originalTable.Name}.  {ex.Message}", ex);
            }
        }

        private (ETypeCode typeCode, int rank) ConvertBsonType(BsonValue bsonValue)
        {
            switch (bsonValue.BsonType)
            {
                case BsonType.Double:
                    return (ETypeCode.Double, 0);
                case BsonType.String:
                    return (ETypeCode.String, 0);
                case BsonType.Document:
                    return (ETypeCode.Json, 0);
                case BsonType.Array:
                    var array = bsonValue.AsBsonArray;
                    if (array.Count == 0)
                    {
                        return (ETypeCode.String, 1);
                    }
                    else
                    {
                        var result = ConvertBsonType(array[0]);
                        return (result.typeCode, result.rank + 1);
                    }
                case BsonType.Binary:
                    return (ETypeCode.Binary, 0);
                case BsonType.ObjectId:
                    return (ETypeCode.String, 0);
                case BsonType.Boolean:
                    return (ETypeCode.Boolean, 0);
                case BsonType.DateTime:
                    return (ETypeCode.DateTime, 0);
                case BsonType.Null:
                    return (ETypeCode.String, 0);
                case BsonType.Int32:
                    return (ETypeCode.Int32, 0);
                case BsonType.Timestamp:
                    return (ETypeCode.Int64, 0);
                case BsonType.Int64:
                    return (ETypeCode.Int64, 0);
                case BsonType.Decimal128:
                    return (ETypeCode.Decimal, 0);
                default:
                    throw new ArgumentOutOfRangeException(nameof(bsonValue.BsonType), bsonValue.BsonType, null);
            }
        }

        /// <summary>
        /// mongo can always return true for CompareTable, as the columns are not created in the same way relational tables are.
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
        /// <param name="incrementalColumn"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task<long> GetLastKey(Table table, TableColumn incrementalColumn, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();
                var collection = database.GetCollection<BsonDocument>(IncrementalKeyTable);

                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("tableName", table.Name) & filterBuilder.Eq("columnName", incrementalColumn.Name);
                var document = await (await collection.FindAsync(filter, cancellationToken: cancellationToken)).FirstOrDefaultAsync(cancellationToken: cancellationToken);

                if (document == null)
                {
                    return 0;
                }
                else
                {
                    var value = document["value"].ToInt64();
                    return value;
                }

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Mongo Error getting incremental key for table {table.Name} {ex.Message}", ex);
            }
        }

        public override async Task UpdateIncrementalKey(Table table, string incrementalColumnName, object value, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();
                var collection = database.GetCollection<BsonDocument>(IncrementalKeyTable);

                var filterBuilder = Builders<BsonDocument>.Filter;
                var filter = filterBuilder.Eq("tableName", table.Name) & filterBuilder.Eq("columnName", incrementalColumnName);
                var document = await (await collection.FindAsync(filter, cancellationToken: cancellationToken)).FirstOrDefaultAsync(cancellationToken: cancellationToken);

                if (document == null)
                {
                    var insertDocument = new BsonDocument()
                    {
                        {"tableName", table.Name},
                        {"columnName", incrementalColumnName},
                        {"value", 1}
                    };
                    await collection.InsertOneAsync(insertDocument, cancellationToken: cancellationToken);
                }
                else
                {
                    var update = Builders<BsonDocument>.Update.Set("value", value);
                    await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Mongo Error updating incremental key for table {table.Name} {ex.Message}", ex);
            }
        }

        public ProjectionDefinition<BsonDocument> BuildProjectionDefinition(List<SelectColumn> selectColumns)
        {
            if (selectColumns == null || selectColumns.Count == 0)
                return null;
            
            ProjectionDefinition<BsonDocument> projectionDefinition = null;
            foreach (var selectColumn in selectColumns)
            {
                if (projectionDefinition == null)
                {
                    projectionDefinition = Builders<BsonDocument>.Projection.Include(selectColumn.Column.Name);
                }
                else
                {
                    projectionDefinition = projectionDefinition.Include(selectColumn.Column.Name);
                }
            }

            return projectionDefinition;
        }
        
        public SortDefinition<BsonDocument> BuildSortDefinition(Sorts sortColumns)
        {
            if (sortColumns == null || sortColumns.Count == 0)
                return null;
            
            SortDefinition<BsonDocument> sortDefinition = null;
            foreach (var sortColumn in sortColumns)
            {
                if (sortDefinition == null)
                {
                    if (sortColumn.Direction == Sort.EDirection.Ascending)
                    {
                        sortDefinition = Builders<BsonDocument>.Sort.Ascending(sortColumn.Column.Name);    
                    }
                    else
                    {
                        sortDefinition = Builders<BsonDocument>.Sort.Descending(sortColumn.Column.Name);    
                    }
                }
                else
                {
                    if (sortColumn.Direction == Sort.EDirection.Ascending)
                    {
                        sortDefinition = sortDefinition.Ascending(sortColumn.Column.Name);    
                    }
                    else
                    {
                        sortDefinition = sortDefinition.Descending(sortColumn.Column.Name);    
                    }
                }
            }

            return sortDefinition;
        }
                
        public FilterDefinition<BsonDocument> BuildFilterDefinition(List<Filter> filters)
        {
            if (filters == null || filters.Count == 0)
                return null;
            else
            {
                var filterDefinitions = new List<FilterDefinition<BsonDocument>>();

                foreach (var filter in filters)
                {
                    FilterDefinition<BsonDocument> filterDefinition;

                    if (filter.Value2.GetType().IsArray)
                    {
                        var array = new List<object>();
                        foreach (var value in (Array)filter.Value2)
                        {
                            try
                            {
                                var valueParse = Operations.Parse(filter.CompareDataType, value);
                                array.Add(valueParse);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The filter value could not be converted to a {filter.CompareDataType}.  {ex.Message}", ex, value);
                            }
                        }

                        var filtersArray = array.Select(c =>
                            GenerateFilterCondition(filter.Column1.Name, ECompare.IsEqual, filter.CompareDataType, c));
                        filterDefinition = Builders<BsonDocument>.Filter.Or(filtersArray);
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

                        filterDefinition = GenerateFilterCondition(filter.Column1.Name, filter.Operator, filter.CompareDataType, value2);
                    }

                    filterDefinitions.Add(filterDefinition);
                }

                var definitions = Builders<BsonDocument>.Filter.And(filterDefinitions);
                return definitions;

            }
        }

        private FilterDefinition<BsonDocument> GenerateFilterCondition(string column, ECompare filterOperator, ETypeCode compareDataType, object value)
        {
            switch (filterOperator)
            {
                case ECompare.IsEqual:
                    return Builders<BsonDocument>.Filter.Eq(column, value);
                case ECompare.GreaterThan:
                    return Builders<BsonDocument>.Filter.Gt(column, value);
                case ECompare.GreaterThanEqual:
                    return Builders<BsonDocument>.Filter.Gte(column, value);
                case ECompare.LessThan:
                    return Builders<BsonDocument>.Filter.Lt(column, value);
                case ECompare.LessThanEqual:
                    return Builders<BsonDocument>.Filter.Lte(column, value);
                case ECompare.NotEqual:
                    return Builders<BsonDocument>.Filter.Ne(column, value);
                case ECompare.IsNull:
                    return Builders<BsonDocument>.Filter.Eq(column, BsonNull.Value);
                case ECompare.IsNotNull:
                    return Builders<BsonDocument>.Filter.Ne(column, BsonNull.Value);
                case ECompare.Like:
                    return Builders<BsonDocument>.Filter.Regex(column, new BsonRegularExpression((string) value));
                default:
                    throw new ArgumentOutOfRangeException(nameof(filterOperator), filterOperator, null);
            }

        }

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                return CreateTable(table, true, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The truncate collection failed for {table.Name}.  {ex.Message}", ex);
            }
        }


        public override Task<Table> InitializeTable(Table table, int position)
        {
            return Task.FromResult(table);
        }
        
        
        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();
                var collection = database.GetCollection<BsonDocument>(table.Name);
                var data = new List<BsonDocument>();
                
                long keyValue = -1;

                var dbAutoIncrement = table.GetColumn(EDeltaType.DbAutoIncrement);
                if (dbAutoIncrement != null)
                {
                    keyValue = await GetLastKey(table, dbAutoIncrement, cancellationToken);
                }

                long identityValue = 0;
                
                foreach (var query in queries)
                {
                    var properties = new BsonDocument();
                    foreach(var insertColumn in query.InsertColumns)
                    {
                        var column = insertColumn.Column;
                        var value = insertColumn.Value;
                        if (value == DBNull.Value) value = null;
                    
                        if (insertColumn.Column.DeltaType == EDeltaType.AutoIncrement)
                        {
                            identityValue = Convert.ToInt64(insertColumn.Value);
                        }
                        
                        var element = new BsonElement(column.Name, CreateBsonValue(value, column, column.Rank));
                        properties.Add(element);
                    }
                    
                    if (dbAutoIncrement != null)
                    {
                        identityValue = keyValue++;
                        var element = new BsonElement(dbAutoIncrement.Name, new BsonInt64(identityValue));
                        properties.Add(element);
                    }

                    data.Add(properties);
                }
                
                if (keyValue > -1 && dbAutoIncrement != null)
                {
                    await UpdateIncrementalKey(table, dbAutoIncrement.Name, keyValue, cancellationToken);
                }
                
                collection.InsertManyAsync(data, cancellationToken: cancellationToken);
                return identityValue;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The mongo insert query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();
                var collection = database.GetCollection<BsonDocument>(table.Name);

                foreach (var query in queries)
                {
                    var filterDefinition = BuildFilterDefinition(query.Filters);

                    var updates = query.UpdateColumns.Select(c =>
                    {
                        var value = CreateBsonValue(c.Value, c.Column, c.Column.Rank);
                        return Builders<BsonDocument>.Update.Set(c.Column.Name, value);
                    });
                    var update = Builders<BsonDocument>.Update.Combine(updates);
                    await collection.UpdateManyAsync(filterDefinition, update, cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The mongo update query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var database = GetMongoDatabase();
                var collection = database.GetCollection<BsonDocument>(table.Name);

                foreach (var query in queries)
                {
                    var filterDefinition = BuildFilterDefinition(query.Filters);
                    collection.DeleteMany(filterDefinition);
                }

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The mongo delete query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
            try
            {
                var documents = await GetCollection(table.Name, query, cancellationToken);
                var document = await documents.FirstOrDefaultAsync(cancellationToken: cancellationToken);

                if (document == null)
                {
                    return null;
                }

                return BsonTypeMapper.MapToDotNetValue(document[1]); // get element 1 (element 0 is id field)
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The Mongo scalar query for {table.Name} failed.  { ex.Message} ", ex);
            }
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("A native database reader is not available for mongo table connections.");
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderMongo(this, table);
            return reader;
        }

    }
}
