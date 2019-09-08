using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.sql;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;
using Npgsql;
using NpgsqlTypes;

namespace dexih.connections.postgressql
{
    [Connection(
        ConnectionCategory = EConnectionCategory.SqlDatabase,
        Name = "PostgreSQL", 
        Description = "PostgreSQL (Postgres), is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards compliance",
        DatabaseDescription = "Database Name",
        ServerDescription = "Server:Port Name",
        AllowsConnectionString = true,
        AllowsSql = true,
        AllowsFlatFiles = false,
        AllowsManagedConnection = true,
        AllowsSourceConnection = true,
        AllowsTargetConnection = true,
        AllowsUserPassword = true,
        AllowsWindowsAuth = true,
        RequiresDatabase = true,
        RequiresLocalStorage = false
    )]
    public class ConnectionPostgreSql : ConnectionSql
    {
        public override bool CanUseArray => true;
        public override bool CanUseCharArray => true;
        public override bool CanUseUnsigned => false;
        public override bool AllowsTruncate { get; } = true;



        public override object GetConnectionMaxValue(DataType.ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case DataType.ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
                case DataType.ETypeCode.DateTime:
                    return new DateTime(9999, 12, 31, 23, 59, 59, 999);
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }
        
        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
        {
            try
            {
                var copyCommand = new StringBuilder();
                copyCommand.Append($"COPY {SqlTableName(table)} (");

                var columns = table.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.DbAutoIncrement).ToArray();
                var ordinals = new int[columns.Length];
                var types = new NpgsqlDbType[columns.Length];
                    
                for(var i = 0; i< columns.Length; i++)
                {
                    ordinals[i] = reader.GetOrdinal(columns[i].Name);
                    types[i] = GetTypeCodeDbType(columns[i].DataType, columns[i].Rank);
                    if (ordinals[i] >= 0)
                    {
                        copyCommand.Append(AddDelimiter(columns[i].Name) + (i == columns.Length - 1 ? "" : ","));
                    }
                }

                copyCommand.Append(") FROM STDIN (FORMAT BINARY)");

                using (var connection = (NpgsqlConnection) await NewConnection())
                using (var writer = connection.BeginBinaryImport(copyCommand.ToString()))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        writer.StartRow();

                        for(var i = 0; i< columns.Length; i++)
                        {
                            try
                            {

                                if (ordinals[i] >= 0)
                                {
                                    var value = reader[ordinals[i]];

                                    if (value == null || value == DBNull.Value)
                                    {
                                        writer.WriteNull();
                                    }
                                    else
                                    {
                                        writer.Write(value, types[i]);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
#if DEBUG
                                throw new ConnectionException($"Column {columns[i].Name}, value {reader[ordinals[i]]}.  {ex.Message}", ex);
#else
                                throw new ConnectionException($"Column {columns[i].Name}.  {ex.Message}", ex);
#endif
                            }
                        }
                    }

                    writer.Complete();
                }
            }
            catch (PostgresException ex)
            {
                throw new ConnectionException($"Postgres bulk insert into table {table.Name} failed. {ex.Message} at {ex.Where}", ex);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Postgres bulk insert into table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "select table_name from information_schema.tables where table_name = @NAME"))
                {
                    cmd.Parameters.Add(CreateParameter(cmd, "@NAME", DataType.ETypeCode.Text, ParameterDirection.Input, table.Name));

                    var tableExists = await cmd.ExecuteScalarAsync(cancellationToken);
                    return tableExists != null;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Table exists for {table.Name} failed. {ex.Message}", ex);
            }
        }

        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <returns></returns>
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
            try
            {
                var tableExists = await TableExists(table, cancellationToken);

                //if table exists, and the dropTable flag is set to false, then error.
                if (tableExists && dropTable == false)
                {
                    throw new ConnectionException("The table already exists on the database.  Drop the table first.");
                }

                //if table exists, then drop it.
                if (tableExists)
                {
                    await DropTable(table);
                }

                var createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + AddDelimiter(table.Name) + " ( ");
                foreach (var col in table.Columns)
                {
                    if (col.DeltaType == TableColumn.EDeltaType.DbAutoIncrement)
                        createSql.Append(AddDelimiter(col.Name) + " BIGSERIAL"); //TODO autoincrement for postgresql
                    else
                    {
                        createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col));
                        if (col.AllowDbNull == false)
                        {
                            createSql.Append(" NOT NULL");
                        }
                        else
                        {
                            createSql.Append(" NULL");
                        }
                    }
                    createSql.Append(",");
                }

                //Add the primary key using surrogate key or autoincrement.
                var key = table.GetAutoIncrementColumn();

                if (key != null)
                {
                    createSql.Append("CONSTRAINT \"PK_" + AddEscape(table.Name) + "\" PRIMARY KEY (" +
                                     AddDelimiter(key.Name) + "),");
                }


                //remove the last comma
                createSql.Remove(createSql.Length - 1, 1);
                createSql.Append(")");

                using (var connection = await NewConnection())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = createSql.ToString();
                    try
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"The sql query failed [{command.CommandText}].  {ex.Message}", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create table for {table.Name} failed. {ex.Message}", ex);
            }
        }

        protected override string GetSqlType(TableColumn column)
        {
            string sqlType;

            switch (column.DataType)
            {
                case DataType.ETypeCode.Int32:
                case DataType.ETypeCode.UInt16:
                    sqlType = "int";
                    break;
                case DataType.ETypeCode.Byte:
                case DataType.ETypeCode.Char:
                case DataType.ETypeCode.Int16:
                case DataType.ETypeCode.SByte:
                    sqlType = "smallint";
                    break;
                case DataType.ETypeCode.Int64:
                case DataType.ETypeCode.UInt32:
                case DataType.ETypeCode.UInt64:
                    sqlType = "bigint";
                    break;
                case DataType.ETypeCode.String:
                    if (column.MaxLength == null)
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(10485760)";
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(" + column.MaxLength + ")";
                    break;
                case DataType.ETypeCode.CharArray:
                    if(column.MaxLength == null) 
                        sqlType = (column.IsUnicode == true ? "n" : "") + "char(0)";
                    else 
                        sqlType= (column.IsUnicode == true ? "n" : "") + "char(" + column.MaxLength + ")";
                    break;
				case DataType.ETypeCode.Text:
                    sqlType = (column.IsUnicode == true ? "n" : "") + "text";
                    break;
                case DataType.ETypeCode.Json:
                case DataType.ETypeCode.Node:
                    sqlType = "json";
                    break;
                case DataType.ETypeCode.Xml:
                    sqlType = "xml";
                    break;
                case DataType.ETypeCode.Single:
                    sqlType = "real";
                    break;
                case DataType.ETypeCode.Double:
                    sqlType = "double precision";
                    break;
                case DataType.ETypeCode.Boolean:
                    sqlType = "bool";
                    break;
                case DataType.ETypeCode.DateTime:
                    sqlType = "timestamp";
                    break;
                case DataType.ETypeCode.Time:
                    sqlType = "time";
                    break;
                case DataType.ETypeCode.Guid:
                    sqlType = "text";
                    break;
                case DataType.ETypeCode.Binary:
                    sqlType = "bytea";
                    break;
                //case TypeCode.TimeSpan:
                //    SQLType = "time(7)";
                //    break;
                case DataType.ETypeCode.Unknown:
                    sqlType = "varchar(10485760)";
                    break;
                case DataType.ETypeCode.Decimal:
                    sqlType =  $"numeric ({column.Precision??28}, {column.Scale??0})";
                    break;
                default:
                    throw new Exception($"The datatype {column.DataType} is not compatible with the create table.");
            }

            if (column.Rank > 0)
            {
                return sqlType + string.Concat(Enumerable.Repeat("[]", column.Rank));
            }

            return sqlType;
        }


        public override async Task<DbConnection> NewConnection()
        {
            NpgsqlConnection connection = null;

            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
                    if (string.IsNullOrEmpty(Server))
                    {
                        throw new ConnectionException("There was no server name specified.");
                    }
                    
                    var hostport = Server.Split(':');
                    string port;
                    var host = hostport[0];
                    if (hostport.Count() == 1)
                    {
                        port = "5432";
                    } else
                    {
                        port = hostport[1];
                    }

                    if (UseWindowsAuth == false)
                        connectionString = "Host=" + host + "; Port=" + port + "; User Id=" + Username + "; Password=" + Password + "; ApplicationName=dexih; ";
                    else
                        connectionString = "Host=" + host + "; Port=" + port + "; Integrated Security=true; ApplicationName=dexih; ";

                    if (!string.IsNullOrEmpty(DefaultDatabase))
                    {
                        connectionString += "Database=" + DefaultDatabase;
                    }
                    else
                    {
                        connectionString += "Database=postgres";
                    }
                }

                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    throw new ConnectionException($"Postgres connection status is {connection.State}.");

                }

                return connection;
            }
            catch (Exception ex)
            {
                if (connection != null)
                    connection.Dispose();
                throw new ConnectionException($"Postgres connection failed. {ex.Message}", ex);
            }
        }

        public override async Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            try
            {
                DefaultDatabase = "";
                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "create database " + AddDelimiter(databaseName)))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
                
                DefaultDatabase = databaseName;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create database {databaseName} failed. {ex.Message}", ex);
            }
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            try
            {
                var list = new List<string>();

                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "SELECT datname FROM pg_database WHERE datistemplate = false order by datname"))
                using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        list.Add((string)reader["datname"]);
                    }
                }
                return list;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get database list failed. {ex.Message}", ex);
            }
        }

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            try
            {
                var tableList = new List<Table>();

                using (var connection = await NewConnection())
                {

                    using (var cmd = CreateCommand(connection, "select table_catalog, table_schema, table_name, table_type from information_schema.tables where table_schema not in ('pg_catalog', 'information_schema')"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            var table = new Table
                            {
                                Name = reader["table_name"].ToString(),
                                Schema = reader["table_schema"].ToString(),
                                TableType = reader["table_type"].ToString() == "VIEW" ? Table.ETableType.View : Table.ETableType.Table
                            };
                            tableList.Add(table);
                        }
                    }

                }
                return tableList;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get table list failed. {ex.Message}", ex);
            }
        }

        public override async Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken = default)
        {
            try
            {
                if (originalTable.UseQuery)
                {
                    return await GetQueryTable(originalTable, cancellationToken);
                }

                var schema = string.IsNullOrEmpty(originalTable.Schema) ? "public" : originalTable.Schema;
                var table = new Table(originalTable.Name, originalTable.Schema);

                using (var connection = await NewConnection())
                {

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    
                    // get the table type
                    using (var cmd = CreateCommand(connection, "select table_type from information_schema.tables where table_name = @NAME"))
                    {
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", DataType.ETypeCode.Text, ParameterDirection.Input, table.Name));

                        var tableType = await cmd.ExecuteScalarAsync(cancellationToken);

                        if (tableType == null)
                        {
                            throw new ConnectionException($"The table {table.Name} could not be found.");
                        }

                        if (tableType.ToString() == "VIEW")
                        {
                            table.TableType = Table.ETableType.View;
                        }
                        else
                        {
                            table.TableType = Table.ETableType.Table;
                        }
                    }

                    List<string> pkColumns = new List<string>();

                    // get primary key columns
                    using(var cmd = CreateCommand(connection, $@"
SELECT
c.column_name
FROM
information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
where constraint_type = 'PRIMARY KEY' and constraint_schema='{schema}' and tc.table_name = '{table.Name}'"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            pkColumns.Add(reader.GetString(0));
                        }
                    }

                    // The schema table 
                    using (var cmd = CreateCommand(connection, $@"
SELECT c.column_name, c.data_type, c.character_maximum_length, c.numeric_precision_radix, c.numeric_scale, c.is_nullable, e.data_type AS element_type
FROM information_schema.columns c LEFT JOIN information_schema.element_types e
     ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
       = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
WHERE c.table_schema = '{schema}' AND c.table_name = '{table.Name}'
ORDER BY c.ordinal_position"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {

                        while (await reader.ReadAsync(cancellationToken))
                        {
                            var col = new TableColumn
                            {
                                //add the basic properties
                                Name = reader["column_name"].ToString(),
                                LogicalName = reader["column_name"].ToString(),
                                IsInput = false,
                                Rank = (string)reader["data_type"] == "ARRAY" ? 1 : 0,
                                DataType = (string)reader["data_type"] == "ARRAY" ? ConvertSqlToTypeCode(reader["element_type"].ToString()) : ConvertSqlToTypeCode(reader["data_type"].ToString())
                            };

                            if (col.DataType == DataType.ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                if (pkColumns.Contains(col.Name))
                                {
                                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                                }
                                else
                                {
                                    col.DeltaType = TableColumn.EDeltaType.TrackingField;
                                }
                            }

                            if (col.DataType == DataType.ETypeCode.String || col.DataType == DataType.ETypeCode.CharArray)
                            {
                                col.MaxLength = ConvertNullableToInt(reader["character_maximum_length"]);
                            }
                            else if (col.DataType == DataType.ETypeCode.Double || col.DataType == DataType.ETypeCode.Decimal)
                            {
                                col.Precision = ConvertNullableToInt(reader["numeric_precision_radix"]);
                                col.Scale = ConvertNullableToInt(reader["numeric_scale"]);
                            }

                            //make anything with a large string unlimited.  This will be created as varchar(max)
                            if (col.MaxLength > 4000)
                                col.MaxLength = null;

                            //col.Description = reader["Description"].ToString();
                            col.AllowDbNull = Convert.ToBoolean((string)reader["is_nullable"] == "YES");
                            //col.IsUnique = Convert.ToBoolean(reader["IsUnique"]);


                            table.Columns.Add(col);
                        }
                    }
                }
                return table;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get source table information for {originalTable.Name} failed. {ex.Message}", ex);
            }
        }

        private int? ConvertNullableToInt(object value)
        {
            if (value == null || value is DBNull)
            {
                return null;
            }

            var parsed = int.TryParse(value.ToString(), out var result);
            if (parsed)
            {
                return result;
            }

            return null;
        }


        public DataType.ETypeCode ConvertSqlToTypeCode(string sqlType)
        {
            switch (sqlType)
            {
                case "bit": return DataType.ETypeCode.Boolean;
                case "varbit": return DataType.ETypeCode.Binary;
                case "bytea": return DataType.ETypeCode.Binary;
                case "smallint": return DataType.ETypeCode.Int16;
                case "int": return DataType.ETypeCode.Int32;
                case "integer": return DataType.ETypeCode.Int32;
                case "bigint": return DataType.ETypeCode.Int64;
                case "smallserial": return DataType.ETypeCode.Int16;
                case "serial": return DataType.ETypeCode.Int32;
                case "bigserial": return DataType.ETypeCode.Int64;
                case "numeric": return DataType.ETypeCode.Decimal;
                case "double precision": return DataType.ETypeCode.Double;
                case "real": return DataType.ETypeCode.Double;
                case "money": return DataType.ETypeCode.Decimal;
                case "bool": return DataType.ETypeCode.Boolean;
                case "boolean": return DataType.ETypeCode.Boolean;
                case "date": return DataType.ETypeCode.DateTime;
                case "timestamp": return DataType.ETypeCode.DateTime;
                case "timestamp without time zone": return DataType.ETypeCode.DateTime;
                case "timestamp with time zone": return DataType.ETypeCode.DateTime;
                case "interval": return DataType.ETypeCode.Time;
                case "time": return DataType.ETypeCode.Time;
                case "time without time zone": return DataType.ETypeCode.Time;
                case "time with time zone": return DataType.ETypeCode.Time;
                case "character varying": return DataType.ETypeCode.String;
                case "varchar": return DataType.ETypeCode.String;
                case "character": return DataType.ETypeCode.CharArray;
                case "text": return DataType.ETypeCode.Text;
                case "json": return DataType.ETypeCode.Json;
                case "xml": return DataType.ETypeCode.Xml;
            }
            return DataType.ETypeCode.Unknown;
        }


        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                if (queries.Count == 0) return 0;
                
                long identityValue = 0;
                long autoIncrementValue = 0;

                var transactionConnection = await GetTransaction(transactionReference);
                var connection = (NpgsqlConnection) transactionConnection.connection;
                var transaction = (NpgsqlTransaction) transactionConnection.transaction;

                try
                {
                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    foreach (var query in queries)
                    {
                        insert.Clear();
                        values.Clear();

                        insert.Append("INSERT INTO " + AddDelimiter(table.Name) + " (");
                        values.Append("VALUES (");

                        for (var i = 0; i < query.InsertColumns.Count; i++)
                        {
                            if (query.InsertColumns[i].Column.DeltaType == TableColumn.EDeltaType.DbAutoIncrement)
                                continue;
                            
                            if (query.InsertColumns[i].Column.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                                autoIncrementValue = Convert.ToInt64(query.InsertColumns[i].Value);
                            
                            insert.Append(AddDelimiter(query.InsertColumns[i].Column.Name) + ",");
                            values.Append("@col" + i + ",");
                        }

                        var insertCommand = insert.Remove(insert.Length - 1, 1) + ") " +
                                            values.Remove(values.Length - 1, 1) + "); ";

                        try
                        {
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = insertCommand;
                                cmd.Transaction = transaction;

                                for (var i = 0; i < query.InsertColumns.Count; i++)
                                {
                                    var param = cmd.CreateParameter();
                                    param.ParameterName = "@col" + i;
                                    param.NpgsqlDbType = GetTypeCodeDbType(query.InsertColumns[i].Column.DataType,
                                        query.InsertColumns[i].Column.Rank);
                                    param.Size = -1;
                                    param.NpgsqlValue = ConvertForWrite(query.InsertColumns[i].Column,
                                        query.InsertColumns[i].Value);
                                    cmd.Parameters.Add(param);
                                }
                                
                                cancellationToken.ThrowIfCancellationRequested();

                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                            
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Insert query failed.  {ex.Message}", ex);
                        }
                    }

                    if (autoIncrementValue > 0)
                    {
                        return autoIncrementValue;
                    }

                    var deltaColumn = table.GetColumn(TableColumn.EDeltaType.DbAutoIncrement);
                    if (deltaColumn != null)
                    {
                        var sql = $" select max({AddDelimiter(deltaColumn.Name)}) from {AddDelimiter(table.Name)}";
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.CommandText = sql;
                            cmd.Transaction = transaction;
                            var identity = await cmd.ExecuteScalarAsync(cancellationToken);
                            identityValue = Convert.ToInt64(identity);
                        }
                    }
                    
                    return identityValue; //sometimes reader returns -1, when we want this to be error condition.

                }
                finally
                {
                    EndTransaction(transactionReference, transactionConnection);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert into table {table.Name} failed. {ex.Message}", ex);
            }

        }

        private NpgsqlDbType GetTypeCodeDbType(DataType.ETypeCode typeCode, int rank)
        {
            if (rank > 0)
            {
                return NpgsqlDbType.Array | GetTypeCodeDbType(typeCode, 0);
            }
            
            switch (typeCode)
            {
                case DataType.ETypeCode.Byte:
                    return NpgsqlDbType.Smallint;
                case DataType.ETypeCode.Char:
                    return NpgsqlDbType.Smallint;
                case DataType.ETypeCode.SByte:
                    return NpgsqlDbType.Smallint;
                case DataType.ETypeCode.UInt16:
                    return NpgsqlDbType.Integer;
                case DataType.ETypeCode.UInt32:
                    return NpgsqlDbType.Bigint;
                case DataType.ETypeCode.UInt64:
                    return NpgsqlDbType.Bigint;
                case DataType.ETypeCode.Int16:
                    return NpgsqlDbType.Smallint;
                case DataType.ETypeCode.Int32:
                    return NpgsqlDbType.Integer;
                case DataType.ETypeCode.Int64:
                    return NpgsqlDbType.Bigint;
                case DataType.ETypeCode.Decimal:
                    return NpgsqlDbType.Numeric;
                case DataType.ETypeCode.Double:
                    return NpgsqlDbType.Double;
                case DataType.ETypeCode.Single:
                    return NpgsqlDbType.Real;
                case DataType.ETypeCode.String:
                    return NpgsqlDbType.Varchar;
				case DataType.ETypeCode.Text:
				    return NpgsqlDbType.Text;
                case DataType.ETypeCode.Boolean:
                    return NpgsqlDbType.Boolean;
                case DataType.ETypeCode.DateTime:
                    return NpgsqlDbType.Timestamp;
                case DataType.ETypeCode.Time:
                    return NpgsqlDbType.Time;
                case DataType.ETypeCode.Guid:
                    return NpgsqlDbType.Varchar;
                case DataType.ETypeCode.Binary:
                    return NpgsqlDbType.Bytea;
                case DataType.ETypeCode.Json:
                case DataType.ETypeCode.Node:
                    return NpgsqlDbType.Json;
                case DataType.ETypeCode.Xml:
                    return NpgsqlDbType.Xml;
                case DataType.ETypeCode.CharArray:
                    return NpgsqlDbType.Char;
                default:
                    return NpgsqlDbType.Varchar;
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var transactionConnection = await GetTransaction(transactionReference);
                var connection = (NpgsqlConnection) transactionConnection.connection;
                var transaction = (NpgsqlTransaction) transactionConnection.transaction;

                try
                {
                    var sql = new StringBuilder();

                    foreach (var query in queries)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        sql.Clear();

                        sql.Append("update " + AddDelimiter(table.Name) + " set ");

                        var count = 0;
                        foreach (var column in query.UpdateColumns)
                        {
                            sql.Append(AddDelimiter(column.Column.Name) + " = @col" + count +
                                       ","); // cstr(count)" + GetSqlFieldValueQuote(column.Column.DataType, column.Value) + ",");
                            count++;
                        }

                        sql.Remove(sql.Length - 1, 1); //remove last comma

                        //  Retrieving schema for columns from a single table
                        using (var cmd = connection.CreateCommand())
                        {
                            sql.Append(" " + BuildFiltersString(query.Filters, cmd) + ";");

                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            for (var i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                var param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i;
                                param.NpgsqlDbType = GetTypeCodeDbType(query.UpdateColumns[i].Column.DataType,
                                    query.UpdateColumns[i].Column.Rank);
                                param.Size = -1;
                                param.Value = ConvertForWrite(query.UpdateColumns[i].Column,
                                    query.UpdateColumns[i].Value);
                                cmd.Parameters.Add(param);
                            }

                            try
                            {
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The update query failed.  {ex.Message}", ex);
                            }
                        }
                    }
                }
                finally
                {
                    EndTransaction(transactionReference, transactionConnection);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Update table {table.Name} failed. {ex.Message}", ex);
            }
        }
    }
}
