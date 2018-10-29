using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.sql
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

        public override string ServerHelp => "Server:Port Name";
        public override string DefaultDatabaseHelp => "Database";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override string DatabaseTypeName => "PostgreSQL";
        public override EConnectionCategory DatabaseConnectionCategory => EConnectionCategory.SqlDatabase;
        public override bool CanUseArray => true;

        // postgre doesn't have unsigned values, so convert the unsigned to signed 
        public override object ConvertParameterType(ETypeCode typeCode, int rank, object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return DBNull.Value;
            }

            if (rank > 0 && !CanUseArray)
            {
                return Operations.Parse(ETypeCode.String, value);
            }

            switch (value)
            {
                case ushort uint16:
                    return (int)uint16;
                case uint uint32:
                    return (long)uint32;
                case ulong uint64:
                    return (long)uint64;
                default:
                    return value;
            }
        }

        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
                case ETypeCode.DateTime:
                    return new DateTime(9999, 12, 31, 23, 59, 59, 999);
                default:
                    return GetDataTypeMaxValue(typeCode, length);
            }
        }
        
        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
        {
            try
            {
                var fieldCount = reader.FieldCount;
                var copyCommand = new StringBuilder();

                copyCommand.Append($"COPY {SqlTableName(table)} (");
                for (var i = 0; i < fieldCount; i++)
                {
                    copyCommand.Append(AddDelimiter(reader.GetName(i)) + (i == fieldCount - 1 ? "" : ","));
                }

                copyCommand.Append(") FROM STDIN (FORMAT BINARY)");

                using (var connection = (NpgsqlConnection) await NewConnection())
                using (var writer = connection.BeginBinaryImport(copyCommand.ToString()))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        writer.StartRow();

                        for (var i = 0; i < fieldCount; i++)
                        {
                            try
                            {

                                var converted = GetSqlDbType(table.Columns[i], reader[i]);

                                if (converted.value == null || converted.value == DBNull.Value)
                                {
                                    writer.WriteNull();
                                }
                                else
                                {
                                    writer.Write(converted.value, converted.type);
                                }
                            }
                            catch (Exception ex)
                            {
#if DEBUG
                                throw new ConnectionException($"Column {table.Columns[i].Name}, value {reader[i]}.  {ex.Message}", ex);
#else
                                throw new ConnectionException($"Column {table.Columns[i].Name}.  {ex.Message}", ex);
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

        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "select table_name from information_schema.tables where table_name = @NAME"))
                {
                    cmd.Parameters.Add(CreateParameter(cmd, "@NAME", table.Name));

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
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
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
                    var dropResult = await DropTable(table);
                }

                var createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + AddDelimiter(table.Name) + " ( ");
                foreach (var col in table.Columns)
                {
                    if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                        createSql.Append(AddDelimiter(col.Name) + " SERIAL"); //TODO autoincrement for postgresql
                    else
                    {
                        createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col));
                        if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                            createSql.Append(" IDENTITY(1,1)"); //TODO autoincrement for postgresql
                        if (col.AllowDbNull == false)
                            createSql.Append(" NOT NULL");
                        else
                            createSql.Append(" NULL");
                    }
                    createSql.Append(",");
                }

                //Add the primary key using surrogate key or autoincrement.
                var key = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
                if (key == null)
                {
                    key = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                }

                if (key != null)
                    createSql.Append("CONSTRAINT \"PK_" + AddEscape(table.Name) + "\" PRIMARY KEY (" + AddDelimiter(key.Name) + "),");


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
                case ETypeCode.Int32:
                case ETypeCode.UInt16:
                    sqlType = "int";
                    break;
                case ETypeCode.Byte:
                case ETypeCode.Int16:
                case ETypeCode.SByte:
                    sqlType = "smallint";
                    break;
                case ETypeCode.Int64:
                case ETypeCode.UInt32:
                case ETypeCode.UInt64:
                    sqlType = "bigint";
                    break;
                case ETypeCode.String:
                    if (column.MaxLength == null)
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(10485760)";
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(" + column.MaxLength + ")";
                    break;
                case ETypeCode.CharArray:
                    if(column.MaxLength == null) 
                        sqlType = (column.IsUnicode == true ? "n" : "") + "char(0)";
                    else 
                        sqlType= (column.IsUnicode == true ? "n" : "") + "char(" + column.MaxLength + ")";
                    break;
				case ETypeCode.Text:
                case ETypeCode.Json:
                case ETypeCode.Xml:
                    sqlType = (column.IsUnicode == true ? "n" : "") + "text";
					break;
                case ETypeCode.Single:
                    sqlType = "real";
                    break;
                case ETypeCode.Double:
                    sqlType = "double precision";
                    break;
                case ETypeCode.Boolean:
                    sqlType = "bool";
                    break;
                case ETypeCode.DateTime:
                    sqlType = "timestamp";
                    break;
                case ETypeCode.Time:
                    sqlType = "time";
                    break;
                case ETypeCode.Guid:
                    sqlType = "text";
                    break;
                case ETypeCode.Binary:
                    sqlType = "bytea";
                    break;
                //case TypeCode.TimeSpan:
                //    SQLType = "time(7)";
                //    break;
                case ETypeCode.Unknown:
                    sqlType = "varchar(10485760)";
                    break;
                case ETypeCode.Decimal:
                    sqlType =  $"numeric ({column.Precision??28}, {column.Scale??0})";
                    break;
                default:
                    throw new Exception($"The datatype {column.DataType} is not compatible with the create table.");
            }

            if (column.IsArray())
            {
                return sqlType + "[]";
            }

            return sqlType;
        }


        /// <summary>
        /// Gets the start quote to go around the values in sql insert statement based in the column type.
        /// </summary>
        /// <returns></returns>
        protected override string GetSqlFieldValueQuote(ETypeCode type, object value)
        {
            string returnValue;

            if (value == null || value is DBNull)
                return "null";

            //if (value is string && type != ETypeCode.String && string.IsNullOrWhiteSpace((string)value))
            //    return "null";

            switch (type)
            {
                case ETypeCode.Byte:
                case ETypeCode.Single:
                case ETypeCode.Int16:
                case ETypeCode.Int32:
                case ETypeCode.Int64:
                case ETypeCode.SByte:
                case ETypeCode.UInt16:
                case ETypeCode.UInt32:
                case ETypeCode.UInt64:
                case ETypeCode.Double:
                case ETypeCode.Decimal:
                    returnValue = AddEscape(value.ToString());
                    break;
                case ETypeCode.String:
				case ETypeCode.Text:
                case ETypeCode.Json:
                case ETypeCode.Xml:
                case ETypeCode.Guid:
                case ETypeCode.Boolean:
                case ETypeCode.Unknown:
                    returnValue = "'" + AddEscape(value.ToString()) + "'";
                    break;
                case ETypeCode.DateTime:
                    if (value is DateTime time)
                        returnValue = "to_timestamp('" + AddEscape(time.ToString("yyyy-MM-dd HH:mm:ss.ff")) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    else
                        returnValue = "to_timestamp('" + AddEscape((string)value) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    break;
                case ETypeCode.Time:
                    if (value is TimeSpan span)
                        returnValue = "to_timestamp('" + AddEscape(span.ToString("yyyy-MM-dd HH:mm:ss.ff")) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    else
                        returnValue = "to_timestamp('" + AddEscape((string)value) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    break;
                default:
                    throw new Exception("The datatype " + type + " is not compatible with the sql statement.");
            }

            return returnValue;
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
                        connectionString = "Host=" + host + "; Port=" + port + "; User Id=" + Username + "; Password=" + Password + "; ";
                    else
                        connectionString = "Host=" + host + "; Port=" + port + "; Integrated Security=true; ";

                    if (!string.IsNullOrEmpty(DefaultDatabase))
                    {
                        connectionString += "Database = " + DefaultDatabase;
                    }
                }

                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    throw new ConnectionException($"Postgre connection status is {connection.State}.");

                }

                return connection;
            }
            catch (Exception ex)
            {
                if (connection != null)
                    connection.Dispose();
                throw new ConnectionException($"Postgre connection failed. {ex.Message}", ex);
            }
        }

        public override async Task CreateDatabase(string databaseName, CancellationToken cancellationToken)
        {
            try
            {
                DefaultDatabase = "";
                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "create database " + AddDelimiter(databaseName)))
                {
                    var value = await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

                DefaultDatabase = databaseName;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create database {databaseName} failed. {ex.Message}", ex);
            }
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
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

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken)
        {
            try
            {
                var tableList = new List<Table>();

                using (var connection = await NewConnection())
                {

                    using (var cmd = CreateCommand(connection, "select table_catalog, table_schema, table_name from information_schema.tables where table_schema not in ('pg_catalog', 'information_schema')"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            var table = new Table
                            {
                                Name = reader["table_name"].ToString(),
                                Schema = reader["table_schema"].ToString()
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

        public override async Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken)
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
                                Rank = (string)reader["data_type"] == "ARRAY" ? (byte) 1 : (byte) 0,
                                DataType = (string)reader["data_type"] == "ARRAY" ? ConvertSqlToTypeCode(reader["element_type"].ToString()) : ConvertSqlToTypeCode(reader["data_type"].ToString())
                            };

                            if (col.DataType == ETypeCode.Unknown)
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

                            if (col.DataType == ETypeCode.String)
                            {
                                col.MaxLength = ConvertNullableToInt(reader["character_maximum_length"]);
                            }
                            else if (col.DataType == ETypeCode.Double || col.DataType == ETypeCode.Decimal)
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


        public ETypeCode ConvertSqlToTypeCode(string sqlType)
        {
            switch (sqlType)
            {
                case "bit": return ETypeCode.Boolean;
                case "varbit": return ETypeCode.Binary;
                case "bytea": return ETypeCode.Binary;
                case "smallint": return ETypeCode.Int16;
                case "int": return ETypeCode.Int32;
                case "integer": return ETypeCode.Int32;
                case "bigint": return ETypeCode.Int64;
                case "smallserial": return ETypeCode.Int16;
                case "serial": return ETypeCode.Int32;
                case "bigserial": return ETypeCode.Int64;
                case "numeric": return ETypeCode.Decimal;
                case "double precision": return ETypeCode.Double;
                case "real": return ETypeCode.Double;
                case "money": return ETypeCode.Decimal;
                case "bool": return ETypeCode.Boolean;
                case "boolean": return ETypeCode.Boolean;
                case "date": return ETypeCode.DateTime;
                case "timestamp": return ETypeCode.DateTime;
                case "timestamp without time zone": return ETypeCode.DateTime;
                case "timestamp with time zone": return ETypeCode.DateTime;
                case "interval": return ETypeCode.Time;
                case "time": return ETypeCode.Time;
                case "time without time zone": return ETypeCode.Time;
                case "time with time zone": return ETypeCode.Time;
                case "character varying": return ETypeCode.String;
                case "varchar": return ETypeCode.String;
                case "character": return ETypeCode.CharArray;
                case "text": return ETypeCode.Text;
            }
            return ETypeCode.Unknown;
        }

        public override async Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var cmd = connection.CreateCommand())
                {

                    cmd.CommandText = "truncate table " + AddDelimiter(table.Name);

                    try
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    catch (Exception)
                    {
                        // if truncate fails, try a delete from
                        cmd.CommandText = "delete from " + AddDelimiter(table.Name);
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Truncate table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                var autoIncrementSql = "";
                var deltaColumn = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                if (deltaColumn != null)
                {
                    autoIncrementSql = "SELECT max(" + AddDelimiter(deltaColumn.Name) + ") from " + AddDelimiter(table.Name);
                }

                long identityValue = 0;

                using (var connection = (NpgsqlConnection) await NewConnection())
                {
                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var query in queries)
                        {
                            insert.Clear();
                            values.Clear();

                            insert.Append("INSERT INTO " + AddDelimiter(table.Name) + " (");
                            values.Append("VALUES (");

                            for (var i = 0; i < query.InsertColumns.Count; i++)
                            {
                                insert.Append(AddDelimiter(query.InsertColumns[i].Column.Name) + ",");
                                values.Append("@col" + i + ",");
                            }

                            var insertCommand = insert.Remove(insert.Length - 1, 1) + ") " +
                                values.Remove(values.Length - 1, 1) + "); " + autoIncrementSql;

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
                                        var converted = GetSqlDbType(query.InsertColumns[i].Column, query.InsertColumns[i].Value);
                                        param.NpgsqlDbType = converted.type;
                                        param.Size = -1;
                                        param.Value = converted.value??DBNull.Value;
                                        cmd.Parameters.Add(param);

                                    }

                                    var identity = await cmd.ExecuteScalarAsync(cancellationToken);
                                    identityValue = Convert.ToInt64(identity);

                                    cancellationToken.ThrowIfCancellationRequested();
                                }
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"Insert query failed.  {ex.Message}", ex);
                            }
                        }
                        transaction.Commit();
                    }

                    return identityValue; //sometimes reader returns -1, when we want this to be error condition.

                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert into table {table.Name} failed. {ex.Message}", ex);
            }

        }

        private (NpgsqlDbType type, object value) GetSqlDbType(TableColumn tableColumn, object value)
        {
            if (tableColumn.IsArray())
            {
                var values =(IEnumerable)value;

                var parsedValues = new List<object>();
                var type = NpgsqlDbType.Varchar;

                var i = 0;
                foreach (var v in values)
                {
                    var result = GetSqlDbType(tableColumn.DataType, v);
                    type = result.type;
                    parsedValues.Add(result.value);
                    i++;
                }

                if (i == 0)
                {
                    return GetSqlDbType(tableColumn.DataType, null);
                }

                return (NpgsqlDbType.Array | type, parsedValues);
            }
            else
            {
                return GetSqlDbType(tableColumn.DataType, value);
            }
        }
        
        private (NpgsqlDbType type, object value) GetSqlDbType(ETypeCode typeCode, object value)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return (NpgsqlDbType.Smallint, Operations.Parse(ETypeCode.Int16, value));
                case ETypeCode.SByte:
                    return (NpgsqlDbType.Smallint, Operations.Parse(ETypeCode.Int16, value));
                case ETypeCode.UInt16:
                    return (NpgsqlDbType.Integer, Operations.Parse(ETypeCode.Int32, value));
                case ETypeCode.UInt32:
                    return (NpgsqlDbType.Bigint, Operations.Parse(ETypeCode.Int64, value));
                case ETypeCode.UInt64:
                    return (NpgsqlDbType.Bigint, Operations.Parse(ETypeCode.Int64, value));
                case ETypeCode.Int16:
                    return (NpgsqlDbType.Smallint, Operations.Parse(ETypeCode.Int16, value));
                case ETypeCode.Int32:
                    return (NpgsqlDbType.Integer, Operations.Parse(ETypeCode.Int32, value));
                case ETypeCode.Int64:
                    return (NpgsqlDbType.Bigint, Operations.Parse(ETypeCode.Int64, value));
                case ETypeCode.Decimal:
                    return (NpgsqlDbType.Numeric, Operations.Parse(ETypeCode.Decimal, value));
                case ETypeCode.Double:
                    return (NpgsqlDbType.Double, Operations.Parse(ETypeCode.Double, value));
                case ETypeCode.Single:
                    return (NpgsqlDbType.Real, Operations.Parse(ETypeCode.Single, value));
                case ETypeCode.String:
                    return (NpgsqlDbType.Varchar, Operations.Parse(ETypeCode.String, value));
				case ETypeCode.Text:
					return (NpgsqlDbType.Text, Operations.Parse(ETypeCode.Text, value));
                case ETypeCode.Boolean:
                    return (NpgsqlDbType.Boolean, Operations.Parse(ETypeCode.Boolean, value));
                case ETypeCode.DateTime:
                    return (NpgsqlDbType.Timestamp, Operations.Parse(ETypeCode.DateTime, value));
                case ETypeCode.Time:
                    return (NpgsqlDbType.Time, Operations.Parse(ETypeCode.Time, value));
                case ETypeCode.Guid:
                    return (NpgsqlDbType.Varchar, value.ToString());
                case ETypeCode.Binary:
                    return (NpgsqlDbType.Bytea, value);
                default:
                    return (NpgsqlDbType.Varchar, Operations.Parse(ETypeCode.String, value));
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = (NpgsqlConnection) await NewConnection())
                {

                    var sql = new StringBuilder();

                    var rows = 0;

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var query in queries)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            sql.Clear();

                            sql.Append("update " + AddDelimiter(table.Name) + " set ");

                            var count = 0;
                            foreach (var column in query.UpdateColumns)
                            {
                                sql.Append(AddDelimiter(column.Column.Name) + " = @col" + count + ","); // cstr(count)" + GetSqlFieldValueQuote(column.Column.DataType, column.Value) + ",");
                                count++;
                            }
                            sql.Remove(sql.Length - 1, 1); //remove last comma
                            sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                            //  Retrieving schema for columns from a single table
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.Transaction = transaction;
                                cmd.CommandText = sql.ToString();

                                for (var i = 0; i < query.UpdateColumns.Count; i++)
                                {
                                    var param = cmd.CreateParameter();
                                    param.ParameterName = "@col" + i;
                                    var converted = GetSqlDbType(query.UpdateColumns[i].Column, query.UpdateColumns[i].Value);
                                    param.NpgsqlDbType = converted.type;
                                    param.Size = -1;
                                    param.Value = converted.value??DBNull.Value;
                                    cmd.Parameters.Add(param);
                                }

                                try
                                {
                                    rows += await cmd.ExecuteNonQueryAsync(cancellationToken);
                                }
                                catch (Exception ex)
                                {
                                    throw new ConnectionException($"The update query failed.  {ex.Message}");
                                }
                            }
                        }
                        transaction.Commit();
                    }
				}
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Update table {table.Name} failed. {ex.Message}", ex);
            }
        }
    }
}
