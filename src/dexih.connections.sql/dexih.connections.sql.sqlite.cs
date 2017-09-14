using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Microsoft.Data.Sqlite;
using dexih.functions;
using System.IO;
using System.Data.Common;
using System.Threading;
using System.Diagnostics;
using static Dexih.Utils.DataType.DataType;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.connections.sql
{
    public class ConnectionSqlite : ConnectionSql
    {

        public override string ServerHelp => "Server Name";

        //help text for what the server means for this description
        public override string DefaultDatabaseHelp => "Database";

        //help text for what the default database means for this description
        public override bool AllowNtAuth => true;

        public override bool AllowUserPass => true;
        public override string DatabaseTypeName => "SQLite";

        public override object ConvertParameterType(object value)
        {
            if (value == null)
                return DBNull.Value;
            else if (value is Guid || value is ulong)
                return value.ToString();
            else
                return value;
        }
        
        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
               case ETypeCode.Decimal:
                   return (decimal) 999999999999999;
                default:
                    return Dexih.Utils.DataType.DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }

        public override object GetConnectionMinValue(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.Decimal:
                    return (decimal)-999999999999999;
                default:
                    return Dexih.Utils.DataType.DataType.GetDataTypeMinValue(typeCode);
            }
        }


        public override async Task<bool> TableExists(Table table, CancellationToken cancelToken)
        {
            try
            {
                using (var connection = await NewConnection())
                {

                    using (DbCommand cmd = CreateCommand(connection,
                        "SELECT name FROM sqlite_master WHERE type = 'table' and name = @NAME;"))
                    {
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", table.Name));
                        var tableExists = await cmd.ExecuteScalarAsync(cancelToken);
                        return tableExists != null;
                    }
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Table exists for table {table.Name} failed. {ex.Message}", ex);
            }
        }

        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <returns></returns>
        public override async Task<bool> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            try
            {

                var tableExists = await TableExists(table, cancelToken);

                //if table exists, and the dropTable flag is set to false, then error.
                if (tableExists && dropTable == false)
                {
                    throw new ConnectionException($"The table {table.Name} already exists. Drop the table first.");
                }

                //if table exists, then drop it.
                if (tableExists)
                {
                    var dropResult = await DropTable(table);
                }

                StringBuilder createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + AddDelimiter(table.Name) + " ");

                //sqlite does not support table/column comments, so add a comment string into the ddl.
                if (!string.IsNullOrEmpty(table.Description))
                    createSql.Append(" -- " + table.Description);

                createSql.AppendLine("");
                createSql.Append("(");

                for (int i = 0; i < table.Columns.Count; i++)
                {
                    TableColumn col = table.Columns[i];

                    //ignore datatypes for autoincrement and create a primary key.
                    if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                    {
                        createSql.Append(AddDelimiter(col.Name) + " INTEGER PRIMARY KEY ");
                    }
                    else
                    {
                        createSql.Append(AddDelimiter(col.Name) + " " +
                                         GetSqlType(col.Datatype, col.MaxLength, col.Scale, col.Precision) + " ");
                        if (col.AllowDbNull == false)
                            createSql.Append("NOT NULL ");
                        else
                            createSql.Append("NULL ");

                        if (col.DeltaType == TableColumn.EDeltaType.SurrogateKey)
                        {
                            if (table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement) == null)
                                createSql.Append("PRIMARY KEY ASC ");
                            else
                                createSql.Append("UNIQUE ");
                        }
                    }

                    if (i < table.Columns.Count - 1)
                        createSql.Append(",");

                    if (!string.IsNullOrEmpty(col.Description))
                        createSql.Append(" -- " + col.Description);

                    createSql.AppendLine();
                }

                createSql.AppendLine(")");

                using (var connection = await NewConnection())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = createSql.ToString();
                    try
                    {
                        await command.ExecuteNonQueryAsync(cancelToken);
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"The create table query failed.  {ex.Message}");
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create table {table.Name} failed. {ex.Message}", ex);
            }

        }

        public override string GetSqlType(ETypeCode dataType, int? length, int? scale, int? precision)
        {
            string sqlType;

            switch (dataType)
            {
                case ETypeCode.Int32:
                case ETypeCode.UInt16:
                    sqlType = "int";
                    break;
                case ETypeCode.Byte:
                    sqlType = "tinyint";
                    break;
                case ETypeCode.Int16:
                case ETypeCode.SByte:
                    sqlType = "smallint";
                    break;
                case ETypeCode.Int64:
                case ETypeCode.UInt32:
                    sqlType = "bigint";
                    break;
                case ETypeCode.String:
                    if (length == null)
                        sqlType = "text";
                    else
                        sqlType = "nvarchar(" + length.ToString() + ")";
                    break;
                case ETypeCode.Single:
                    sqlType = "float";
                    break;
                case ETypeCode.UInt64:
                    sqlType = "nvarchar(25)";
                    break;
                case ETypeCode.Double:
                    sqlType = "float";
                    break;
                case ETypeCode.Boolean:
                    sqlType = "boolean";
                    break;
                case ETypeCode.DateTime:
                    sqlType = "datetime";
                    break;
                case ETypeCode.Time:
                    sqlType = "text"; //sqlite doesn't have a time type.
                    break;
                case ETypeCode.Guid:
                    sqlType = "text";
                    break;
                case ETypeCode.Unknown:
                    sqlType = "text";
                    break;
                case ETypeCode.Decimal:
                    if (precision.ToString() == "" || scale.ToString() == "")
                        sqlType = "decimal";
                    else
                        sqlType = "decimal (" + precision.ToString() + "," + scale.ToString() + ")";
                    break;
                case ETypeCode.Binary:
                    sqlType = "blob";
                    break;
                default:
                    throw new Exception("The datatype " + dataType.ToString() +
                                        " is not compatible with the create table.");
            }

            return sqlType;
        }


        /// <summary>
        /// Gets the start quote to go around the values in sql insert statement based in the column type.
        /// </summary>
        /// <returns></returns>
        public override string GetSqlFieldValueQuote(ETypeCode type, object value)
        {
            string returnValue;

            if (value == null || value.GetType().ToString() == "System.DBNull")
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
                case ETypeCode.Boolean:
                    returnValue = (bool) value ? "1" : "0";
                    break;
                case ETypeCode.String:
                case ETypeCode.Guid:
                case ETypeCode.Unknown:
                    returnValue = "'" + AddEscape(value.ToString()) + "'";
                    break;
                case ETypeCode.DateTime:
                case ETypeCode.Time:
                    //sqlite does not have date fields, so convert to format that will work for greater/less compares
                    if (value is DateTime)
                        returnValue = "'" + AddEscape(((DateTime) value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "'";
                    else if (value is TimeSpan)
                        returnValue = "'" + AddEscape(((TimeSpan) value).ToString()) + "'";
                    else
                        returnValue = "'" + AddEscape((string) value) + "'";
                    break;
                default:
                    throw new Exception("The datatype " + type.ToString() +
                                        " is not compatible with the create table.");
            }

            return returnValue;
        }


        public override async Task<DbConnection> NewConnection()
        {
            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
                    if (Server.Substring(Server.Length - 1) != "/" || Server.Substring(Server.Length - 1) != "/")
                        Server += "/";
                    connectionString = "Data Source=" + Server + DefaultDatabase + ".sqlite";
                }

                SqliteConnection connection = new SqliteConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState) connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    throw new ConnectionException($"The Sqlite connection has a state of {connection.State}.");
                }

                using (var command = new SqliteCommand())
                {
                    command.Connection = connection;
                    command.CommandText = "PRAGMA journal_mode=WAL";
                    command.ExecuteNonQuery();
                }

                return connection;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Sqlite connection failed at directory {Server} for file {DefaultDatabase}. {ex.Message}", ex);
            }
        }

        public override Task<bool> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            try
            {
                string fileName = Server + "/" + databaseName + ".sqlite";

                bool fileExists = File.Exists(fileName);

                if (fileExists)
                {
                    return Task.FromResult(false);
                }

                var stream = File.Create(fileName);
                stream.Dispose();
                DefaultDatabase = databaseName;

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create database {databaseName} failed. {ex.Message}", ex);
            }
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancelToken)
        {
            try
            {
                bool directoryExists = Directory.Exists(Server);
                if (!directoryExists)
                {
                    throw new ConnectionException($"The directory {Server} does not exist.");
                }

                var files = Directory.GetFiles(Server, "*.sqlite");

                List<string> list = new List<string>();

                foreach (var file in files)
                {
                    list.Add(Path.GetFileName(file).Replace(".sqlite", ""));
                }


                return Task.FromResult(list);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get database list failed. {ex.Message}", ex);
            }
        }

        public override async Task<List<Table>> GetTableList(CancellationToken cancelToken)
        {
            try
            {
                using (var connection = await NewConnection())
                using (DbCommand cmd = CreateCommand(connection, "SELECT name FROM sqlite_master WHERE type='table';"))
                {
                    DbDataReader reader;
                    reader = await cmd.ExecuteReaderAsync(cancelToken);

                    using (reader)
                    {

                        List<Table> tableList = new List<Table>();

                        while (await reader.ReadAsync(cancelToken))
                        {
                            tableList.Add(new Table((string)reader["name"]));
                        }

                        return tableList;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get table list failed. {ex.Message}", ex);
            }
        }

        public override async Task<Table> GetSourceTableInfo(Table originalTable,
            CancellationToken cancelToken)
        {
            if (originalTable.UseQuery)
            {
                return await GetQueryTable(originalTable, cancelToken);
            }

            try
            {

                using (var connection = await NewConnection())
                {

                    Table table = new Table(originalTable.Name);

                    table.Description = ""; //sqllite doesn't have table descriptions.

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"PRAGMA table_info('" + table.Name + "')"))
                    using (DbDataReader reader = await cmd.ExecuteReaderAsync(cancelToken))
                    {

                        //for the logical, just trim out any "
                        table.LogicalName = table.Name.Replace("\"", "");

                        while (await reader.ReadAsync(cancelToken))
                        {
                            TableColumn col = new TableColumn();

                            //add the basic properties
                            col.Name = reader["name"].ToString();
                            col.LogicalName = reader["name"].ToString();
                            col.IsInput = false;

                            string[] dataType = reader["type"].ToString().Split('(', ')');
                            col.Datatype = ConvertSqlToTypeCode(dataType[0]);
                            if (col.Datatype == ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                if (Convert.ToInt32(reader["pk"]) >= 1)
                                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                                else
                                    col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            }

                            if (col.Datatype == ETypeCode.String)
                            {
                                if (dataType.Length > 1)
                                    col.MaxLength = Convert.ToInt32(dataType[1]);
                            }
                            else if (col.Datatype == ETypeCode.Double || col.Datatype == ETypeCode.Decimal)
                            {
                                if (dataType.Length > 1)
                                {
                                    string[] precisionScale = dataType[1].Split(',');
                                    col.Scale = Convert.ToInt32(precisionScale[0]);
                                    if (precisionScale.Length > 1)
                                        col.Precision = Convert.ToInt32(precisionScale[1]);
                                }
                            }

                            //make anything with a large string unlimited.  
                            if (col.MaxLength > 4000)
                                col.MaxLength = null;

                            col.Description = "";
                            col.AllowDbNull = Convert.ToInt32(reader["notnull"]) == 0;
                            table.Columns.Add(col);
                        }
                    }

                    return table;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get source talbe information for {originalTable.Name} failed. {ex.Message}", ex);
            }
        }

        public ETypeCode ConvertSqlToTypeCode(string sqlType)
        {
            switch (sqlType)
            {
                case "INT":
                    return ETypeCode.Int32;
                case "INTEGER":
                    return ETypeCode.Int32;
                case "TINYINT":
                    return ETypeCode.Int16;
                case "SMALLINT":
                    return ETypeCode.Int16;
                case "MEDIUMINT":
                    return ETypeCode.Int32;
                case "BIGINT":
                    return ETypeCode.Int64;
                case "UNSIGNED BIG INT":
                    return ETypeCode.UInt64;
                case "INT2":
                    return ETypeCode.UInt16;
                case "INT8":
                    return ETypeCode.UInt32;
                case "CHARACTER":
                    return ETypeCode.String;
                case "CHAR":
                    return ETypeCode.String;
                case "CURRENCY":
                    return ETypeCode.Decimal;
                case "VARCHAR":
                    return ETypeCode.String;
                case "VARYING CHARACTER":
                    return ETypeCode.String;
                case "NCHAR":
                    return ETypeCode.String;
                case "NATIVE CHARACTER":
                    return ETypeCode.String;
                case "NVARCHAR":
                    return ETypeCode.String;
                case "TEXT":
                    return ETypeCode.String;
                case "CLOB":
                    return ETypeCode.String;
                case "BLOB":
                    return ETypeCode.Unknown;
                case "REAL":
                    return ETypeCode.Double;
                case "DOUBLE":
                    return ETypeCode.Double;
                case "DOUBLE PRECISION":
                    return ETypeCode.Double;
                case "FLOAT":
                    return ETypeCode.Double;
                case "NUMERIC":
                    return ETypeCode.Decimal;
                case "DECIMAL":
                    return ETypeCode.Decimal;
                case "BOOLEAN":
                    return ETypeCode.Boolean;
                case "DATE":
                    return ETypeCode.DateTime;
                case "DATETIME":
                    return ETypeCode.DateTime;
            }
            return ETypeCode.Unknown;
        }

        public override async Task<Tuple<long, long>> ExecuteInsert(Table table, List<InsertQuery> queries,
            CancellationToken cancelToken)
        {
            try
            {

                var autoIncrementSql = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement) == null
                    ? ""
                    : " select last_insert_rowid() from [" + table.Name + "]";
                long identityValue = 0;

                using (var connection = await NewConnection())
                {
                    StringBuilder insert = new StringBuilder();
                    StringBuilder values = new StringBuilder();

                    var timer = Stopwatch.StartNew();
                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var query in queries)
                        {
                            cancelToken.ThrowIfCancellationRequested();

                            insert.Clear();
                            values.Clear();

                            insert.Append("INSERT INTO " + AddDelimiter(table.Name) + " (");
                            values.Append("VALUES (");

                            for (int i = 0; i < query.InsertColumns.Count; i++)
                            {
                                insert.Append("[" + query.InsertColumns[i].Column.Name + "],");
                                values.Append("@col" + i.ToString() + ",");
                            }

                            string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " +
                                                   values.Remove(values.Length - 1, 1).ToString() + "); " +
                                                   autoIncrementSql;

                            try
                            {
                                using (var cmd = connection.CreateCommand())
                                {
                                    cmd.CommandText = insertCommand;
                                    cmd.Transaction = transaction;

                                    for (int i = 0; i < query.InsertColumns.Count; i++)
                                    {
                                        var param = new SqliteParameter(); // cmd.CreateParameter();
                                        param.ParameterName = "@col" + i.ToString();

                                        // sqlite writes guids as binary, so need logic to convert to string first.
                                        if (query.InsertColumns[i].Column.Datatype == ETypeCode.Guid)
                                        {
                                            param.Value = query.InsertColumns[i].Value == null ? (object)DBNull.Value
                                                : query.InsertColumns[i].Value.ToString();

                                        }
                                        else
                                        {
                                            param.Value = query.InsertColumns[i].Value == null ? DBNull.Value
                                                : query.InsertColumns[i].Value;
                                        }
                                        param.DbType = GetDbType(query.InsertColumns[i].Column.Datatype);

                                        cmd.Parameters.Add(param);
                                    }

                                    var identity = await cmd.ExecuteScalarAsync(cancelToken);
                                    identityValue = Convert.ToInt64(identity);

                                }
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The insert query failed.  {ex.Message}");
                            }
                        }
                        transaction.Commit();
                    }

                    timer.Stop();
                    return Tuple.Create(timer.ElapsedTicks, identityValue); //sometimes reader returns -1, when we want this to be error condition.
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Insert into table {table.Name} failed. {ex.Message}", ex);
            }
        }
    }

}
