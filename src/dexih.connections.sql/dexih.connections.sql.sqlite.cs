using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Microsoft.Data.Sqlite;
using dexih.functions;
using Newtonsoft.Json;
using System.IO;
using System.Data.Common;
using static dexih.functions.DataType;
using dexih.transforms;
using System.Threading;
using System.Diagnostics;

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
            else if (value.GetType() == typeof(Guid) || value.GetType() == typeof(UInt64))
                return value.ToString();
            else
                return value;
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<bool>(connectionResult);
            }

            using (var connection = connectionResult.Value)
            {

                using (DbCommand cmd = CreateCommand(connection, "SELECT name FROM sqlite_master WHERE type = 'table' and name = @NAME;"))
                {
                    cmd.Parameters.Add(CreateParameter(cmd, "@NAME", table.TableName));

                    object tableExists = null;
                    try
                    {
                        tableExists = await cmd.ExecuteScalarAsync();
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<bool>(false, "The table exists query could not be run due to the following error: " + ex.Message, ex);
                    }

                    if (tableExists == null)
                        return new ReturnValue<bool>(true, false);
                    else
                        return new ReturnValue<bool>(true, true);
                }
            }
        }

        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <returns></returns>
        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable = false)
        {
            try
            {

                var tableExistsResult = await TableExists(table);
                if (!tableExistsResult.Success)
                    return tableExistsResult;

                //if table exists, and the dropTable flag is set to false, then error.
                if (tableExistsResult.Value && dropTable == false)
                {
                    return new ReturnValue(false, "The table " + table.TableName + " already exists on the underlying database.  Please drop the table first.", null);
                }

                //if table exists, then drop it.
                if (tableExistsResult.Value)
                {
                    var dropResult = await DropTable(table);
                    if (!dropResult.Success)
                        return dropResult;
                }

                StringBuilder createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + AddDelimiter(table.TableName) + " ");

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
                        createSql.Append(AddDelimiter(col.ColumnName) + " INTEGER PRIMARY KEY ");
                    }
                    else
                    {
                        createSql.Append(AddDelimiter(col.ColumnName) + " " + GetSqlType(col.DataType, col.MaxLength, col.Scale, col.Precision) + " ");
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

                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }

                using (var connection = connectionResult.Value)
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = createSql.ToString();
                    try
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.TableName + ".  " + ex.Message, ex);
                    }
                }

                return new ReturnValue(true, "", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "An error occurred creating the table " + table.TableName + ".  " + ex.Message, ex);
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
                default:
                    throw new Exception("The datatype " + dataType.ToString() + " is not compatible with the create table.");
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
                    returnValue = (bool)value == true ? "'True'" : "'False'";
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
                        returnValue = "'" + AddEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "'";
                    else if (value is TimeSpan)
                        returnValue = "'" + AddEscape(((TimeSpan)value).ToString()) + "'";
                    else
                        returnValue = "'" + AddEscape((string)value) + "'";
                    break;
                default:
                    throw new Exception("The datatype " + type.ToString() + " is not compatible with the create table.");
            }

            return returnValue;
        }


        public override async Task<ReturnValue<DbConnection>> NewConnection()
        {
            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
                    if (ServerName.Substring(ServerName.Length - 1) != "/" || ServerName.Substring(ServerName.Length - 1) != "/") ServerName += "/";
                    connectionString = "Data Source=" + ServerName + DefaultDatabase + ".sqlite";
                }

                SqliteConnection connection = new SqliteConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    return new ReturnValue<DbConnection>(false, "The sqlserver connection failed to open with a state of : " + connection.State.ToString(), null, null);
                }

                //using (var command = new SqliteCommand())
                //{
                //    command.Connection = connection;
                //    command.CommandText = "PRAGMA journal_mode=WAL";
                //    command.ExecuteNonQuery();
                //}

                return new ReturnValue<DbConnection>(true, "", null, connection);
            }
            catch (Exception ex)
            {
                return new ReturnValue<DbConnection>(false, "The sqlserver connection failed with the following message: " + ex.Message, null, null);
            }
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName)
        {
            try
            {
                string fileName = ServerName + "/" + databaseName + ".sqlite";

                bool fileExists = await Task.Run(() => File.Exists(fileName));

                if (fileExists)
                    return new ReturnValue(false, "The file " + fileName + " already exists.  Delete or move this file before attempting to create a new database.", null);

                var stream = await Task.Run(() => File.Create(fileName));
                stream.Dispose();
                DefaultDatabase = databaseName;

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "Error creating database " + DefaultDatabase + ".   " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            try
            {
                bool directoryExists = await Task.Run(() => Directory.Exists(ServerName));
                if (!directoryExists)
                    return new ReturnValue<List<string>>(false, "The directory " + ServerName + " does not exist.", null);

                var dbList = await Task.Factory.StartNew(() =>
                {
                    var files = Directory.GetFiles(ServerName, "*.sqlite");

                    List<string> list = new List<string>();

                    foreach (var file in files)
                    {
                        list.Add(Path.GetFileName(file).Replace(".sqlite", ""));
                    }

                    return list;
                });

                return new ReturnValue<List<string>>(true, "", null, dbList);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The databases could not be listed due to the following error: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<List<string>>> GetTableList()
        {
            try
            {
                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<string>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                using (var connection = connectionResult.Value)
                {

                    using (DbCommand cmd = CreateCommand(connection, "SELECT name FROM sqlite_master WHERE type='table';"))
                    {
                        DbDataReader reader;
                        try
                        {
                            reader = await cmd.ExecuteReaderAsync();
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue<List<string>>(false, "The sqllite 'get tables' query could not be run due to the following error: " + ex.Message, ex);
                        }

                        using (reader)
                        {

                            List<string> tableList = new List<string>();

                            while (await reader.ReadAsync())
                            {
                                tableList.Add((string)reader["name"]);
                            }

                            return new ReturnValue<List<string>>(true, "", null, tableList);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The database tables could not be listed due to the following error: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, string> Properties = null)
        {
            try
            {
                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<Table>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                using (var connection = connectionResult.Value)
                {

                    Table table = new Table(tableName);

                    table.Description = ""; //sqllite doesn't have table descriptions.

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"PRAGMA table_info('" + table.TableName + "')"))
                    using (DbDataReader reader = await cmd.ExecuteReaderAsync())
                    {

                        //for the logical, just trim out any "
                        table.LogicalName = table.TableName.Replace("\"", "");

                        while (await reader.ReadAsync())
                        {
                            TableColumn col = new TableColumn();

                            //add the basic properties
                            col.ColumnName = reader["name"].ToString();
                            col.LogicalName = reader["name"].ToString();
                            col.IsInput = false;

                            string[] dataType = reader["type"].ToString().Split('(', ')');
                            col.DataType = ConvertSqlToTypeCode(dataType[0]);
                            if (col.DataType == ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                if (Convert.ToInt32(reader["pk"]) == 1)
                                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                                else
                                    col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            }

                            if (col.DataType == ETypeCode.String)
                            {
                                if (dataType.Length > 1)
                                    col.MaxLength = Convert.ToInt32(dataType[1]);
                            }
                            else if (col.DataType == ETypeCode.Double || col.DataType == ETypeCode.Decimal)
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

                    return new ReturnValue<Table>(true, table);
                }
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The source sqlserver table + " + tableName + " could not be read due to the following error: " + ex.Message, ex);
            }
        }

        public override ETypeCode ConvertSqlToTypeCode(string SqlType)
        {
            switch (SqlType)
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

        public override async Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<Tuple<long, long>>(false, connectionResult.Message, connectionResult.Exception);
            }

            var autoIncrementSql = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement) == null ? "" : " select last_insert_rowid() from [" + table.TableName + "]";
            long identityValue = 0;

            using (var connection = connectionResult.Value)
            {
                StringBuilder insert = new StringBuilder();
                StringBuilder values = new StringBuilder();

                var timer = Stopwatch.StartNew();
                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        insert.Clear();
                        values.Clear();

                        insert.Append("INSERT INTO " + AddDelimiter(table.TableName) + " (");
                        values.Append("VALUES (");

                        for (int i = 0; i < query.InsertColumns.Count; i++)
                        {
                            insert.Append("[" + query.InsertColumns[i].Column + "],");
                            values.Append("@col" + i.ToString() + ",");
                        }

                        string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " +
                            values.Remove(values.Length - 1, 1).ToString() + "); " + autoIncrementSql;

                        try
                        {
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = insertCommand;
                                cmd.Transaction = transaction;

                                for (int i = 0; i < query.InsertColumns.Count; i++)
                                {
                                    var param = cmd.CreateParameter();
                                    param.ParameterName = "@col" + i.ToString();
                                    param.Value = query.InsertColumns[i].Value == null ? DBNull.Value : query.InsertColumns[i].Value;
                                    cmd.Parameters.Add(param);
                                }

                                var identity = await cmd.ExecuteScalarAsync(cancelToken);
                                identityValue = Convert.ToInt64(identity);

                                if (cancelToken.IsCancellationRequested)
                                {
                                    return new ReturnValue<Tuple<long, long>>(false, "Insert rows cancelled.", null, Tuple.Create(timer.ElapsedTicks, identityValue));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue<Tuple<long, long>>(false, "The insert query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + insertCommand?.ToString(), ex, Tuple.Create(timer.ElapsedTicks, (long)0));
                        }
                    }
                    transaction.Commit();
                }

                timer.Stop();
                return new ReturnValue<Tuple<long, long>>(true, Tuple.Create(timer.ElapsedTicks, identityValue)); //sometimes reader returns -1, when we want this to be error condition.
            }
        }


    }
}
