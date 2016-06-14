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

namespace dexih.connections
{
    public class ConnectionSqlite : Connection
    {

        public override string ServerHelp => "Server Name";
        //help text for what the server means for this description
        public override string DefaultDatabaseHelp => "Database";
        //help text for what the default database means for this description
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override bool AllowDataPoint => true;
        public override bool AllowManaged => true;
        public override bool AllowPublish => true;
        public override string DatabaseTypeName => "SQLite";
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;


        private SqliteConnection _connection; //used to for the datareader function
        private SqliteDataReader _sqlReader;

        public override bool CanBulkLoad => true;

        protected override async Task<ReturnValue> WriteDataBulkInner(DbDataReader reader, Table table)
        {
            try
            {
                ReturnValue<SqliteConnection> connection = await NewConnection();
                if (connection.Success == false)
                {
                    return connection;
                }

                StringBuilder insert = new StringBuilder();
                StringBuilder values = new StringBuilder();

                insert.Append("INSERT INTO " + DatabaseTableName(table.TableName) + " (");
                values.Append("VALUES (");

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    insert.Append("[" + reader.GetName(i) + "],");
                    values.Append("@col" + i.ToString() + ",");
                }

                string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " + values.Remove(values.Length - 1, 1).ToString() + ");";

                using (var transaction = connection.Value.BeginTransaction())
                {
                    using (var cmd = connection.Value.CreateCommand())
                    {
                        cmd.CommandText = insertCommand;
                        cmd.Transaction = transaction;
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            cmd.Parameters.AddWithValue("@col" + i.ToString(), "");
                        }
                        cmd.Prepare();

                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                cmd.Parameters[i].Value = reader[i];
                            }
                            cmd.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }

                connection.Value.Dispose();

                return new ReturnValue(true, "", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred in the bulkload processing: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <returns></returns>
        public override async Task<ReturnValue> CreateManagedTable(Table table, bool dropTable = false)
        {
            try
            {
                ReturnValue<SqliteConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }

                SqliteConnection connection = connectionResult.Value;

                string tableName = DatabaseTableName(table.TableName);

                SqliteCommand cmd = new SqliteCommand("SELECT name FROM sqlite_master WHERE type='table' and name = @NAME;", connection);
                cmd.Parameters.Add("@NAME", SqliteType.Text);
                cmd.Parameters["@NAME"].Value = tableName;

                object tableExists = null;
                try
                {
                    tableExists = await cmd.ExecuteScalarAsync();
                }
                catch (Exception ex)
                {
                    return new ReturnValue<List<string>>(false, "The sqllite 'get tables' query could not be run due to the following error: " + ex.Message, ex);
                }

                if (tableExists != null && dropTable == false)
                {
                    return new ReturnValue(false, "The table " + tableName + " already exists on the underlying database.  Please drop the table first.", null);
                }

                SqliteCommand command;
                if (tableExists != null)
                {
                    command = new SqliteCommand("drop table " + SqlEscape(tableName), connection);
                    try
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue(false, "The following error occurred when attempting to drop the table " + table.TableName + ".  " + ex.Message, ex);
                    }
                }

                StringBuilder createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + tableName + " ");

                //sqlite does not support table/column comments, so add a comment string into the ddl.
                if(!string.IsNullOrEmpty(table.Description))
                    createSql.Append(" -- " + table.Description);

                createSql.AppendLine("");
                createSql.Append("(");

                for(int i = 0; i< table.Columns.Count; i++)
                {
                    TableColumn col = table.Columns[i];

                    createSql.Append("[" + SqlEscape(col.ColumnName) + "] " + GetSqlType(col.DataType, col.MaxLength, col.Scale, col.Precision) + " ");
                    if (col.AllowDbNull == false)
                        createSql.Append("NOT NULL ");
                    else
                        createSql.Append("NULL ");

                    if (col.DeltaType == TableColumn.EDeltaType.SurrogateKey)
                        createSql.Append("PRIMARY KEY ASC ");

                    if(i < table.Columns.Count -1)
                        createSql.Append(",");

                    if (!string.IsNullOrEmpty(col.Description))
                        createSql.Append(" -- " + col.Description);

                    createSql.AppendLine();
                }

                createSql.AppendLine(")");

                command = connectionResult.Value.CreateCommand();
                command.CommandText = createSql.ToString();
                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.TableName + ".  " + ex.Message, ex);
                }

                connectionResult.Value.Dispose();

                return new ReturnValue(true, "", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "An error occurred creating the table " + table.TableName + ".  " + ex.Message, ex);
            }
        }

        /// <summary>
        /// This will add any escape charaters to sql name or value to ensure sql injection is avoided.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public string SqlEscape(string value)
        {
            return value.Replace("'", "''");
        }

        /// <summary>
        /// Converts the table name 
        /// </summary>
        /// <returns></returns>
        public string DatabaseTableName(string tableName)
        {
            string newTableName = tableName;

            if (newTableName.Substring(0, 1) == "[")
                newTableName = newTableName.Substring(1, newTableName.Length - 2);

            return "[" + SqlEscape(newTableName) + "]";
        }

        private string GetSqlType(ETypeCode dataType, int? length, int? scale, int? precision)
        {
            string sqlType;

            switch (dataType)
            {
                case ETypeCode.Int32:
                    sqlType = "int";
                    break;
                case ETypeCode.Byte:
                    sqlType = "tinyint";
                    break;
                case ETypeCode.Int16:
                    sqlType = "smallint";
                    break;
                case ETypeCode.Int64:
                    sqlType = "bigint";
                    break;
                case ETypeCode.String:
                    if (length == null)
                        sqlType = "text";
                    else
                        sqlType = "nvarchar(" + length.ToString() + ")";
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
        public string GetSqlFieldValueQuote(ETypeCode type, object value)
        {
            string returnValue;

            if (value.GetType().ToString() == "System.DBNull")
                return "null";

            switch (type)
            {
                case ETypeCode.Byte:
                case ETypeCode.Int16:
                case ETypeCode.Int32:
                case ETypeCode.Int64:
                case ETypeCode.SByte:
                case ETypeCode.UInt16:
                case ETypeCode.UInt32:
                case ETypeCode.UInt64:
                case ETypeCode.Double:
                case ETypeCode.Decimal:
                    returnValue = SqlEscape(value.ToString());
                    break;
                case ETypeCode.Boolean:
                    returnValue = (bool)value == true ? "1" : "0";
                    break;
                case ETypeCode.String:
                case ETypeCode.Unknown:
                    returnValue = "'" + SqlEscape(value.ToString()) + "'";
                    break;
                case ETypeCode.DateTime:
                case ETypeCode.Time:
                    //sqlite does not have date fields, so convert to format that will work for greater/less compares
                    if (value is DateTime)
                        returnValue = "'" + SqlEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "'";
                    else
                        returnValue = "'" + SqlEscape((string)value) + "'";
                    break;
                default:
                    throw new Exception("The datatype " + type.ToString() + " is not compatible with the create table.");
            }

            return returnValue;
        }

        public string GetSqlCompare(Filter.ECompare compare)
        {
            switch (compare)
            {
                case Filter.ECompare.EqualTo: return "=";
                case Filter.ECompare.GreaterThan: return ">";
                case Filter.ECompare.GreaterThanEqual: return ">=";
                case Filter.ECompare.LessThan: return "<";
                case Filter.ECompare.LessThanEqual: return "<=";
                case Filter.ECompare.NotEqual: return "!=";
                default:
                    return "";
            }
        }

        private string ConnectionString
        {
            get
            {
                string con;
                if (ServerName.Substring(ServerName.Length - 1) != "\\" || ServerName.Substring(ServerName.Length - 1) != "/") ServerName += "\\";
                con = "Data Source=" + ServerName + DefaultDatabase + ".sqlite";
                return con;
            }
        }

        public override bool CanRunQueries => true;


        private async Task<ReturnValue<SqliteConnection>> NewConnection()
        {
            try
            {
                SqliteConnection connection = new SqliteConnection(ConnectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    return new ReturnValue<SqliteConnection>(false, "The sqlserver connection failed to open with a state of : " + connection.State.ToString(), null, null);
                }

                using (var command = new SqliteCommand())
                {
                    command.Connection = connection;
                    command.CommandText = "PRAGMA journal_mode=WAL";
                    command.ExecuteNonQuery();
                }

                return new ReturnValue<SqliteConnection>(true, "", null, connection);
            }
            catch (Exception ex)
            {
                return new ReturnValue<SqliteConnection>(false, "The sqlserver connection failed with the following message: " + ex.Message, null, null);
            }
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName)
        {
            try
            {
                string fileName = ServerName + "\\" + databaseName + ".sqlite";

                if (File.Exists(fileName))
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
                var dbList = await Task.Factory.StartNew(() =>
                {
                    var files = Directory.GetFiles(ServerName, "*.sqlite");

                    List<string> list = new List<string>();

                    foreach (var file in files)
                    {
                        list.Add(Path.GetFileName(file));
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
                ReturnValue<SqliteConnection> connection = await NewConnection();
                if (connection.Success == false)
                {
                    return new ReturnValue<List<string>>(connection.Success, connection.Message, connection.Exception, null);
                }

                SqliteCommand cmd = new SqliteCommand("SELECT name FROM sqlite_master WHERE type='table';", connection.Value);
                SqliteDataReader reader;
                try
                {
                    reader = await cmd.ExecuteReaderAsync();
                }
                catch (Exception ex)
                {
                    return new ReturnValue<List<string>>(false, "The sqllite 'get tables' query could not be run due to the following error: " + ex.Message, ex);
                }

                List<string> tableList = new List<string>();

                while (reader.Read())
                {
                    tableList.Add((string)reader["name"]);
                }

                reader.Dispose();

                connection.Value.Dispose();
                return new ReturnValue<List<string>>(true, "", null, tableList);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The database tables could not be listed due to the following error: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties = null)
        {
            try
            {
                ReturnValue<SqliteConnection> connection = await NewConnection();
                if (connection.Success == false)
                {
                    return new ReturnValue<Table>(connection.Success, connection.Message, connection.Exception);
                }

                SqliteDataReader reader;

                Table table = new Table(tableName);

                table.Description = ""; //sqllite doesn't have table descriptions.

                //The new datatable that will contain the table schema
                table.Columns.Clear();

                // The schema table 
                var cmd = new SqliteCommand(@"PRAGMA table_info('" + table.TableName + "')"
                        , connection.Value);

                try
                {
                    reader = await cmd.ExecuteReaderAsync();
                }
                catch (Exception ex)
                {
                    return new ReturnValue<Table>(false, "The source sqlite table + " + table.TableName + " could have a select query run against it with the following error: " + ex.Message, ex);
                }

                table.LogicalName = table.TableName;

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

                reader.Dispose();
                connection.Value.Dispose();

                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The source sqlserver table + " + tableName + " could not be read due to the following error: " + ex.Message, ex);
            }
        }

        private ETypeCode ConvertSqlToTypeCode(string SqlType)
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

        private string AggregateFunction(SelectColumn column)
        {
            switch (column.Aggregate)
            {
                case SelectColumn.EAggregate.None: return column.Column;
                case SelectColumn.EAggregate.Sum: return "Sum([" + column.Column + "])";
                case SelectColumn.EAggregate.Average: return "Avg([" + column.Column + "])";
                case SelectColumn.EAggregate.Min: return "Min([" + column.Column + "])";
                case SelectColumn.EAggregate.Max: return "Max([" + column.Column + "])";
                case SelectColumn.EAggregate.Count: return "Count([" + column.Column + "])";
            }

            return ""; //not possible to get here.
        }

        private string BuildSelectQuery(Table table, SelectQuery query)
        {
            StringBuilder sql = new StringBuilder();

            sql.Append("select ");
            sql.Append(String.Join(",", query.Columns.Select(c=> AggregateFunction(c)).ToArray()) + " ");
            sql.Append("from " + DatabaseTableName(table.TableName) + " ");
            sql.Append(BuildFiltersString(query.Filters));

            if (query.Groups?.Count > 0)
            {
                sql.Append("group by ");
                sql.Append("[" + String.Join("],[", query.Groups.Select(c => SqlEscape(c)).ToArray()) + "] ");
            }
            if (query.Sorts?.Count > 0)
            {
                sql.Append("order by ");
                sql.Append(String.Join(",", query.Sorts.Select(c => "[" + SqlEscape(c.Column) + "] " + (c.Direction == Sort.EDirection.Descending ? " desc" : "")).ToArray()));
            }

            return sql.ToString();
        }

        private string BuildFiltersString(List<Filter> filters)
        {
            if (filters == null || filters.Count == 0)
                return "";
            else
            {

                StringBuilder sql = new StringBuilder();
                sql.Append("where ");

                foreach (var filter in filters)
                {
                    if (filter.Column1 != null)
                        sql.Append(" [" + SqlEscape(filter.Column1) + "] ");
                    else
                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value1) + " ");

                    sql.Append(GetSqlCompare(filter.Operator));

                    if (filter.Column2 != null)
                        sql.Append(" [" + SqlEscape(filter.Column2) + "] ");
                    else
                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value2) + " ");

                    sql.Append(filter.AndOr.ToString());
                }

                sql.Remove(sql.Length - 3, 3); //remove last or/and

                return sql.ToString();
            }
        }

        protected override async Task<ReturnValue> DataReaderStartQueryInner(Table table, SelectQuery query)
        {
            if (OpenReader)
            {
                return new ReturnValue(false, "The current connection is already open.", null);
            }

            CachedTable = table;

            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return connection;
            }

            _connection = connection.Value;

            SqliteCommand cmd = new SqliteCommand(BuildSelectQuery(table, query), _connection);

            try
            {
                _sqlReader = await cmd.ExecuteReaderAsync();
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The connection reader for the sqlserver table " + table.TableName + " could failed due to the following error: " + ex.Message, ex);
            }

            if (_sqlReader == null)
            {
                return new ReturnValue(false, "The connection reader for the sqlserver table " + table.TableName + " return null for an unknown reason.", null);
            }
            else
            {
                OpenReader = true;
                return new ReturnValue(true, "", null);
            }
        }

        public override async Task<ReturnValue<int>> ExecuteUpdateQuery(Table table, List<UpdateQuery> queries)
        {
            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
            }

            StringBuilder sql = new StringBuilder();

            int rows = 0;

            using (var transaction = connection.Value.BeginTransaction())
            {
                foreach (var query in queries)
                {
                    sql.Clear();

                    sql.Append("update " + DatabaseTableName(table.TableName) + " set ");

                    foreach (QueryColumn column in query.UpdateColumns)
                        sql.Append("[" + SqlEscape(column.Column) + "] = " + GetSqlFieldValueQuote(column.ColumnType, column.Value) + ",");
                    sql.Remove(sql.Length - 1, 1); //remove last comma
                    sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                    //  Retrieving schema for columns from a single table
                    SqliteCommand cmd = new SqliteCommand(sql.ToString(), connection.Value, transaction);

                    try
                    {
                        rows += await cmd.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<int>(false, "The sqllite update query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, -1);
                    }
                }
                transaction.Commit();
            }

            connection.Value.Close();
            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
        }

        public override async Task<ReturnValue<int>> ExecuteDeleteQuery(Table table, List<DeleteQuery> queries)
        {
            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
            }

            StringBuilder sql = new StringBuilder();
            int rows = 0;

            using (var transaction = connection.Value.BeginTransaction())
            {
                foreach (var query in queries)
                {
                    sql.Clear();
                    sql.Append("delete from " + DatabaseTableName(table.TableName) + " ");
                    sql.Append(BuildFiltersString(query.Filters));

                    SqliteCommand cmd = new SqliteCommand(sql.ToString(), connection.Value, transaction);

                    try
                    {
                        rows += await cmd.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<int>(false, "The sqllite delete query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, -1);
                    }
                }
                transaction.Commit();
            }

            connection.Value.Close();
            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
        }

        public override async Task<ReturnValue<int>> ExecuteInsertQuery(Table table, List<InsertQuery> queries)
        {
            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
            }

            StringBuilder insert = new StringBuilder();
            StringBuilder values = new StringBuilder();
            int rows = 0;

            using (var transaction = connection.Value.BeginTransaction())
            {
                foreach (var query in queries)
                {
                    insert.Clear();
                    values.Clear();

                    insert.Append("INSERT INTO " + DatabaseTableName(table.TableName) + " (");
                    values.Append("VALUES (");

                    for (int i = 0; i < query.InsertColumns.Count; i++)
                    {
                        insert.Append("[" + query.InsertColumns[i].Column + "],");
                        values.Append("@col" + i.ToString() + ",");
                    }

                    string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " + values.Remove(values.Length - 1, 1).ToString() + ");";

                    try
                    {
                        using (var cmd = connection.Value.CreateCommand())
                        {
                            cmd.CommandText = insertCommand;
                            cmd.Transaction = transaction;

                            for (int i = 0; i < query.InsertColumns.Count; i++)
                            {
                                cmd.Parameters.AddWithValue("@col" + i.ToString(), query.InsertColumns[i].Value);
                            }
                            rows += cmd.ExecuteNonQuery();
                        }
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<int>(false, "The sqllite insert query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + insertCommand?.ToString(), ex, -1);
                    }
                }
                transaction.Commit();
            }

            connection.Value.Close();
            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query)
        {
            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<object>(connection);
            }

            string sql = BuildSelectQuery(table, query);

            //  Retrieving schema for columns from a single table
            SqliteCommand cmd = new SqliteCommand(sql, connection.Value);
            object value;
            try
            {
                value = await cmd.ExecuteScalarAsync();
            }
            catch (Exception ex)
            {
                return new ReturnValue<object>(false, "The sqllite select query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  Sql command was: " + sql, ex, null);
            }

            connection.Value.Close();
            return new ReturnValue<object>(true, value);
        }

        public override async Task<ReturnValue> TruncateTable(Table table)
        {
            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return connection;
            }

            SqliteCommand cmd = new SqliteCommand("delete from " + DatabaseTableName(table.TableName), connection.Value);
            try
            {
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The sqllite update query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex);
            }

            connection.Value.Close();

            //if(rows == -1)
            //    return new ReturnValue(false, "The sqllite truncate table query for " + Table.TableName + " could appears to have failed for an unknown reason." , null);
            //else
            return new ReturnValue(true, "", null);
        }

        public override string GetCurrentFile()
        {
            throw new NotImplementedException();
        }

        public override ReturnValue ResetTransform()
        {
            throw new NotImplementedException();
        }

        public override bool Initialize()
        {
            throw new NotImplementedException();
        }

        public override string Details()
        {
            StringBuilder details = new StringBuilder();
            details.AppendLine("<b>Source</b> <br />");
            details.AppendLine("<b>Database</b>: sqllite<br />");
            details.AppendLine("<b>Table</b>: " + CachedTable.TableName + "<br />");
            details.AppendLine("<b>SQL</b>: " + BuildSelectQuery(CachedTable, SelectQuery));
            return details.ToString();
        }

        public override List<Sort> RequiredSortFields()
        {
            throw new NotImplementedException();
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue(true, "", null));
        }

        public override async Task<ReturnValue> DataWriterStart(Table table)
        {
            return await Task.Run(() => new ReturnValue(true, "", null));
        }

        public override async Task<ReturnValue> DataWriterFinish(Table table)
        {
            return await Task.Run(() => new ReturnValue(true, "", null));
        }

        public override bool CanLookupRowDirect { get; } = true;        

        public override async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        {
            ReturnValue<SqliteConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<object[]>(connection);
            }

            SelectQuery query = new SelectQuery()
            {
                Columns = CachedTable.Columns.Select(c => new SelectColumn(c.ColumnName)).ToList(),
                Table = CachedTable.TableName,
                Filters = filters
            };
            string sql = BuildSelectQuery(CachedTable, query);

            //  Retrieving schema for columns from a single table
            SqliteCommand cmd = new SqliteCommand(sql, connection.Value);
            DbDataReader reader;
            try
            {
                reader = await cmd.ExecuteReaderAsync();
            }
            catch (Exception ex)
            {
                return new ReturnValue<object[]>(false, "The sqllite lookup query for " + CachedTable.TableName + " could not be run due to the following error: " + ex.Message + ".  Sql command was: " + sql, ex);
            }

            if (reader.Read())
            {
                object[] values = new object[CachedTable.Columns.Count];
                reader.GetValues(values);
                return new ReturnValue<object[]>(true, values);
            }
            else
                return new ReturnValue<object[]>(false, "The sqllite lookup query for " + CachedTable.TableName + " return no rows.  Sql command was: " + sql, null);

        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            if (_sqlReader == null)
                throw new Exception("The sqllite reader has not been set.");

            try
            {
                bool result = _sqlReader.Read();
                if (result == false && _connection != null && _connection.State == ConnectionState.Open)
                    _connection.Close();

                if (result)
                {
                    object[] row = new object[CachedTable.Columns.Count];
                    _sqlReader.GetValues(row);
                    return new ReturnValue<object[]>(true, row);
                }
                else
                    return new ReturnValue<object[]>(false, null);
            }
            catch (Exception ex)
            {
                throw new Exception("The sqllite reader failed due to the following error: " + ex.Message, ex);
            }
        }


    }
}
