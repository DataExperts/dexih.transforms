//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using System.Data.SqlClient;
//using System.Data;
//using dexih.functions;
//using dexih.core;
//using Newtonsoft.Json;
//using System.IO;
//using System.Data.Common;
//using static dexih.functions.DataType;
//using Oracle.ManagedDataAccess.Client;

//namespace dexih.connections
//{
//    public class ConnectionOracle : Connection
//    {

//        public override string ServerHelp => "Server Name";
////help text for what the server means for this description
//        public override string DefaultDatabaseHelp => "Database";
////help text for what the default database means for this description
//        public override bool AllowNtAuth => true;
//        public override bool AllowUserPass => true;
//        public override bool AllowDefaultDatabase => true;
//        public override bool AllowDataPoint => true;
//        public override bool AllowManaged => true;
//        public override bool AllowPublish => true;
//        public override string DatabaseTypeName => "Oracle";
//        public override DatabaseType.ECategory DatabaseCategory => DatabaseType.ECategory.Database;

//        private OracleConnection _connection; //used to for the datareader function

//        public override bool CanBulkLoad => true;

//        protected override async Task<ReturnValue> WriteDataBulkInner(DbDataReader reader, Table table = false)
//        {
//            try
//            {
//                ReturnValue<OracleConnection> connection = await NewConnection();
//                if (connection.Success == false)
//                {
//                    return connection;
//                }
//                //OracleBulkCopy 
//                //SqlBulkCopy bulkCopy = new SqlBulkCopy(connection.Value)
//                //{
//                //    DestinationTableName = DatabaseTableName(table, rejectedTable)
//                //};

//                //await bulkCopy.WriteToServerAsync(reader);

//                connection.Value.Close();

//                return new ReturnValue(true, "", null);
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue(false, "The following error occurred in the bulkload processing: " + ex.Message, ex);
//            }
//        }

//        /// <summary>
//        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
//        /// </summary>
//        /// <returns></returns>
//        public override async Task<ReturnValue> CreateManagedTable(Table table, bool dropTable = false)
//        {
//            try
//            {
//                ReturnValue<OracleConnection> connectionResult = await NewConnection();
//                if (connectionResult.Success == false)
//                {
//                    return connectionResult;
//                }

//                OracleConnection connection = connectionResult.Value;

//                List<Table> tables = new List<Table> {table};

//                if (!string.IsNullOrEmpty(table.RejectedTableName))
//                {
//                    tables.Add(table.GetRejectedTable());
//                }

//                foreach (Table table1 in tables)
//                {
//                    string tableName = DatabaseTableName(table1, false);

//                    OracleCommand cmd = new OracleCommand("select name from sys.tables where object_id = OBJECT_ID(@NAME)", connection);
//                    cmd.Parameters.Add("@NAME", SqlDbType.VarChar);
//                    cmd.Parameters["@NAME"].Value = tableName;

//                    object tableExists = null;
//                    try
//                    {
//                        tableExists = await cmd.ExecuteScalarAsync();
//                    }
//                    catch (Exception ex)
//                    {
//                        return new ReturnValue<List<string>>(false, "The sql server 'get tables' query could not be run due to the following error: " + ex.Message, ex);
//                    }

//                    if (tableExists != null && dropTable == false)
//                    {
//                        return new ReturnValue(false, "The table " + tableName + " already exists on the underlying database.  Please drop the table first.", null);
//                    }

//                    OracleCommand command;
//                    if (tableExists != null)
//                    {
//                        command = new OracleCommand("drop table " + SqlEscape(tableName), connection);
//                        try
//                        {
//                            await command.ExecuteNonQueryAsync();
//                        }
//                        catch (Exception ex)
//                        {
//                            return new ReturnValue(false, "The following error occurred when attempting to drop the table " + table1.TableName + ".  " + ex.Message, ex);
//                        }
//                    }

//                    StringBuilder createSql = new StringBuilder();

//                    //Create the table
//                    createSql.Append("create table " + tableName + " ( ");
//                    foreach (TableColumn col in table1.Columns)
//                    {
//                        createSql.Append("[" + SqlEscape(col.ColumnName) + "] " + GetSqlType(col.DataType, col.MaxLength, col.Scale, col.Precision) + " ");
//                        if (col.AllowDbNull == false)
//                            createSql.Append("NOT NULL");
//                        else
//                            createSql.Append("NULL");

//                        createSql.Append(",");
//                    }
//                    //remove the last comma
//                    createSql.Remove(createSql.Length - 1, 1);
//                    createSql.Append(")");

//                    //Add the primary key
//                    TableColumn key = table1.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);

//                    if (key != null && table1.IsRejected == false)
//                    {
//                        createSql.Append("ALTER TABLE " + tableName + " ADD CONSTRAINT [PK_" + tableName.Substring(1, tableName.Length - 2) + "] PRIMARY KEY CLUSTERED ([" + SqlEscape(key.ColumnName) + "])");
//                    }

//                    command = connectionResult.Value.CreateCommand();
//                    command.CommandText = createSql.ToString();
//                    try
//                    {
//                        await command.ExecuteNonQueryAsync();
//                    }
//                    catch (Exception ex)
//                    {
//                        return new ReturnValue(false, "The following error occurred when attempting to create the table " + table1.TableName + ".  " + ex.Message, ex);
//                    }
//                }

//                connectionResult.Value.Close();

//                return new ReturnValue(true, "", null);
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue(false, "An error occurred creating the table " + table.TableName + ".  " + ex.Message, ex);
//            }
//        }

//        /// <summary>
//        /// This will add any escape charaters to sql name or value to ensure sql injection is avoided.
//        /// </summary>
//        /// <param name="value"></param>
//        /// <returns></returns>
//        public string SqlEscape(string value)
//        {
//            return value.Replace("'", "''");
//        }

//        /// <summary>
//        /// Appends the connection key to the talbe to ensure table names are unique for connections.
//        /// </summary>
//        /// <returns></returns>
//        public override string DatabaseTableName(Table table, bool isRejectTable)
//        {
//            string newTableName;
//            if (isRejectTable == false)
//                newTableName = table.TableName;
//            else
//                newTableName = table.RejectedTableName;

//            if (newTableName.Substring(0, 1) == "[")
//                newTableName = newTableName.Substring(1, newTableName.Length - 2);

//            if (Purpose == EConnectionPurpose.Managed && EmbedTableKey)
//                return "[" + table.TableKey.ToString() + "_" + SqlEscape(newTableName) + "]";
//            else
//                return "[" + SqlEscape(newTableName) + "]";
//        }

//        private string GetSqlType(ETypeCode dataType, int? length, int? scale, int? precision)
//        {
//            string sqlType;

//            switch (dataType)
//            {
//                case ETypeCode.Int32:
//                    sqlType = "int";
//                    break;
//                case ETypeCode.Byte:
//                    sqlType = "tinyint";
//                    break;
//                case ETypeCode.Int16:
//                    sqlType = "smallint";
//                    break;
//                case ETypeCode.Int64:
//                    sqlType = "bigint";
//                    break;
//                case ETypeCode.String:
//                    if(length == null)
//                        sqlType = "nvarchar(max)";
//                    else
//                        sqlType = "nvarchar(" + length.ToString() + ")";
//                    break;
//                case ETypeCode.Double:
//                    sqlType = "float";
//                    break;
//                case ETypeCode.Boolean:
//                    sqlType = "bit";
//                    break;
//                case ETypeCode.DateTime:
//                    sqlType = "datetime";
//                    break;
//                case ETypeCode.Time:
//                    sqlType = "time(7)";
//                    break;
//                //case TypeCode.TimeSpan:
//                //    SQLType = "time(7)";
//                //    break;
//                case ETypeCode.Unknown:
//                    sqlType = "nvarchar(max)";
//                    break;
//                case ETypeCode.Decimal:
//                    if (precision.ToString() == "" || scale.ToString() == "")
//                        sqlType = "decimal";
//                    else
//                        sqlType = "decimal (" + precision.ToString() + "," + scale.ToString() + ")";
//                    break;
//                default:
//                    throw new Exception("The datatype " +dataType.ToString() + " is not compatible with the create table.");
//            }

//            return sqlType;
//        }


//        /// <summary>
//        /// Gets the start quote to go around the values in sql insert statement based in the column type.
//        /// </summary>
//        /// <returns></returns>
//        public string GetSqlFieldValueQuote(ETypeCode type, object value)
//        {
//            string returnValue;

//            if (value.GetType().ToString() == "System.DBNull")
//                return "null";

//            switch (type)
//            {
//                case ETypeCode.Byte:
//                case ETypeCode.Int16:
//                case ETypeCode.Int32:
//                case ETypeCode.Int64:
//                case ETypeCode.SByte:
//                case ETypeCode.UInt16:
//                case ETypeCode.UInt32:
//                case ETypeCode.UInt64:
//                case ETypeCode.Double:
//                case ETypeCode.Decimal:
//                    returnValue = SqlEscape(value.ToString());
//                    break;
//                case ETypeCode.String:
//                case ETypeCode.Boolean:
//                case ETypeCode.Unknown:
//                    returnValue = "'" + SqlEscape(value.ToString()) + "'";
//                    break;
//                case ETypeCode.DateTime:
//                    if(value is DateTime)
//                        returnValue = "convert(datetime, '" + SqlEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "')";
//                    else
//                        returnValue = "convert(datetime, '" + SqlEscape((string)value) + "')";
//                    break;
//                case ETypeCode.Time:
//                    if (value is TimeSpan)
//                        returnValue = "convert(time, '" + SqlEscape(((TimeSpan)value).ToString("HH:mm:ss.ff")) + "')";
//                    else
//                        returnValue = "convert(time, '" + SqlEscape((string)value) + "')";
//                    break;
//                default:
//                    throw new Exception("The datatype " + type.ToString() + " is not compatible with the create table.");
//            }

//            return returnValue;
//        }

//        public string GetSqlCompare(Filter.ECompare compare)
//        {
//            switch(compare)
//            {
//                case Filter.ECompare.EqualTo: return "="; 
//                case Filter.ECompare.GreaterThan: return ">";
//                case Filter.ECompare.GreaterThanEqual: return ">=";
//                case Filter.ECompare.LessThan: return "<";
//                case Filter.ECompare.LessThanEqual: return "<=";
//                case Filter.ECompare.NotEqual: return "!=";
//                default:
//                    return "";
//            }
//        }

//        private string ConnectionString
//        {
//            get
//            {
//                string con;
//                if (NtAuthentication == false)
//                    con = "Data Source=" + ServerName + "; User Id=" + UserName + "; Password=" + Password + ";Initial Catalog=" + DefaultDatabase;
//                else
//                    con = "Data Source=" + ServerName + "; Trusted_Connection=True;Initial Catalog=" + DefaultDatabase;
//                return con;
//            }
//        }

//        public override bool CanRunQueries => true;

//        [JsonIgnore]
//        public override bool PrefersSort
//        {
//            get
//            {
//                throw new NotImplementedException();
//            }
//        }

//        [JsonIgnore]
//        public override bool RequiresSort
//        {
//            get
//            {
//                throw new NotImplementedException();
//            }
//        }

//        private async Task<ReturnValue<OracleConnection>> NewConnection()
//        {
//            try
//            {
//                OracleConnection connection = new OracleConnection(ConnectionString);
//                await connection.OpenAsync();
//                State = (EConnectionState)connection.State;

//                if (connection.State != ConnectionState.Open)
//                {
//                    return new ReturnValue<OracleConnection>(false, "The oracle connection failed to open with a state of : " + connection.State.ToString(), null, null);
//                }
//                return new ReturnValue<OracleConnection>(true, "", null, connection);
//            }
//            catch(Exception ex)
//            {
//                return new ReturnValue<OracleConnection>(false, "The sqlserver connection failed with the following message: " + ex.Message, null, null);
//            }
//        }

//        public override async Task<ReturnValue> CreateDatabase()
//        {
//            try
//            {
//                string NewDatabase = DefaultDatabase;
//                DefaultDatabase = "";
//                ReturnValue<OracleConnection> connection = await NewConnection();

//                if (connection.Success == false)
//                {
//                    return new ReturnValue<List<string>>(connection.Success, connection.Message, connection.Exception, null);
//                }

//                OracleCommand cmd = new OracleCommand("create database [" + SqlEscape(NewDatabase) + "]", connection.Value);
//                int value = await cmd.ExecuteNonQueryAsync();

//                connection.Value.Close();

//                DefaultDatabase = NewDatabase;

//                return new ReturnValue(true);
//            }
//            catch(Exception ex)
//            {
//                return new ReturnValue<List<string>>(false, "Error creating database " + DefaultDatabase + ".   " + ex.Message, ex);
//            }
//        }

//        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
//        {
//            try
//            {
//                ReturnValue<OracleConnection> connection = await NewConnection();
//                if (connection.Success == false)
//                {
//                    return new ReturnValue<List<string>>(connection.Success, connection.Message, connection.Exception, null);
//                }

//                OracleCommand cmd = new OracleCommand("SELECT name FROM sys.databases where name NOT IN ('master', 'tempdb', 'model', 'msdb') order by name", connection.Value);
//                DbDataReader reader;
//                try
//                {
//                    reader = await cmd.ExecuteReaderAsync();
//                }
//                catch (Exception ex)
//                {
//                    return new ReturnValue<List<string>>(false, "The sql server 'get databases' query could not be run due to the following error: " + ex.Message, ex);
//                }

//                List<string> list = new List<string>();

//                while (await reader.ReadAsync())
//                {
//                    list.Add((string)reader["name"]);
//                }

//                connection.Value.Close();
//                return new ReturnValue<List<string>>(true, "", null,  list);
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<List<string>>(false, "The databases could not be listed due to the following error: " + ex.Message, ex, null);
//            }
//        }

//        public override async Task<ReturnValue<List<string>>> GetTableList()
//        {
//            try
//            {
//                ReturnValue<OracleConnection> connection = await NewConnection();
//                if (connection.Success == false)
//                {
//                    return new ReturnValue<List<string>>(connection.Success, connection.Message, connection.Exception, null);
//                }

//                OracleCommand cmd = new OracleCommand("SELECT * FROM INFORMATION_SCHEMA.Tables where TABLE_TYPE='BASE TABLE' order by TABLE_NAME", connection.Value);
//                DbDataReader reader;
//                try
//                {
//                    reader = await cmd.ExecuteReaderAsync();
//                }
//                catch (Exception ex)
//                {
//                    return new ReturnValue<List<string>>(false, "The sql server 'get tables' query could not be run due to the following error: " + ex.Message, ex);
//                }

//                List<string> tableList = new List<string>();

//                while(await reader.ReadAsync())
//                {
//                    tableList.Add("[" + reader["TABLE_SCHEMA"] + "].[" + reader["TABLE_NAME"] + "]");
//                }

//                reader.Dispose();

//                connection.Value.Close();

//                return new ReturnValue<List<string>>(true, "", null, tableList);
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<List<string>>(false, "The database tables could not be listed due to the following error: " + ex.Message, ex, null);
//            }
//        }

//        public override async Task<ReturnValue> GetSourceTableInfo(Table table, FileFormat fileFormat, Stream fileStream)
//        {
//            try
//            {
//                ReturnValue<OracleConnection> connection = await NewConnection();
//                if (connection.Success == false)
//                {
//                    return new ReturnValue(connection.Success, connection.Message, connection.Exception);
//                }

//                DbDataReader reader;

//                // The schema table description if it exists
//                OracleCommand cmd = new OracleCommand(@"select value 'Description' 
//                            FROM sys.extended_properties
//                            WHERE minor_id = 0 and class = 1 and name = 'MS_Description' and
//                            major_id = OBJECT_ID('" + DatabaseTableName(table, false) + "')" 
//                , connection.Value);

//                try
//                {
//                    reader = await cmd.ExecuteReaderAsync();
//                }
//                catch (Exception ex)
//                {
//                    return new ReturnValue(false, "The source sqlserver table + " + table.TableName + " could have a select query run against it with the following error: " + ex.Message, ex);
//                }

//                if (await reader.ReadAsync())
//                {
//                    table.Description = (string)reader["Description"];
//                }
//                else
//                {
//                    table.Description = "";
//                }

//                reader.Dispose();

//                //The new datatable that will contain the table schema
//                table.Columns.Clear();
//                table.ConnectionKey = ConnectionKey;
//                table.IsSorted = false;

//                // The schema table 
//                cmd = new OracleCommand(@"
//                         SELECT c.column_id, c.name 'ColumnName', t2.Name 'DataType', c.max_length 'MaxLength', c.precision 'Precision', c.scale 'Scale', c.is_nullable 'IsNullable', ep.value 'Description',
//                        case when exists(select * from sys.index_columns ic JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id where ic.object_id = c.object_id and ic.column_id = c.column_id and is_primary_key = 1) then 1 else 0 end 'PrimaryKey'
//                        FROM sys.columns c
//                        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
//						INNER JOIN sys.types t2 on t.system_type_id = t2.user_type_id 
//                        LEFT OUTER JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id and ep.name = 'MS_Description' and ep.class = 1 
//                        WHERE c.object_id = OBJECT_ID('" + DatabaseTableName(table, false) + "') "
//                        , connection.Value);

//                try
//                {
//                    reader = await cmd.ExecuteReaderAsync();
//                }
//                catch(Exception ex)
//                {
//                    return new ReturnValue(false, "The source sqlserver table + " + table.TableName + " could have a select query run against it with the following error: " + ex.Message, ex);
//                }

//                table.LogicalName = table.BaseTableName();

//                while (await reader.ReadAsync())
//                {
//                    TableColumn col = new TableColumn(table);

//                    //add the basic properties
//                    col.ColumnName = reader["ColumnName"].ToString();
//                    col.LogicalName = reader["ColumnName"].ToString();
//                    col.IsInput = false;
//                    col.DataType = ConvertSqlToTypeCode(reader["DataType"].ToString());
//                    if (col.DataType == ETypeCode.Unknown)
//                    {
//                        col.DeltaType = TableColumn.EDeltaType.IgnoreField;
//                    }
//                    else
//                    {
//                        //add the primary key
//                        if (Convert.ToBoolean(reader["PrimaryKey"]) == true)
//                            col.DeltaType = TableColumn.EDeltaType.NaturalKey;
//                        else
//                            col.DeltaType = TableColumn.EDeltaType.TrackingField;
//                    }

//                    if (col.DataType == ETypeCode.String)
//                        col.MaxLength = ConvertSqlMaxLength(reader["DataType"].ToString(), Convert.ToInt32(reader["MaxLength"]));
//                    else if (col.DataType == ETypeCode.Double || col.DataType == ETypeCode.Decimal )
//                    {
//                        col.Precision = Convert.ToInt32(reader["Precision"]);
//                        if ((string)reader["DataType"] == "money" || (string)reader["DataType"] == "smallmoney") // this is required as bug in sqlschematable query for money types doesn't get proper scale.
//                            col.Scale = 4;
//                        else
//                            col.Scale = Convert.ToInt32(reader["Scale"]);
//                    }

//                    //make anything with a large string unlimited.  This will be created as varchar(max)
//                    if (col.MaxLength > 4000)
//                        col.MaxLength = null;


//                    col.Description = reader["Description"].ToString();
//                    col.AllowDbNull = Convert.ToBoolean(reader["IsNullable"]);
//                    //col.IsUnique = Convert.ToBoolean(reader["IsUnique"]);
//                    table.Columns.Add(col);
//                }

//                reader.Dispose();
//                connection.Value.Close();


//                return new ReturnValue(true);
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue(false, "The source sqlserver table + " + table.TableName + " could not be read due to the following error: " + ex.Message, ex);
//            }
//        }

//        private ETypeCode ConvertSqlToTypeCode(string SqlType)
//        {
//            switch(SqlType)
//            {
//                case "bigint": return ETypeCode.Int64;
//                case "binary": return ETypeCode.Unknown;
//                case "bit": return ETypeCode.Boolean;
//                case "char": return ETypeCode.String;
//                case "date": return ETypeCode.DateTime;
//                case "datetime": return ETypeCode.DateTime;
//                case "datetime2": return ETypeCode.DateTime;
//                case "datetimeoffset": return ETypeCode.Time;
//                case "decimal": return ETypeCode.Decimal;
//                case "float": return ETypeCode.Double;
//                case "image": return ETypeCode.Unknown;
//                case "int": return ETypeCode.Int32;
//                case "money": return ETypeCode.Decimal;
//                case "nchar": return ETypeCode.String;
//                case "ntext": return ETypeCode.String;
//                case "numeric": return ETypeCode.Decimal;
//                case "nvarchar": return ETypeCode.String;
//                case "real": return ETypeCode.Single;
//                case "rowversion": return ETypeCode.Unknown;
//                case "smalldatetime": return ETypeCode.DateTime;
//                case "smallint": return ETypeCode.Int16;
//                case "smallmoney": return ETypeCode.Int16;
//                case "text": return ETypeCode.String;
//                case "time": return ETypeCode.Time;
//                case "timestamp": return ETypeCode.Int64;
//                case "tinyint": return ETypeCode.Byte;
//                case "uniqueidentifier": return ETypeCode.String;
//                case "varbinary": return ETypeCode.Unknown;
//                case "varchar": return ETypeCode.String;
//                case "xml": return ETypeCode.String;
//            }
//            return ETypeCode.Unknown;
//        }

//        public int? ConvertSqlMaxLength(string sqlType, int byteLength)
//        {
//            if (byteLength == -1)
//                return null;

//            switch (sqlType)
//            {
//                case "char":
//                case "varchar": return byteLength;
//                case "nchar":
//                case "nvarchar": return byteLength / 2;
//            }

//            return null;
//        }



//        private string BuildSelectQuery(Table table, SelectQuery query)
//        {
//            StringBuilder sql = new StringBuilder();

//            sql.Append("select ");
//            sql.Append("[" + String.Join("],[", query.Columns.ToArray()) + "] ");
//            sql.Append("from " + DatabaseTableName(table, rejectedTable) + " WITH (NOLOCK) ");
//            sql.Append(BuildFiltersString(query.Filters));

//            if (query.Groups?.Count > 0)
//            {
//                sql.Append("group by ");
//                sql.Append("[" + String.Join("],[", query.Groups.Select(c=>SqlEscape(c)).ToArray()) + "] ");
//            }
//            if (query.Sorts?.Count > 0)
//            {
//                sql.Append("order by ");
//                sql.Append(String.Join(",", query.Sorts.Select(c => "[" + SqlEscape(c.Column) + "] " + (c.Direction == Sort.EDirection.Descending ? " desc" : "" )).ToArray())) ;
//            }

//            return sql.ToString();
//        }

//        private string BuildFiltersString(List<Filter> filters)
//        {
//            if (filters == null || filters.Count == 0)
//                return "";
//            else
//            {

//                StringBuilder sql = new StringBuilder();
//                sql.Append("where ");

//                foreach (var filter in filters)
//                {
//                    if (filter.Column1 != null)
//                        sql.Append(" [" + SqlEscape(filter.Column1) + "] ");
//                    else
//                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value1) + " ");

//                    sql.Append(GetSqlCompare(filter.Operator));

//                    if (filter.Column2 != null)
//                        sql.Append(" [" + SqlEscape(filter.Column2) + "] ");
//                    else
//                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value2) + " ");

//                    sql.Append(filter.AndOr.ToString());
//                }

//                sql.Remove(sql.Length - 3, 3); //remove last or/and

//                return sql.ToString();
//            }
//        }

//        protected override async Task<ReturnValue> DataReaderStartQueryInner(Table table, SelectQuery query)
//        {
//            if (OpenReader)
//            {
//                return new ReturnValue(false, "The current connection is already open.", null);
//            }

//            Table = table;

//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return connection;
//            }

//            _connection = connection.Value;

//            OracleCommand cmd = new OracleCommand(BuildSelectQuery(table, query, rejectedTable), _connection);

//            try
//            {
//                InReader = await cmd.ExecuteReaderAsync();
//            }
//            catch(Exception ex)
//            {
//                return new ReturnValue(false, "The connection reader for the sqlserver table " + table.TableName + " could failed due to the following error: " + ex.Message, ex);
//            }

//            if (InReader == null)
//            {
//                return new ReturnValue(false, "The connection reader for the sqlserver table " + table.TableName + " return null for an unknown reason.", null);
//            }
//            else
//            {
//                OpenReader = true;
//                return new ReturnValue(true, "", null);
//            }
//        }

//        public override async Task<ReturnValue<int>> ExecuteUpdateQuery(Table table, UpdateQuery query)
//        {
//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
//            }

//            StringBuilder sql = new StringBuilder();

//            sql.Append("update " + DatabaseTableName(table, rejectedTable) + " set ");

//            foreach (QueryColumn column in query.UpdateColumns)
//                sql.Append("[" + SqlEscape(column.Column) + "] = " + GetSqlFieldValueQuote(column.ColumnType, column.Value) + ",");
//            sql.Remove(sql.Length - 1, 1); //remove last comma
//            sql.Append(" " + BuildFiltersString(query.Filters));

//            //  Retrieving schema for columns from a single table
//            OracleCommand cmd = new OracleCommand(sql.ToString(), connection.Value);

//            int rows;
//            try
//            {
//                rows = await cmd.ExecuteNonQueryAsync();
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<int>(false, "The sql server update query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, -1);
//            }

//            connection.Value.Close();
//            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
//        }

//        public override async Task<ReturnValue<int>> ExecuteDeleteQuery(Table table, DeleteQuery query)
//        {
//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
//            }

//            StringBuilder sql = new StringBuilder();

//            sql.Append("delete from " + DatabaseTableName(table, rejectedTable) + " ");
//            sql.Append(BuildFiltersString(query.Filters));

//            OracleCommand cmd = new OracleCommand(sql.ToString(), connection.Value);

//            int rows;
//            try
//            {
//                rows = await cmd.ExecuteNonQueryAsync();
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<int>(false, "The sql server delete query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, -1);
//            }

//            connection.Value.Close();
//            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
//        }

//        public override async Task<ReturnValue<int>> ExecuteInsertQuery(Table table, InsertQuery query)
//        {
//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
//            }

//            StringBuilder insert = new StringBuilder();
//            StringBuilder values = new StringBuilder();

//            insert.Append("INSERT INTO " + DatabaseTableName(table, rejectedTable) + " (");
//            values.Append("VALUES (");

//            for (int i = 0; i < query.InsertColumns.Count; i++)
//            {
//                insert.Append("[" + query.InsertColumns[i].Column + "],");
//                values.Append("@col" + i.ToString() + ",");
//            }

//            string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " + values.Remove(values.Length - 1, 1).ToString() + ");";

//            int rows;
//            try
//            {
//                using (var cmd = connection.Value.CreateCommand())
//                {
//                    cmd.CommandText = insertCommand;
//                    for (int i = 0; i < query.InsertColumns.Count; i++)
//                    {
//                        cmd.Parameters.Add("@col" + i.ToString(), query.InsertColumns[i].Value);
//                    }
//                    rows = cmd.ExecuteNonQuery();
//                }
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<int>(false, "The sql server insert query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + insertCommand?.ToString(), ex, -1);
//            }

//            connection.Value.Close();
//            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
//        }

//        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query)
//        {
//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return new ReturnValue<object>(false, connection.Message, connection.Exception, null);
//            }

//            //  Retrieving schema for columns from a single table
//            OracleCommand cmd = new OracleCommand(BuildSelectQuery(table, query, rejectedTable), connection.Value);
//            object value;
//            try
//            {
//                value = await cmd.ExecuteScalarAsync();
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<object>(false, "The sql server select query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, null);
//            }

//            connection.Value.Close();
//            return new ReturnValue<object>(true, "", null, value); //sometimes reader returns -1, when we want this to be error condition.
//        }

//        public override async Task<ReturnValue> TruncateTable(Table table)
//        {
//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return connection;
//            }

//            OracleCommand cmd = new OracleCommand("truncate table " + DatabaseTableName(table, rejectedTable), connection.Value);
//            try
//            {
//                await cmd.ExecuteNonQueryAsync();
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue(false, "The sql server update query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex);
//            }

//            connection.Value.Close();

//            //if(rows == -1)
//            //    return new ReturnValue(false, "The sql server truncate table query for " + Table.TableName + " could appears to have failed for an unknown reason." , null);
//            //else
//            return new ReturnValue(true, "", null);
//        }

//        public override string GetCurrentFile()
//        {
//            throw new NotImplementedException();
//        }

//         public override bool ResetValues()
//        {
//            throw new NotImplementedException();
//        }

//        public override bool Initialize()
//        {
//            throw new NotImplementedException();
//        }

//        public override string Details()
//        {
//            StringBuilder details = new StringBuilder();
//            details.AppendLine("<b>Source</b> <br />");
//            details.AppendLine("<b>Database</b>: SQL Server<br />");
//            details.AppendLine("<b>Table</b>: " + Table.TableName + "<br />");
//            details.AppendLine("<b>SQL</b>: " + BuildSelectQuery(Table, SelectQuery, false));
//            return details.ToString();
//        }

//        public override List<Sort> RequiredSortFields()
//        {
//            throw new NotImplementedException();
//        }

//        public override List<Sort> RequiredJoinSortFields()
//        {
//            throw new NotImplementedException();
//        }

//        public override List<Sort> OutputSortFields()
//        {
//            throw new NotImplementedException();
//        }

//        public override async Task<ReturnValue<object>> GetMaxValue(Table table, string columnName)
//        {
//            ReturnValue<OracleConnection> connection = await NewConnection();
//            if (connection.Success == false)
//            {
//                return new ReturnValue<object>(false, connection.Message, connection.Exception, null);
//            }

//            string sql = "select max(" + SqlEscape(columnName) + ") from " + DatabaseTableName(table, rejectedTable) ;
//            OracleCommand cmd = new OracleCommand(sql, connection.Value);

//            object value;
//            try
//            {
//                value = await cmd.ExecuteScalarAsync();
//            }
//            catch (Exception ex)
//            {
//                return new ReturnValue<object>(false, "The sql server select query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex, null);
//            }

//            connection.Value.Close();

//            return new ReturnValue<object>(true, "", null, value is int ? (int)value + 1 : 1);
//        }

//        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
//        {
//            return new ReturnValue(true, "", null);
//        }

//        public override async Task<ReturnValue> DataWriterStart(Table table)
//        {
//            return new ReturnValue(true, "", null);
//        }

//        public override async Task<ReturnValue> DataWriterFinish(Table table)
//        {
//            return new ReturnValue(true, "", null);
//        }

//        public override Task<ReturnValue> LookupRow(List<Filter> filters)
//        {
//            throw new NotImplementedException();
//        }

//        protected override bool ReadRecord()
//        {
//            if(InReader == null)
//                throw new Exception("The sql server reader has not been set.");

//            try
//            {
//                bool result = InReader.Read();
//                if (result == false && _connection != null && _connection.State == ConnectionState.Open)
//                    _connection.Close();

//                return result;
//            }
//            catch(Exception ex)
//            {
//                throw new Exception( "The sql server reader failed due to the following error: " + ex.Message, ex);
//            }
//        }


//    }
//}
