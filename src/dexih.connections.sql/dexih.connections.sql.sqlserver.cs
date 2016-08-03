using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
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
    public class ConnectionSqlServer : ConnectionSql
    {

        public override string ServerHelp => "Server Name";
        public override string DefaultDatabaseHelp => "Database";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override string DatabaseTypeName => "SQL Server";
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

        public override string SqlSelectNoLock { get; } = "WITH (NOLOCK)";

        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                var timer = Stopwatch.StartNew();

                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<long>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, timer.ElapsedTicks);
                }

                using (SqlConnection sqlConnection = (SqlConnection)connectionResult.Value)
                {

                    SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection)
                    {
                        DestinationTableName = table.TableName
                    };

                    await bulkCopy.WriteToServerAsync(reader, cancelToken);

                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Insert rows cancelled.", null, timer.ElapsedTicks);
                }

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, "The following error occurred in the bulkload processing: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<bool>(connectionResult);
            }

            using (DbConnection connection = connectionResult.Value)
            using (DbCommand cmd = CreateCommand(connection, "select name from sys.tables where object_id = OBJECT_ID(@NAME)"))
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
                createSql.Append("create table " + AddDelimiter(table.TableName) + " ( ");
                foreach (TableColumn col in table.Columns)
                {
                    createSql.Append(AddDelimiter(col.ColumnName) + " " + GetSqlType(col.DataType, col.MaxLength, col.Scale, col.Precision) + " ");
                    if (col.AllowDbNull == false)
                        createSql.Append("NOT NULL");
                    else
                        createSql.Append("NULL");

                    createSql.Append(",");
                }
                //remove the last comma
                createSql.Remove(createSql.Length - 1, 1);
                createSql.Append(")");

                //Add the primary key
                TableColumn key = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
                if (key != null)
                    createSql.Append("ALTER TABLE " + AddDelimiter(table.TableName) + " ADD CONSTRAINT [PK_" + AddEscape(table.TableName) + "] PRIMARY KEY CLUSTERED ([" + AddEscape(key.ColumnName) + "])");

                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }

                using (DbConnection connection = connectionResult.Value)
                {
                    using (var cmd = connectionResult.Value.CreateCommand())
                    {
                        cmd.CommandText = createSql.ToString();
                        try
                        {
                            await cmd.ExecuteNonQueryAsync();
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.TableName + ".  " + ex.Message, ex);
                        }
                    }

                    //run a query to get the schema name and also check the table has been created.
                    object schemaName = null;
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT s.name SchemaName FROM sys.tables AS t INNER JOIN sys.schemas AS s ON t.[schema_id] = s.[schema_id] where object_id = OBJECT_ID(@NAME)";
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", table.TableName));

                        try
                        {
                            schemaName = await cmd.ExecuteScalarAsync();
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue<List<string>>(false, "The sql server 'get tables' query could not be run due to the following error: " + ex.Message, ex);
                        }

                        if (schemaName == null)
                        {
                            return new ReturnValue(false, "The table " + table.TableName + " was not correctly created.  The reason is unknown.", null);
                        }
                    }

                    try
                    {
                        //Add the table description
                        if (!string.IsNullOrEmpty(table.Description))
                        {
                            using (var cmd = connectionResult.Value.CreateCommand())
                            {
                                cmd.CommandText = "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=@description , @level0type=N'SCHEMA',@level0name=@schemaname, @level1type=N'TABLE',@level1name=@tablename";
                                cmd.Parameters.Add(CreateParameter(cmd, "@description", table.Description));
                                cmd.Parameters.Add(CreateParameter(cmd, "@schemaname", schemaName));
                                cmd.Parameters.Add(CreateParameter(cmd, "@tablename", table.TableName));
                                await cmd.ExecuteNonQueryAsync();
                            }
                        }

                        //Add the column descriptions
                        foreach (TableColumn col in table.Columns)
                        {
                            if (!string.IsNullOrEmpty(col.Description))
                            {
                                using (var cmd = connectionResult.Value.CreateCommand())
                                {
                                    cmd.CommandText = "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=@description , @level0type=N'SCHEMA',@level0name=@schemaname, @level1type=N'TABLE',@level1name=@tablename, @level2type=N'COLUMN',@level2name=@columnname";
                                    cmd.Parameters.Add(CreateParameter(cmd, "@description", col.Description));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@schemaname", schemaName));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@tablename", table.TableName));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@columnname", col.ColumnName));
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue(false, "The table " + table.TableName + " encountered an error when adding table/column descriptions: " + ex.Message, ex);
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
                        sqlType = "nvarchar(max)";
                    else
                        sqlType = "nvarchar(" + length.ToString() + ")";
                    break;
                case ETypeCode.Single:
                    sqlType = "float";
                    break;
                case ETypeCode.UInt64:
                    sqlType = "DECIMAL(20,0)";
                    break;
                case ETypeCode.Double:
                    sqlType = "float";
                    break;
                case ETypeCode.Boolean:
                    sqlType = "bit";
                    break;
                case ETypeCode.DateTime:
                    sqlType = "datetime";
                    break;
                case ETypeCode.Time:
                    sqlType = "time(7)";
                    break;
                case ETypeCode.Guid:
                    sqlType = "uniqueidentifier";
                    break;
                //case TypeCode.TimeSpan:
                //    SQLType = "time(7)";
                //    break;
                case ETypeCode.Unknown:
                    sqlType = "nvarchar(max)";
                    break;
                case ETypeCode.Decimal:
                    if (precision.ToString() == "")
                        precision = 28;
                    if (scale.ToString() == "")
                        scale = 0;
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

            if (value == null || value is DBNull)
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
                case ETypeCode.String:
                case ETypeCode.Guid:
                case ETypeCode.Boolean:
                case ETypeCode.Unknown:
                    returnValue = "'" + AddEscape(value.ToString()) + "'";
                    break;
                case ETypeCode.DateTime:
                    if (value is DateTime)
                        returnValue = "convert(datetime, '" + AddEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "')";
                    else
                        returnValue = "convert(datetime, '" + AddEscape((string)value) + "')";
                    break;
                case ETypeCode.Time:
                    if (value is TimeSpan)
                        returnValue = "convert(time, '" + AddEscape(((TimeSpan)value).ToString()) + "')";
                    else
                        returnValue = "convert(time, '" + AddEscape((string)value) + "')";
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
                    if (NtAuthentication == false)
                        connectionString = "Data Source=" + ServerName + "; User Id=" + UserName + "; Password=" + Password + ";Initial Catalog=" + DefaultDatabase;
                    else
                        connectionString = "Data Source=" + ServerName + "; Trusted_Connection=True;Initial Catalog=" + DefaultDatabase;
                }

                SqlConnection connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    return new ReturnValue<DbConnection>(false, "The sqlserver connection failed to open with a state of : " + connection.State.ToString(), null, null);
                }
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
                DefaultDatabase = "";
                ReturnValue<DbConnection> connectionResult = await NewConnection();

                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<string>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                using (var connection = connectionResult.Value)
                using (DbCommand cmd = CreateCommand(connection, "create database " + AddDelimiter(databaseName)))
                {
                    int value = await cmd.ExecuteNonQueryAsync();
                }

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
                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<string>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                List<string> list = new List<string>();

                using (var connection = connectionResult.Value)
                using (DbCommand cmd = CreateCommand(connection, "SELECT name FROM sys.databases where name NOT IN ('master', 'tempdb', 'model', 'msdb') order by name"))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        list.Add((string)reader["name"]);
                    }
                }
                return new ReturnValue<List<string>>(true, "", null, list);
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

                List<string> tableList = new List<string>();

                using (var connection = connectionResult.Value)
                using (DbCommand cmd = CreateCommand(connection, "SELECT * FROM INFORMATION_SCHEMA.Tables where TABLE_TYPE='BASE TABLE' order by TABLE_NAME"))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tableList.Add(AddDelimiter(reader["TABLE_SCHEMA"].ToString()) + "." + AddDelimiter(reader["TABLE_NAME"].ToString()));
                    }
                }
                return new ReturnValue<List<string>>(true, "", null, tableList);
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
                Table table = new Table(tableName);

                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<Table>(connectionResult.Success, connectionResult.Message, connectionResult.Exception);
                }

                using (var connection = connectionResult.Value)
                {
                    using (DbCommand cmd = CreateCommand(connection, @"select value 'Description' 
                            FROM sys.extended_properties
                            WHERE minor_id = 0 and class = 1 and name = 'MS_Description' and
                            major_id = OBJECT_ID('" + AddEscape(tableName) + "')"))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            table.Description = (string)reader["Description"];
                        }
                        else
                        {
                            table.Description = "";
                        }

                    }

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"
                         SELECT c.column_id, c.name 'ColumnName', t2.Name 'DataType', c.Max_Length 'Max_Length', c.precision 'Precision', c.scale 'Scale', c.is_nullable 'IsNullable', ep.value 'Description',
                        case when exists(select * from sys.index_columns ic JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id where ic.object_id = c.object_id and ic.column_id = c.column_id and is_primary_key = 1) then 1 else 0 end 'PrimaryKey'
                        FROM sys.columns c
                        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
						INNER JOIN sys.types t2 on t.system_type_id = t2.user_type_id 
                        LEFT OUTER JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id and ep.name = 'MS_Description' and ep.class = 1 
                        WHERE c.object_id = OBJECT_ID('" + AddEscape(tableName) + "') "
                            ))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {

                        //for the logical, just trim out any "
                        table.LogicalName = table.TableName.Replace("\"", "");

                        while (await reader.ReadAsync())
                        {
                            TableColumn col = new TableColumn();

                            //add the basic properties
                            col.ColumnName = reader["ColumnName"].ToString();
                            col.LogicalName = reader["ColumnName"].ToString();
                            col.IsInput = false;
                            col.DataType = ConvertSqlToTypeCode(reader["DataType"].ToString());
                            if (col.DataType == ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                if (Convert.ToBoolean(reader["PrimaryKey"]) == true)
                                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                                else
                                    col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            }

                            if (col.DataType == ETypeCode.String)
                                col.MaxLength = ConvertSqlMaxLength(reader["DataType"].ToString(), Convert.ToInt32(reader["Max_Length"]));
                            else if (col.DataType == ETypeCode.Double || col.DataType == ETypeCode.Decimal)
                            {
                                col.Precision = Convert.ToInt32(reader["Precision"]);
                                if ((string)reader["DataType"] == "money" || (string)reader["DataType"] == "smallmoney") // this is required as bug in sqlschematable query for money types doesn't get proper scale.
                                    col.Scale = 4;
                                else
                                    col.Scale = Convert.ToInt32(reader["Scale"]);
                            }

                            //make anything with a large string unlimited.  This will be created as varchar(max)
                            if (col.MaxLength > 4000)
                                col.MaxLength = null;


                            col.Description = reader["Description"].ToString();
                            col.AllowDbNull = Convert.ToBoolean(reader["IsNullable"]);
                            //col.IsUnique = Convert.ToBoolean(reader["IsUnique"]);
                            table.Columns.Add(col);
                        }
                    }
                }
                return new ReturnValue<Table>(true, table);
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
                case "bigint": return ETypeCode.Int64;
                case "binary": return ETypeCode.Unknown;
                case "bit": return ETypeCode.Boolean;
                case "char": return ETypeCode.String;
                case "date": return ETypeCode.DateTime;
                case "datetime": return ETypeCode.DateTime;
                case "datetime2": return ETypeCode.DateTime;
                case "datetimeoffset": return ETypeCode.Time;
                case "decimal": return ETypeCode.Decimal;
                case "float": return ETypeCode.Double;
                case "image": return ETypeCode.Unknown;
                case "int": return ETypeCode.Int32;
                case "money": return ETypeCode.Decimal;
                case "nchar": return ETypeCode.String;
                case "ntext": return ETypeCode.String;
                case "numeric": return ETypeCode.Decimal;
                case "nvarchar": return ETypeCode.String;
                case "real": return ETypeCode.Single;
                case "rowversion": return ETypeCode.Unknown;
                case "smalldatetime": return ETypeCode.DateTime;
                case "smallint": return ETypeCode.Int16;
                case "smallmoney": return ETypeCode.Int16;
                case "text": return ETypeCode.String;
                case "time": return ETypeCode.Time;
                case "timestamp": return ETypeCode.Int64;
                case "tinyint": return ETypeCode.Byte;
                case "uniqueidentifier": return ETypeCode.Guid;
                case "varbinary": return ETypeCode.Unknown;
                case "varchar": return ETypeCode.String;
                case "xml": return ETypeCode.String;
            }
            return ETypeCode.Unknown;
        }

        public int? ConvertSqlMaxLength(string sqlType, int byteLength)
        {
            if (byteLength == -1)
                return null;

            switch (sqlType)
            {
                case "char":
                case "varchar": return byteLength;
                case "nchar":
                case "nvarchar": return byteLength / 2;
            }

            return null;
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return connectionResult;
            }

            using (var connection = connectionResult.Value)
            using (DbCommand cmd = connection.CreateCommand())
            {

                cmd.CommandText = "truncate table " + AddDelimiter(table.TableName);

                try
                {
                    await cmd.ExecuteNonQueryAsync(cancelToken);
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue(false, "Truncate cancelled", null);
                }
                catch (Exception ex)
                {
                    return new ReturnValue(false, "The truncate table query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex);
                }
            }

            return new ReturnValue(true, "", null);
        }


    }
}
