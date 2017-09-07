using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using dexih.functions;
using System.Data.Common;
using static dexih.functions.DataType;
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

        protected override string SqlFromAttribute(Table table)
        {
            string sql = "";

            if (table.IsVersioned)
                sql = "FOR system_time all";

            sql = sql + " WITH(NOLOCK) ";

            return sql;
        }
        
        public override object GetDataTypeMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(9999,12,31);
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }
	    
        public override object GetDataTypeMinValue(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(1753,1,1);
                default:
                    return DataType.GetDataTypeMinValue(typeCode);
            }
		    
        }

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
                        DestinationTableName = SqlTableName(table)
                    };

                    bulkCopy.BulkCopyTimeout = 60;
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

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<bool>(connectionResult);
            }

            using (DbConnection connection = connectionResult.Value)
            using (DbCommand cmd = CreateCommand(connection, "select name from sys.tables where object_id = OBJECT_ID(@NAME)"))
            {
                cmd.Parameters.Add(CreateParameter(cmd, "@NAME", SqlTableName(table)));

                object tableExists = null;
                try
                {
                    tableExists = await cmd.ExecuteScalarAsync(cancelToken);
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
        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            try
            {
                var tableExistsResult = await TableExists(table, cancelToken);
                if (!tableExistsResult.Success)
                    return tableExistsResult;

                //if table exists, and the dropTable flag is set to false, then error.
                if (tableExistsResult.Value && dropTable == false)
                {
                    return new ReturnValue(false, "The table " + table.Name + " already exists on the underlying database.  Please drop the table first.", null);
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
                createSql.Append("create table " + SqlTableName(table) + " ( ");
                foreach (TableColumn col in table.Columns)
                {
                    createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col.Datatype, col.MaxLength, col.Scale, col.Precision));
                    if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                        createSql.Append(" IDENTITY(1,1)");
                    if (col.AllowDbNull == false)
                        createSql.Append(" NOT NULL");
                    else
                        createSql.Append(" NULL");

                    createSql.Append(",");
                }
                //remove the last comma
                createSql.Remove(createSql.Length - 1, 1);
                createSql.Append(")");

                //Add the primary key using surrogate key or autoincrement.
                TableColumn key = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
                if(key == null)
                {
                    key = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                }

                if (key != null)
                    createSql.Append("ALTER TABLE " + SqlTableName(table) + " ADD CONSTRAINT [PK_" + AddEscape(table.Name) + "] PRIMARY KEY CLUSTERED ([" + AddEscape(key.Name) + "])");

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
                            await cmd.ExecuteNonQueryAsync(cancelToken);
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.Name + ".  " + ex.Message, ex);
                        }
                    }

                    //run a query to get the schema name and also check the table has been created.
                    object schemaName = null;
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT s.name SchemaName FROM sys.tables AS t INNER JOIN sys.schemas AS s ON t.[schema_id] = s.[schema_id] where object_id = OBJECT_ID(@NAME)";
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", SqlTableName(table)));

                        try
                        {
                            schemaName = await cmd.ExecuteScalarAsync(cancelToken);
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue<List<string>>(false, "The sql server 'get tables' query could not be run due to the following error: " + ex.Message, ex);
                        }

                        if (schemaName == null)
                        {
                            return new ReturnValue(false, "The table " + table.Name + " was not correctly created.  The reason is unknown.", null);
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
                                cmd.Parameters.Add(CreateParameter(cmd, "@tablename", table.Name));
                                await cmd.ExecuteNonQueryAsync(cancelToken);
                            }
                        }

                        //Add the column descriptions
                        foreach (var col in table.Columns)
                        {
                            if (!string.IsNullOrEmpty(col.Description))
                            {
                                using (var cmd = connectionResult.Value.CreateCommand())
                                {
                                    cmd.CommandText = "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=@description , @level0type=N'SCHEMA',@level0name=@schemaname, @level1type=N'TABLE',@level1name=@tablename, @level2type=N'COLUMN',@level2name=@columnname";
                                    cmd.Parameters.Add(CreateParameter(cmd, "@description", col.Description));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@schemaname", schemaName));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@tablename", table.Name));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@columnname", col.Name));
                                    await cmd.ExecuteNonQueryAsync(cancelToken);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue(false, "The table " + table.Name + " encountered an error when adding table/column descriptions: " + ex.Message, ex);
                    }
                }

                return new ReturnValue(true, "", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "An error occurred creating the table " + table.Name + ".  " + ex.Message, ex);
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
                case ETypeCode.Binary:
                    if (length == null)
                        sqlType = "varbinary(max)";
                    else
                        sqlType = "varbinary(" + length.ToString() + ")";
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
                    throw new Exception("The datatype " + type.ToString() + " is not compatible with the sql statement.");
            }

            return returnValue;
        }

        public override async Task<ReturnValue<DbConnection>> NewConnection()
        {
            SqlConnection connection = null;

            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
                    if (UseWindowsAuth == false)
                        connectionString = "Data Source=" + Server + "; User Id=" + Username + "; Password=" + Password + ";Initial Catalog=" + DefaultDatabase;
                    else
                        connectionString = "Data Source=" + Server + "; Trusted_Connection=True;Initial Catalog=" + DefaultDatabase;
                }

                connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    return new ReturnValue<DbConnection>(false, "The sqlserver connection failed to open with a state of : " + connection.State.ToString(), null, null);
                }
                return new ReturnValue<DbConnection>(true, "", null, connection);
            }
            catch (Exception ex)
            {
                if(connection != null)
                    connection.Dispose();
                return new ReturnValue<DbConnection>(false, "The sqlserver connection failed with the following message: " + ex.Message, null, null);
            }
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
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
                    int value = await cmd.ExecuteNonQueryAsync(cancelToken);
                }

                DefaultDatabase = databaseName;

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "Error creating database " + DefaultDatabase + ".   " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
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
                using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                {
                    while (await reader.ReadAsync(cancelToken))
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

        public override async Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
        {
            try
            {
                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<Table>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                List<Table> tableList = new List<Table>();

                using (var connection = connectionResult.Value)
                {
                    int sqlversion = 0;
                    //get the sql server version.
                    using (DbCommand cmd = CreateCommand(connection, "SELECT SERVERPROPERTY('ProductVersion') AS ProductVersion"))
                    {
                        string fullversion = cmd.ExecuteScalar().ToString();

                        sqlversion = Convert.ToInt32(fullversion.Split('.')[0]);
                    }

                    using (DbCommand cmd = CreateCommand(connection, "SELECT * FROM INFORMATION_SCHEMA.Tables where TABLE_TYPE='BASE TABLE' order by TABLE_NAME"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                    {
                        while (await reader.ReadAsync(cancelToken))
                        {
							var table = new Table()
							{
								Name = reader["TABLE_NAME"].ToString(),
								Schema = reader["TABLE_SCHEMA"].ToString()
							};
                            tableList.Add(table);
                        }
                    }

                    if (sqlversion >= 13)
                    {
                        var newTableList = new List<Table>();

                        foreach (var table in tableList)
                        {
                            //select the temporal type 
                            using (DbCommand cmd = CreateCommand(connection, "select temporal_type from sys.tables where object_id = OBJECT_ID('" + SqlTableName(table) + "')"))
                            {
                                int temporalType = Convert.ToInt32(cmd.ExecuteScalar());
                                //Exclude history table from the list (temporalType = 1)
                                if (temporalType != 1)
                                    newTableList.Add(table);
                            }
                        }

                        tableList = newTableList;
                    }

                }
                return new ReturnValue<List<Table>>(true, "", null, tableList);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<Table>>(false, "The database tables could not be listed due to the following error: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            if (originalTable.UseQuery)
            {
                return await GetQueryTable(originalTable, cancelToken);
            }
            
            try
            {
                Table table = new Table(originalTable.Name, originalTable.Schema);
                var tableName = SqlTableName(table);

                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<Table>(connectionResult.Success, connectionResult.Message, connectionResult.Exception);
                }

                using (var connection = connectionResult.Value)
                {
                    int sqlversion = 0;

                    //get the sql server version.
                    using (DbCommand cmd = CreateCommand(connection, "SELECT SERVERPROPERTY('ProductVersion') AS ProductVersion"))
                    {
                        string fullversion = cmd.ExecuteScalar().ToString();

                        sqlversion = Convert.ToInt32(fullversion.Split('.')[0]);
                    }

                    //get the column descriptions.
                    using (DbCommand cmd = CreateCommand(connection, @"select value 'Description' 
                            FROM sys.extended_properties
                            WHERE minor_id = 0 and class = 1 and (name = 'MS_Description' or name = 'Description') and
                            major_id = OBJECT_ID('" + tableName + "')"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                    {
                        if (await reader.ReadAsync(cancelToken))
                        {
                            table.Description = (string)reader["Description"];
                        }
                        else
                        {
                            table.Description = "";
                        }
                    }

                    if (sqlversion >= 13)
                    {
                        //select the temporal type 
                        using (DbCommand cmd = CreateCommand(connection, "select temporal_type from sys.tables where object_id = OBJECT_ID('" + tableName + "')"))
                        {
                            int temporalType = Convert.ToInt32(cmd.ExecuteScalar());
                        //If the table is a temporarl table, mark it.
                        if (temporalType == 2)
                            table.IsVersioned = true;
                        }
                    }

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    string generatedAlwaysTypeColumn = "";

                    //if this is sql server 2016 or newer, check is the column is a temporal row_start or row_end column
                    if (sqlversion >= 13)
                    {
                        generatedAlwaysTypeColumn = "c.generated_always_type,";
                    }

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"
                         SELECT c.column_id, c.name 'ColumnName', t.Name 'DataType', c.Max_Length 'Max_Length', c.precision 'Precision', c.scale 'Scale', c.is_nullable 'IsNullable', ep.value 'Description', " + generatedAlwaysTypeColumn + 
                        @"case when exists(select * from sys.index_columns ic JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id where ic.object_id = c.object_id and ic.column_id = c.column_id and is_primary_key = 1) then 1 else 0 end 'PrimaryKey'
                        FROM sys.columns c
                        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                        LEFT OUTER JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id and (ep.name = 'MS_Description' or ep.name = 'Description') and ep.class = 1 
                        WHERE c.object_id = OBJECT_ID('" + tableName + "') "
                            ))
                    using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                    {

                        //for the logical, just trim out any "
                        table.LogicalName = table.Name.Replace("\"", "");

                        while (await reader.ReadAsync(cancelToken))
                        {
                            TableColumn col = new TableColumn();

                            //add the basic properties
                            col.Name = reader["ColumnName"].ToString();
                            col.LogicalName = reader["ColumnName"].ToString();
                            col.IsInput = false;
                            col.Datatype = ConvertSqlToTypeCode(reader["DataType"].ToString());
                            if (col.Datatype == ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                if (Convert.ToBoolean(reader["PrimaryKey"]))
                                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                                else
                                    col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            }

                            if (col.Datatype == ETypeCode.String)
                                col.MaxLength = ConvertSqlMaxLength(reader["DataType"].ToString(), Convert.ToInt32(reader["Max_Length"]));
                            else if (col.Datatype == ETypeCode.Double || col.Datatype == ETypeCode.Decimal)
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

                            //if this is sql server 2016 or newer, check is the column is a temporal row_start or row_end column
                            if (sqlversion >= 13)
                            {
                                int generatedAlwaysTypeValue = Convert.ToInt32(reader["generated_always_type"]);
                                
                                if(generatedAlwaysTypeValue == 1)
                                    col.DeltaType = TableColumn.EDeltaType.ValidFromDate;
                                if(generatedAlwaysTypeValue == 2)
                                    col.DeltaType = TableColumn.EDeltaType.ValidToDate;
                            }

                            table.Columns.Add(col);
                        }
                    }
                }
                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The source sqlserver table + " + originalTable.Name + " could not be read due to the following error: " + ex.Message, ex);
            }
        }

        public ETypeCode ConvertSqlToTypeCode(string sqlType)
        {
            switch (sqlType)
            {
                case "bigint": return ETypeCode.Int64;
                case "binary": return ETypeCode.Binary;
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
                case "geography": return ETypeCode.Unknown;
                case "varbinary": return ETypeCode.Binary;
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

                cmd.CommandText = "truncate table " + SqlTableName(table);

                try
                {
                    await cmd.ExecuteNonQueryAsync(cancelToken);
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue(false, "Truncate cancelled", null);
                }
                catch (Exception ex)
                {
                    cmd.CommandText = "delete from " + SqlTableName(table);
                    try
                    {
                        await cmd.ExecuteNonQueryAsync(cancelToken);
                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue(false, "Delete table cancelled", null);
                    }
                    catch(Exception ex2)
                    {
                        return new ReturnValue(false, "The truncate and delete table query for " + table.Name + " could not be run due to the following error: " + ex.Message, ex2);
                    }
                }
            }

            return new ReturnValue(true, "", null);
        }

        public override async Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<Tuple<long, long>>(false, connectionResult.Message, connectionResult.Exception);
            }

            var autoIncrementSql = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement) == null ? "" : "SELECT SCOPE_IDENTITY()";
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

                        insert.Append("INSERT INTO " + SqlTableName(table) + " (");
                        values.Append("VALUES (");

                        for (int i = 0; i < query.InsertColumns.Count; i++)
                        {
                            insert.Append("[" + query.InsertColumns[i].Column.Name + "],");
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
                            return new ReturnValue<Tuple<long, long>>(false, "The insert query for " + table.Name + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + insertCommand?.ToString(), ex, Tuple.Create(timer.ElapsedTicks, (long)0));
                        }
                    }
                    transaction.Commit();
                }

                timer.Stop();
                return new ReturnValue<Tuple<long, long>>(true, Tuple.Create(timer.ElapsedTicks, identityValue)); //sometimes reader returns -1, when we want this to be error condition.
            }
        }

        public static SqlDbType GetSqlDbType(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return SqlDbType.VarChar;
                case ETypeCode.SByte:
                    return SqlDbType.SmallInt;
                case ETypeCode.UInt16:
                    return SqlDbType.Int;
                case ETypeCode.UInt32:
                    return SqlDbType.BigInt;
                case ETypeCode.UInt64:
                    return SqlDbType.VarChar;
                case ETypeCode.Int16:
                    return SqlDbType.SmallInt;
                case ETypeCode.Int32:
                    return SqlDbType.Int;
                case ETypeCode.Int64:
                    return SqlDbType.BigInt;
                case ETypeCode.Decimal:
                    return SqlDbType.Decimal;
                case ETypeCode.Double:
                    return SqlDbType.Float;
                case ETypeCode.Single:
                    return SqlDbType.Real;
                case ETypeCode.String:
                    return SqlDbType.NVarChar;
                case ETypeCode.Boolean:
                    return SqlDbType.Bit;
                case ETypeCode.DateTime:
                    return SqlDbType.DateTime;
                case ETypeCode.Time:
                    return SqlDbType.Time;
                case ETypeCode.Guid:
                    return SqlDbType.UniqueIdentifier;
                case ETypeCode.Binary:
                    return SqlDbType.Binary;
                default:
                    return SqlDbType.VarChar;
            }
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<long>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, -1);
            }

            using (var connection = (SqlConnection)connectionResult.Value)
            {

                StringBuilder sql = new StringBuilder();

                int rows = 0;

                var timer = Stopwatch.StartNew();

                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        sql.Clear();

                        sql.Append("update " + SqlTableName(table) + " set ");

                        int count = 0;
                        foreach (QueryColumn column in query.UpdateColumns)
                        {
                            sql.Append(AddDelimiter(column.Column.Name) + " = @col" + count.ToString() + ","); // cstr(count)" + GetSqlFieldValueQuote(column.Column.DataType, column.Value) + ",");
                            count++;
                        }
                        sql.Remove(sql.Length - 1, 1); //remove last comma
                        sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                        //  Retrieving schema for columns from a single table
                        using (SqlCommand cmd = connection.CreateCommand())
                        {
                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            SqlParameter[] parameters = new SqlParameter[query.UpdateColumns.Count];
                            for (int i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                SqlParameter param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i.ToString();
                                param.SqlDbType = GetSqlDbType(query.UpdateColumns[i].Column.Datatype);
                                param.Size = -1;
                                param.Value = query.UpdateColumns[i].Value == null ? DBNull.Value : query.UpdateColumns[i].Value;
                                cmd.Parameters.Add(param);
                                parameters[i] = param;
                            }

                            try
                            {
                                rows += await cmd.ExecuteNonQueryAsync(cancelToken);

                                if (cancelToken.IsCancellationRequested)
                                {
                                    return new ReturnValue<long>(false, "Update rows cancelled.", null, timer.ElapsedTicks);
                                }
                            }
                            catch (Exception ex)
                            {
                                return new ReturnValue<long>(false, "The update query for " + table.Name + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, timer.ElapsedTicks);
                            }
                        }
                    }
                    transaction.Commit();
                }

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks); //sometimes reader returns -1, when we want this to be error condition.
            }
        }


    }
}
