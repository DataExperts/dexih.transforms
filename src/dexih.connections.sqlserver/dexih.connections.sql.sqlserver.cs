using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
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
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.sqlserver
{
    [Connection(
        ConnectionCategory = EConnectionCategory.SqlDatabase,
        Name = "Microsoft SqlServer", 
        Description = "Microsoft SQL Server is a relational database management system developed by Microsoft",
        DatabaseDescription = "Database Name",
        ServerDescription = "Server Name",
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
    public class ConnectionSqlServer : ConnectionSql
    {
        public override bool CanUseGuid { get; } = true;
        public override bool AllowsTruncate { get; } = true;

        public override byte[] ConvertFromGeometry(Geometry value)
        {
            var geometryWriter = new SqlServerBytesWriter();
            return geometryWriter.Write(value);
        }

        protected override string SqlFromAttribute(Table table)
        {
            var sql = "";

            if (table.IsVersioned)
                sql = "FOR system_time all";

            sql = sql + " WITH(NOLOCK) ";

            return sql;
        }
        
        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(9999,12,31);
                default:
                    return GetDataTypeMaxValue(typeCode, length);
            }
        }
	    
        public override object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(1753,1,1);
                default:
                    return GetDataTypeMinValue(typeCode, length);
            }
		    
        }

        public override object ConvertForRead(DbDataReader reader, int ordinal, TableColumn column)
        {
            if (column.DataType == ETypeCode.Geometry && reader is SqlDataReader sqlReader)
            {
                var geometryReader = new SqlServerBytesReader();
                if (sqlReader.IsDBNull(ordinal)) return null;
                return geometryReader.Read(sqlReader.GetSqlBytes(ordinal).Value);
            }

            return base.ConvertForRead(reader, ordinal, column);
        }

        public override DbParameter CreateParameter(DbCommand command, string name, ETypeCode typeCode, int rank, ParameterDirection direction, object value)
        {
            var param = command.CreateParameter();
            param.ParameterName = name;
            param.Direction = direction;
            var writeValue = ConvertForWrite(param.ParameterName, typeCode, rank, true, value);
            param.Value = writeValue.value;

            switch (writeValue.typeCode)
            {
                case ETypeCode.UInt16:
                    param.DbType = DbType.Int32;
                    break;
                case ETypeCode.UInt32:
                    param.DbType = DbType.Int64;
                    break;
                case ETypeCode.UInt64:
                    param.DbType = DbType.Decimal;
                    break;
                default:
                    param.DbType = writeValue.typeCode.GetDbType();
                    break;
            }
            
            return param;
        }

        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
        {
            try
            {
                using (var connection = await NewConnection())
                {

                    var bulkCopy = new SqlBulkCopy((SqlConnection) connection)
                    {
                        DestinationTableName = SqlTableName(table),
                        BulkCopyTimeout = 60,
                        
                    };

                    //Add column mapping to ensure unsupported columns (i.e. location datatype) are ignored.
                    foreach(var column in table.Columns.Where(c => c.DeltaType != EDeltaType.DbAutoIncrement))
                    {
                        bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                    }
                    
                    await bulkCopy.WriteToServerAsync(reader, cancellationToken);

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Bulk insert into table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "select name from sys.tables where object_id = OBJECT_ID(@NAME)"))
                {
                    cmd.Parameters.Add(CreateParameter(cmd, "@NAME", ETypeCode.String,0, ParameterDirection.Input, SqlTableName(table)));
                    var tableExistsResult = await cmd.ExecuteScalarAsync(cancellationToken);
                    if(tableExistsResult == null)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
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
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
            try
            {
                var tableExists = await TableExists(table, cancellationToken);

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

                var createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + SqlTableName(table) + " ( ");
                foreach (var col in table.Columns)
                {
                    createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col));
                    if (col.DeltaType == EDeltaType.DbAutoIncrement)
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
                var key = table.GetAutoIncrementColumn();

                if (key != null)
                    createSql.Append("ALTER TABLE " + SqlTableName(table) + " ADD CONSTRAINT [PK_" + AddEscape(table.Name) + "] PRIMARY KEY CLUSTERED ([" + AddEscape(key.Name) + "])");

                using (var connection = await NewConnection())
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = createSql.ToString();
                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"The create table query failed.  {ex.Message}");
                        }
                    }

                    //run a query to get the schema name and also check the table has been created.
                    object schemaName = null;
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "SELECT s.name SchemaName FROM sys.tables AS t INNER JOIN sys.schemas AS s ON t.[schema_id] = s.[schema_id] where object_id = OBJECT_ID(@NAME)";
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", ETypeCode.Text, 0, ParameterDirection.Input, SqlTableName(table)));

                        try
                        {
                            schemaName = await cmd.ExecuteScalarAsync(cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"The get database tables query failed.  {ex.Message}");
                        }

                        if (schemaName == null)
                        {
                            throw new ConnectionException($"The table was not correctly created.");
                        }
                    }

                    try
                    {
                        //Add the table description
                        if (!string.IsNullOrEmpty(table.Description))
                        {
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=@description , @level0type=N'SCHEMA',@level0name=@schemaname, @level1type=N'TABLE',@level1name=@tablename";
                                cmd.Parameters.Add(CreateParameter(cmd, "@description", ETypeCode.Text, 0, ParameterDirection.Input, table.Description));
                                cmd.Parameters.Add(CreateParameter(cmd, "@schemaname", ETypeCode.Text, 0, ParameterDirection.Input, schemaName));
                                cmd.Parameters.Add(CreateParameter(cmd, "@tablename", ETypeCode.Text, 0, ParameterDirection.Input, table.Name));
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                        }

                        //Add the column descriptions
                        foreach (var col in table.Columns)
                        {
                            if (!string.IsNullOrEmpty(col.Description))
                            {
                                using (var cmd = connection.CreateCommand())
                                {
                                    cmd.CommandText = "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=@description , @level0type=N'SCHEMA',@level0name=@schemaname, @level1type=N'TABLE',@level1name=@tablename, @level2type=N'COLUMN',@level2name=@columnname";
                                    cmd.Parameters.Add(CreateParameter(cmd, "@description", ETypeCode.Text, 0, ParameterDirection.Input, col.Description));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@schemaname", ETypeCode.Text, 0, ParameterDirection.Input, schemaName));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@tablename", ETypeCode.Text, 0, ParameterDirection.Input, table.Name));
                                    cmd.Parameters.Add(CreateParameter(cmd, "@columnname", ETypeCode.Text, 0, ParameterDirection.Input, col.Name));
                                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"Failed to add table/column descriptions. {ex.Message}", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create table {table.Name} failed. {ex.Message}", ex);
            }
        }

        protected override string GetSqlType(TableColumn column)
        {
            string sqlType;

            if (column.Rank > 0)
            {
                return "varchar(max)";
            }

            switch (column.DataType)
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
                    if (column.MaxLength == null)
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(max)";
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(" + column.MaxLength + ")";
                    break;
                case ETypeCode.CharArray:
                    if (column.MaxLength == null)
                        throw new Exception($"The column {column.Name} has a char datatype however no length is specified.");
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "char(" + column.MaxLength + ")";
                    break;
                case ETypeCode.Text:
                case ETypeCode.Json:
                case ETypeCode.Xml:
                case ETypeCode.Node:
					sqlType = "text";
					break;
                case ETypeCode.Single:
                    sqlType = "real";
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
                    if (column.MaxLength == null)
                        sqlType = "varbinary(max)";
                    else
                        sqlType = "varbinary(" + column.MaxLength + ")";
                    break;
                //case TypeCode.TimeSpan:
                //    SQLType = "time(7)";
                //    break;
                case ETypeCode.Unknown:
                    sqlType = "nvarchar(max)";
                    break;
                case ETypeCode.Decimal:
                    sqlType = $"DECIMAL ({column.Precision??29}, {column.Scale??0})";
                    break;
                case ETypeCode.Geometry:
                    sqlType = "Geometry";
                    break;
                default:
                    throw new Exception($"The column {column.Name} has datatype datatype {column.DataType} which is not compatible with the create table.");
            }

            return sqlType;
        }


        public override async Task<DbConnection> NewConnection()
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
                    throw new ConnectionException($"The SqlServer connection has a state of {connection.State}.");
                }
                return connection;
            }
            catch (Exception ex)
            {
                if(connection != null)
                    connection.Dispose();
                throw new ConnectionException($"SqlServer connection to server {Server} and database {DefaultDatabase} failed. {ex.Message}", ex);
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
                    var value = await cmd.ExecuteNonQueryAsync(cancellationToken);
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
                using (var cmd = CreateCommand(connection, "SELECT name FROM sys.databases where name NOT IN ('master', 'tempdb', 'model', 'msdb') order by name"))
                using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        list.Add((string)reader["name"]);
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
                    var sqlversion = 0;
                    //get the sql server version.
                    using (var cmd = CreateCommand(connection, "SELECT SERVERPROPERTY('ProductVersion') AS ProductVersion"))
                    {
                        var fullversion = (await cmd.ExecuteScalarAsync(cancellationToken)).ToString();

                        sqlversion = Convert.ToInt32(fullversion.Split('.')[0]);
                    }

                    using (var cmd = CreateCommand(connection, "SELECT * FROM INFORMATION_SCHEMA.Tables where TABLE_TYPE='BASE TABLE' order by TABLE_NAME"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
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
                            using (var cmd = CreateCommand(connection, "select temporal_type from sys.tables where object_id = OBJECT_ID('" + SqlTableName(table) + "')"))
                            {
                                var temporalType = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken));
                                //Exclude history table from the list (temporalType = 1)
                                if (temporalType != 1)
                                    newTableList.Add(table);
                            }
                        }

                        tableList = newTableList;
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
            if (originalTable.UseQuery)
            {
                return await GetQueryTable(originalTable, cancellationToken);
            }
            
            try
            {
                var table = new Table(originalTable.Name, originalTable.Schema);
                var tableName = SqlTableName(table);

                using (var connection = await NewConnection())
                {
                    var sqlversion = 0;

                    //get the sql server version.
                    using (var cmd = CreateCommand(connection, "SELECT SERVERPROPERTY('ProductVersion') AS ProductVersion"))
                    {
                        var fullVersion = (await cmd.ExecuteScalarAsync(cancellationToken)).ToString();

                        sqlversion = Convert.ToInt32(fullVersion.Split('.')[0]);
                    }

                    //get the column descriptions.
                    using (var cmd = CreateCommand(connection, $@"select value 'Description' 
                            FROM sys.extended_properties
                            WHERE minor_id = 0 and class = 1 and (name = 'MS_Description' or name = 'Description') and
                            major_id = OBJECT_ID('" + tableName + "')"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        if (await reader.ReadAsync(cancellationToken))
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
                        using (var cmd = CreateCommand(connection, "select temporal_type from sys.tables where object_id = OBJECT_ID('" + tableName + "')"))
                        {
                            var temporalType = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken));
                        //If the table is a temporal table, mark it.
                        if (temporalType == 2)
                            table.IsVersioned = true;
                        }
                    }

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    var generatedAlwaysTypeColumn = "";

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
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            var col = new TableColumn
                            {
                                //add the basic properties
                                Name = reader["ColumnName"].ToString(),
                                LogicalName = reader["ColumnName"].ToString(),
                                IsInput = false,
                                DataType = ConvertSqlToTypeCode(reader["DataType"].ToString())
                            };

                            if (col.DataType == ETypeCode.Unknown)
                            {
                                col.DeltaType = EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                if (Convert.ToBoolean(reader["PrimaryKey"]))
                                    col.DeltaType = EDeltaType.NaturalKey;
                                else
                                    col.DeltaType = EDeltaType.TrackingField;
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

                            //if this is sql server 2016 or newer, check is the column is a temporal row_start or row_end column
                            if (sqlversion >= 13)
                            {
                                var generatedAlwaysTypeValue = Convert.ToInt32(reader["generated_always_type"]);
                                
                                if(generatedAlwaysTypeValue == 1)
                                    col.DeltaType = EDeltaType.ValidFromDate;
                                if(generatedAlwaysTypeValue == 2)
                                    col.DeltaType = EDeltaType.ValidToDate;
                            }

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
                case "varbinary": return ETypeCode.Binary;
                case "varchar": return ETypeCode.String;
                case "xml": return ETypeCode.String;
                case "geometry": return ETypeCode.Geometry;
                case "geography": return ETypeCode.Geometry;
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


        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var autoIncrementSql = table.GetColumn(EDeltaType.DbAutoIncrement) == null ? "" : "SELECT SCOPE_IDENTITY()";
                long identityValue = 0;
                long autoIncrementValue = 0;

                var transactionConnection = await GetTransaction(transactionReference);
                var connection = (SqlConnection) transactionConnection.connection;
                var transaction = (SqlTransaction) transactionConnection.transaction;

                try
                {

                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    foreach (var query in queries)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        insert.Clear();
                        values.Clear();

                        insert.Append($"INSERT INTO {SqlTableName(table)} (");
                        values.Append("VALUES (");

                        for (var i = 0; i < query.InsertColumns.Count; i++)
                        {
                            insert.Append("[" + query.InsertColumns[i].Column.Name + "],");
                            values.Append("@col" + i + ",");

                            if (query.InsertColumns[i].Column.DeltaType == EDeltaType.AutoIncrement)
                            {
                                autoIncrementValue = Convert.ToInt64(query.InsertColumns[i].Value);
                            }
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
                                    var converted = ConvertForWrite(query.InsertColumns[i].Column,
                                        query.InsertColumns[i].Value);
                                    param.SqlDbType = GetSqlDbType(converted.typeCode);
                                    param.Value = converted.value ;
                                    cmd.Parameters.Add(param);
                                }

                                var identity = await cmd.ExecuteScalarAsync(cancellationToken);
                                identityValue = Convert.ToInt64(identity);

                            }
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"The insert query failed.  {ex.Message}");
                        }
                    }

                    if (autoIncrementValue > 0)
                    {
                        return autoIncrementValue;
                    }
                    
                    var deltaColumn = table.GetColumn(EDeltaType.AutoIncrement);
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

                    return identityValue;
                }
                finally
                {
                    EndTransaction(transactionReference, transactionConnection);
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Insert rows into table {table.Name} failed. {ex.Message}", ex);
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
                case ETypeCode.Geometry:
                    return SqlDbType.Binary;
                default:
                    return SqlDbType.VarChar;
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var transactionConnection = await GetTransaction(transactionReference);
                var connection = (SqlConnection) transactionConnection.connection;
                var transaction = (SqlTransaction) transactionConnection.transaction;

                try
                {
                    var sql = new StringBuilder();

                    foreach (var query in queries)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        sql.Clear();

                        sql.Append("update " + SqlTableName(table) + " set ");

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
                            sql.Append(BuildFiltersString(query.Filters, cmd) + ";");

                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            var parameters = new SqlParameter[query.UpdateColumns.Count];
                            for (var i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                var converted = ConvertForWrite(query.UpdateColumns[i].Column,
                                    query.UpdateColumns[i].Value);
                                var param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i;
                                param.SqlDbType = GetSqlDbType(converted.typeCode);
                                param.Size = -1;
                                param.Value = converted.value;
                                cmd.Parameters.Add(param);
                                parameters[i] = param;
                            }

                            try
                            {
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The update query failed.  {ex.Message}");
                            }
                        }
                    }
                }
                finally
                {
                    EndTransaction(transactionReference, transactionConnection);
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Update rows in table {table.Name} failed. {ex.Message}", ex);
            }
        }


    }
}
