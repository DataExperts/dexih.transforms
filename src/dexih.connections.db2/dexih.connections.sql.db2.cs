using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using dexih.functions;
using System.Data.Common;
using System.Linq;
using System.Threading;
using dexih.connections.sql;
using dexih.transforms;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;
using IBM.Data.DB2.Core;


namespace dexih.connections.db2
{
    [Connection(
        ConnectionCategory = Connection.EConnectionCategory.DatabaseFile,
        Name = "DB2", 
        Description = "IBM DB2 Data Server",
        DatabaseDescription = "DB2 Schema",
        ServerDescription = "DB2 Server",
        ServerHelp = "Use the format server:port/database",
        AllowsConnectionString = true,
        AllowsSql = true,
        AllowsFlatFiles = false,
        AllowsManagedConnection = true,
        AllowsSourceConnection = true,
        AllowsTargetConnection = true,
        AllowsUserPassword = true,
        AllowsWindowsAuth = false,
        RequiresDatabase = true,
        RequiresLocalStorage = false
    )]
    public class ConnectionDB2 : ConnectionSql
    {
        public override bool CanUseUnsigned => false;
        public override bool CanUseBoolean => false;
        public override bool CanUseDbAutoIncrement => true;
        public override bool CanUseArray => false;

        public override bool CanUseSByte => false;
        public override bool CanUseByte => false;

        // this is creator for linux/ owner for zos (stupid IBM!!!!)
        protected virtual string OwnerColumn => "creator"; //owner
        
        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.UInt64:
                    return (ulong)long.MaxValue;
                case ETypeCode.DateTime:
                    return new DateTime(9999,12,31,23,59,59); 
                case ETypeCode.Date:
                    return new DateTime(9999,12,31); 
                case ETypeCode.Time:
                    return new TimeSpan(23, 59, 59); 
                case ETypeCode.Decimal:
                    return 1E+20m;
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }
        
        public override object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.Decimal:
                    return -1E+20m;
                default:
                    return DataType.GetDataTypeMinValue(typeCode, length);
            }
        }
        
        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
        {
            try
            {
                await using (var connection = await NewConnection(cancellationToken))
                {

                    var bulkCopy = new DB2BulkCopy((DB2Connection) connection)
                    {
                        DestinationTableName = SqlTableName(table),
                        BulkCopyTimeout = 60,
                    };

                    //Add column mapping to ensure unsupported columns (i.e. location datatype) are ignored.
                    foreach(var column in table.Columns.Where(c => c.DeltaType != EDeltaType.DbAutoIncrement))
                    {
                        bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                    }
                    
                    bulkCopy.WriteToServer(reader);

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
                await using (var connection = await NewConnection(cancellationToken))
                {
                    await using (var cmd = CreateCommand(connection,
                        $"select * from sysibm.SYSTABLES where {OwnerColumn} = @SCHEMA and name = @NAME and TYPE = 'T';"))
                    {
                        cmd.Parameters.Add(CreateParameter(cmd, "@SCHEMA", ETypeCode.Text, 0, ParameterDirection.Input, DefaultDatabase));
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", ETypeCode.Text, 0, ParameterDirection.Input, table.Name));
                        var tableExists = await cmd.ExecuteScalarAsync(cancellationToken);
                        return tableExists != null;
                    }
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Table exists for table {table.Name} failed. {ex.Message}", ex);
            }
        }
        
        public override string SqlTableName(Table table)
        {
            if (!string.IsNullOrEmpty(table.Schema))
            {
                return AddDelimiter(DefaultDatabase) + "." + AddDelimiter(table.Schema) + "." + AddDelimiter(table.Name);
            }

            return AddDelimiter(DefaultDatabase) + "." + AddDelimiter(table.Name);
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
                    await DropTable(table, cancellationToken);
                }

                var createSql = new StringBuilder();

                //Create the table
                createSql.Append($"create table {SqlTableName(table)} ");

                //db2 does not support table/column comments, so add a comment string into the ddl.
                if (!string.IsNullOrEmpty(table.Description))
                    createSql.Append(" -- " + table.Description);

                createSql.AppendLine("");
                createSql.Append("(");

                for (var i = 0; i < table.Columns.Count; i++)
                {
                    var col = table.Columns[i];

                    //ignore datatypes for autoincrement and create a primary key.
                    if (col.DeltaType == EDeltaType.DbAutoIncrement)
                    {
                        createSql.Append($"{AddDelimiter(col.Name)} {GetSqlType(col)} NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) ");
                    }
                    else
                    {
                        createSql.Append(AddDelimiter(col.Name) + " " +
                                         GetSqlType(col) + " ");
                        if (col.AllowDbNull == false)
                            createSql.Append("NOT NULL ");
                        else
                            createSql.Append("NULL ");
                    }

                    if (i < table.Columns.Count - 1)
                        createSql.Append(",");

                    if (!string.IsNullOrEmpty(col.Description))
                        createSql.Append(" -- " + col.Description);

                    createSql.AppendLine();
                }

                createSql.AppendLine(")");

                await using (var connection = await NewConnection(cancellationToken))
                await using (var command = connection.CreateCommand())
                {
                    try
                    {
                        command.CommandText = createSql.ToString();
                        await command.ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"Create table failed: {ex.Message}, sql command: {createSql}.", ex);
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

            if (column.Rank > 0)
            {
                return "clob";
            }
            
            string sqlType;

            switch (column.DataType)
            {
                case ETypeCode.Int32:
                case ETypeCode.UInt16:
                    sqlType = "integer";
                    break;
                case ETypeCode.Byte:
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
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(4000)";
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(" + column.MaxLength + ")";
                    break;
				case ETypeCode.Text:
                case ETypeCode.Xml:
                case ETypeCode.Json:
                case ETypeCode.Node:
                    sqlType = "clob";
					break;
                case ETypeCode.Single:
                    sqlType = "real";
                    break;
                case ETypeCode.UInt64:
                    sqlType = "varchar(25)";
                    break;
                case ETypeCode.Double:
                    sqlType = "float";
                    break;
                case ETypeCode.Boolean:
                    sqlType = "smallint";
                    break;
                case ETypeCode.DateTime:
                    sqlType = "timestamp";
                    break;
                case ETypeCode.Date:
                    sqlType = "date";
                    break;
                case ETypeCode.Time:
                    sqlType = "time";
                    break;
                case ETypeCode.Guid:
                    sqlType = "varchar(40)";
                    break;
                case ETypeCode.Unknown:
                    sqlType = "clob";
                    break;
                case ETypeCode.Decimal:
                    sqlType = $"decimal ({column.Precision??29}, {column.Scale??8})";
                    break;
                case ETypeCode.Binary:
                    sqlType = "blob";
                    break;
                case ETypeCode.Enum:
                    sqlType = "integer";
                    break;
                case ETypeCode.CharArray:
                    sqlType = $"char({column.MaxLength})";
                    break;
                case ETypeCode.Geometry:
                    sqlType = "blob";
                    break;
                default:
                    throw new Exception($"The datatype {column.DataType} is not compatible with the create table.");
            }

            return sqlType;
        }


    
        public override async Task<DbConnection> NewConnection(CancellationToken cancellationToken)
        {
            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
                    var server = Server.Split('/');
                    if (server.Length != 2)
                    {
                        throw new ConnectionException("The server must be in the format server:port/database.");
                    }
                    connectionString = $"DATABASE={server[1]};SERVER={server[0]};UID={Username};PWD={Password};";
                    
                }

                var connection = new DB2Connection(connectionString);
                await connection.OpenAsync(cancellationToken);
                State = (EConnectionState) connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    throw new ConnectionException($"The DB2 connection has a state of {connection.State}.");
                }

                return connection;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"DB2 connection failed at directory {Server} for file {DefaultDatabase}. {ex.Message}", ex);
            }
        }

        public override async Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            try
            {
                
                DefaultDatabase = "";
                await using (var connection = await NewConnection(cancellationToken))
                await using (var cmd = CreateCommand(connection, "CREATE SCHEMA " + AddDelimiter(databaseName)))
                {
                    
                    var value = await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

                DefaultDatabase = databaseName;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create SCHEMA {databaseName} failed. {ex.Message}", ex);
            }
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            try
            {
                var list = new List<string>();

                await using (var connection = await NewConnection(cancellationToken))
                await using (var cmd = CreateCommand(connection, "select schemaname from syscat.schemata"))
                await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        list.Add((string)reader["schemaname"]);
                    }
                }
                return list;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get schema list failed. {ex.Message}", ex);
            }
        }

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            try
            {
                await using (var connection = await NewConnection(cancellationToken))
                await using (var cmd = CreateCommand(connection, $"select * from QSYS2.SYSTABLES where TABLE_SCHEMA like '{DefaultDatabase}' and TYPE = 'T';"))
                {
                    var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                    await using (reader)
                    {
                        var tableList = new List<Table>();

                        while (await reader.ReadAsync(cancellationToken))
                        {
                            tableList.Add(new Table((string)reader["TABLE_NAME"]));
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
            CancellationToken cancellationToken = default)
        {
            if (originalTable.TableType == Table.ETableType.Query)
            {
                return await GetQueryTable(originalTable, cancellationToken);
            }

            try
            {
                await using (var connection = await NewConnection(cancellationToken))
                {

                    var table = new Table(originalTable.Name)
                    {
                        //sqllite doesn't have table descriptions.
                        Description = ""
                    };

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    await using (var cmd = CreateCommand(connection, @"PRAGMA table_info('" + table.Name + "')"))
                    await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            var col = new TableColumn
                            {
                                //add the basic properties
                                Name = reader["name"].ToString(),
                                LogicalName = reader["name"].ToString(),
                                IsInput = false
                            };

                            var dataType = reader["type"].ToString().Split('(', ')');
                            col.DataType = ConvertSqlToTypeCode(dataType[0]);
                            if (col.DataType == ETypeCode.Unknown)
                            {
                                col.DeltaType = EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                if (Convert.ToInt32(reader["pk"]) >= 1)
                                    col.DeltaType = EDeltaType.NaturalKey;
                                else
                                    col.DeltaType = EDeltaType.TrackingField;
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
                                    var precisionScale = dataType[1].Split(',');
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
                throw new ConnectionException($"Get source table information for {originalTable.Name} failed. {ex.Message}", ex);
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
                    return ETypeCode.Text;
                case "CLOB":
                    return ETypeCode.Text;
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
                    return ETypeCode.Date;
                case "DATETIME":
                    return ETypeCode.DateTime;
            }
            return ETypeCode.Unknown;
        }
        
        public override DbParameter CreateParameter(DbCommand command, string name, ETypeCode typeCode, int rank, ParameterDirection direction, object value)
        {
            var param = (DB2Parameter) command.CreateParameter();
            param.ParameterName = name;
            param.Direction = direction;
            var writeValue = ConvertForWrite(param.ParameterName, typeCode, rank, true, value);
            param.Value = writeValue.value;

            switch (writeValue.typeCode)
            {
                case ETypeCode.Decimal:
                    param.DB2Type = DB2Type.Decimal;
                    break;
                default:
                    param.DbType = writeValue.typeCode.GetDbType();
                    break;
            }
            
            return param;
        }
    }

}
