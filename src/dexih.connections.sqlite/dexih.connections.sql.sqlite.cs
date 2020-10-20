using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.sql;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;
using Microsoft.Data.Sqlite;

namespace dexih.connections.sqlite
{
    [Connection(
        ConnectionCategory = EConnectionCategory.DatabaseFile,
        Name = "SQLite", 
        Description = "SQLite is an embedded relational database management system. In contrast to many other database management systems, SQLite is not a client–server database engine, and requires no other software to use.",
        DatabaseDescription = "Sqlite File Name",
        ServerDescription = "Directory",
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
    public class ConnectionSqlite : ConnectionSql
    {
     
        protected override string SqlDelimiterOpen { get; } = "[";

        protected override string SqlDelimiterClose { get; } = "]";

        public override bool AllowsTruncate { get; } = false;
        
        public override object GetConnectionMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
               case ETypeCode.Decimal:
                   return (decimal) 999999999999999;
               case ETypeCode.UInt64:
                   return (ulong) long.MaxValue;
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }

        public override object GetConnectionMinValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.Decimal:
                    return (decimal)-999999999999999;
                default:
                    return DataType.GetDataTypeMinValue(typeCode, length);
            }
        }


        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
                await using (var connection = await NewConnection(cancellationToken))
                {
                    await using (var cmd = CreateCommand(connection,
                        "SELECT name FROM sqlite_master WHERE type = 'table' and name = @NAME;"))
                    {
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
                createSql.Append("create table " + AddDelimiter(table.Name) + " ");

                //sqlite does not support table/column comments, so add a comment string into the ddl.
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
                        createSql.Append(AddDelimiter(col.Name) + " INTEGER PRIMARY KEY ");
                    }
                    else
                    {
                        createSql.Append(AddDelimiter(col.Name) + " " +
                                         GetSqlType(col) + " ");
                        if (col.AllowDbNull == false)
                            createSql.Append("NOT NULL ");
                        else
                            createSql.Append("NULL ");

                        if (col.DeltaType == EDeltaType.AutoIncrement)
                        {
                            // createSql.Append("PRIMARY KEY ASC ");
                            createSql.Append("UNIQUE");
                        }
                    }

                    if (i < table.Columns.Count - 1)
                        createSql.Append(",");

                    if (!string.IsNullOrEmpty(col.Description))
                        createSql.Append(" -- " + col.Description);

                    createSql.AppendLine();
                }

                createSql.AppendLine(");");

                var naturalKey = table.GetColumns(EDeltaType.NaturalKey);
                if (naturalKey.Length > 0)
                {
                    createSql.AppendLine(
                        $"create index {AddDelimiter($"index_{table.Name}_nk")} on {AddDelimiter(table.Name)} ({string.Join(", ", naturalKey.Select(c => AddDelimiter(c.Name)))});");
                }

                foreach (var index in table.Indexes)
                {
                    createSql.AppendLine(
                        $"create index {AddDelimiter(index.Name)} on {AddDelimiter(table.Name)} ({string.Join(", ", index.Columns.Select(c => AddDelimiter(c.ColumnName)))});");
                }

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
        
        public override async Task<bool> DropTable(Table table, CancellationToken cancellationToken)
        {
            try
            {
                await using (var connection = await NewConnection(cancellationToken))
                await using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"drop table {SqlTableName(table)}";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
                
                // if there are no remaining tables, then clean the Sqlite file up.
                var tables = await GetTableList(cancellationToken);
                if (tables.Count == 0)
                {
                    var files = new[]
                    {
                        Server + "/" + DefaultDatabase + ".sqlite",
                        Server + "/" + DefaultDatabase + ".sqlite-wal",
                        Server + "/" + DefaultDatabase + ".sqlite-shm",
                    };

                    foreach (var file in files)
                    {
                        var fileExists = File.Exists(file);

                        if (fileExists)
                        {
                            // try to delete the files as they're not needed, if it fails, just catch and continue.
                            try
                            {
                                File.Delete(file);
                            }
                            catch (IOException)
                            {
                            }
                        }
                    }
                }

                return true;                
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Drop table {table.Name} failed.  {ex.Message}", ex);
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
                        sqlType = (column.IsUnicode == true ? "n" : "") + "text";
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(" + column.MaxLength + ")";
                    break;
                case ETypeCode.Geometry:
				case ETypeCode.Text:
                case ETypeCode.Xml:
                case ETypeCode.Json:
                case ETypeCode.Node:
                    sqlType = (column.IsUnicode == true ? "n" : "") + "text";
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
                case ETypeCode.Date:
                    sqlType = "date";
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
                    sqlType = $"numeric ({column.Precision??29}, {column.Scale??8})";
                    break;
                case ETypeCode.Binary:
                    sqlType = "blob";
                    break;
                case ETypeCode.Enum:
                    sqlType = "text";
                    break;
                case ETypeCode.CharArray:
                    sqlType = $"nchar({column.MaxLength})";
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
                    if (Server.Substring(Server.Length - 1) != "/" || Server.Substring(Server.Length - 1) != "/")
                        Server += "/";
                    connectionString = "Data Source=" + Server + DefaultDatabase + ".sqlite";
                }

                var connection = new SqliteConnection(connectionString);
                await connection.OpenAsync(cancellationToken);
                State = (EConnectionState) connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    throw new ConnectionException($"The Sqlite connection has a state of {connection.State}.");
                }

                await using (var command = new SqliteCommand())
                {
                    command.Connection = connection;
                    command.CommandText = "PRAGMA journal_mode=WAL";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }

                return connection;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Sqlite connection failed at directory {Server} for file {DefaultDatabase}. {ex.Message}", ex);
            }
        }

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            try
            {
                var fileName = Server + "/" + databaseName + ".sqlite";

                var fileExists = File.Exists(fileName);

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

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            try
            {
                var directoryExists = Directory.Exists(Server);
                if (!directoryExists)
                {
                    throw new ConnectionException($"The directory {Server} does not exist.");
                }

                var files = Directory.GetFiles(Server, "*.sqlite");

                var list = new List<string>();

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

        public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            try
            {
                await using (var connection = await NewConnection(cancellationToken))
                await using (var cmd = CreateCommand(connection, "SELECT name FROM sqlite_master WHERE type='table';"))
                {
                    var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                    await using (reader)
                    {

                        var tableList = new List<Table>();

                        while (await reader.ReadAsync(cancellationToken))
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

    }

}
