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
        DatabaseDescription = "Directory",
        ServerDescription = "Sqlite File Name",
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
        
        public override object GetConnectionMaxValue(DataType.ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
               case DataType.ETypeCode.Decimal:
                   return (decimal) 999999999999999;
               case DataType.ETypeCode.UInt64:
                   return (ulong) long.MaxValue;
                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }

        public override object GetConnectionMinValue(DataType.ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case DataType.ETypeCode.Decimal:
                    return (decimal)-999999999999999;
                default:
                    return DataType.GetDataTypeMinValue(typeCode, length);
            }
        }


        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
                using (var connection = await NewConnection())
                {

                    using (var cmd = CreateCommand(connection,
                        "SELECT name FROM sqlite_master WHERE type = 'table' and name = @NAME;"))
                    {
                        cmd.Parameters.Add(CreateParameter(cmd, "@NAME", DataType.ETypeCode.Text, 0, ParameterDirection.Input, table.Name));
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
                    var dropResult = await DropTable(table);
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
                    if (col.DeltaType == TableColumn.EDeltaType.DbAutoIncrement)
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

                        if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
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

                var naturalKey = table.GetColumns(TableColumn.EDeltaType.NaturalKey);
                if (naturalKey.Length > 0)
                {
                    createSql.AppendLine(
                        $"create index {AddDelimiter($"index_{table.Name}_nk")} on {AddDelimiter(table.Name)} ({string.Join(", ", naturalKey.Select(c => AddDelimiter(c.Name)))});");
                }

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
                        throw new ConnectionException($"The create table query failed.  {ex.Message}");
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

            switch (column.DataType)
            {
                case DataType.ETypeCode.Int32:
                case DataType.ETypeCode.UInt16:
                    sqlType = "int";
                    break;
                case DataType.ETypeCode.Byte:
                    sqlType = "tinyint";
                    break;
                case DataType.ETypeCode.Int16:
                case DataType.ETypeCode.SByte:
                    sqlType = "smallint";
                    break;
                case DataType.ETypeCode.Int64:
                case DataType.ETypeCode.UInt32:
                    sqlType = "bigint";
                    break;
                case DataType.ETypeCode.String:
                    if (column.MaxLength == null)
                        sqlType = (column.IsUnicode == true ? "n" : "") + "text";
                    else
                        sqlType = (column.IsUnicode == true ? "n" : "") + "varchar(" + column.MaxLength.ToString() + ")";
                    break;
                case DataType.ETypeCode.Geometry:
				case DataType.ETypeCode.Text:
                case DataType.ETypeCode.Xml:
                case DataType.ETypeCode.Json:
                case DataType.ETypeCode.Node:
                    sqlType = (column.IsUnicode == true ? "n" : "") + "text";
					break;
                case DataType.ETypeCode.Single:
                    sqlType = "float";
                    break;
                case DataType.ETypeCode.UInt64:
                    sqlType = "nvarchar(25)";
                    break;
                case DataType.ETypeCode.Double:
                    sqlType = "float";
                    break;
                case DataType.ETypeCode.Boolean:
                    sqlType = "boolean";
                    break;
                case DataType.ETypeCode.DateTime:
                    sqlType = "datetime";
                    break;
                case DataType.ETypeCode.Time:
                    sqlType = "text"; //sqlite doesn't have a time type.
                    break;
                case DataType.ETypeCode.Guid:
                    sqlType = "text";
                    break;
                case DataType.ETypeCode.Unknown:
                    sqlType = "text";
                    break;
                case DataType.ETypeCode.Decimal:
                    sqlType = $"numeric ({column.Precision??28}, {column.Scale??0})";
                    break;
                case DataType.ETypeCode.Binary:
                    sqlType = "blob";
                    break;
                case DataType.ETypeCode.Enum:
                    sqlType = "text";
                    break;
                case DataType.ETypeCode.CharArray:
                    sqlType = $"nchar({column.MaxLength})";
                    break;
                default:
                    throw new Exception($"The datatype {column.DataType} is not compatible with the create table.");
            }

            return sqlType;
        }


        /// <summary>
        /// Gets the start quote to go around the values in sql insert statement based in the column type.
        /// </summary>
        /// <returns></returns>
//        protected override string GetSqlFieldValueQuote(ETypeCode typeCode, int rank, object value)
//        {
//            string returnValue;
//
//            if (value == null || value.GetType().ToString() == "System.DBNull")
//                return "null";
//
//            //if (value is string && type != ETypeCode.String && string.IsNullOrWhiteSpace((string)value))
//            //    return "null";
//
//            if (rank > 0)
//            {
//                return "'" + AddEscape(value.ToString()) + "'";
//            }
//            
//            switch (typeCode)
//            {
//                case ETypeCode.Byte:
//                case ETypeCode.Single:
//                case ETypeCode.Int16:
//                case ETypeCode.Int32:
//                case ETypeCode.Int64:
//                case ETypeCode.SByte:
//                case ETypeCode.UInt16:
//                case ETypeCode.UInt32:
//                case ETypeCode.UInt64:
//                case ETypeCode.Double:
//                case ETypeCode.Decimal:
//                    returnValue = AddEscape(value.ToString());
//                    break;
//                case ETypeCode.Boolean:
//                    returnValue = (bool) value ? "1" : "0";
//                    break;
//				case ETypeCode.String:
//                case ETypeCode.Text:
//                case ETypeCode.Json:
//                case ETypeCode.Xml:
//                case ETypeCode.Guid:
//                case ETypeCode.Unknown:
//                    returnValue = "'" + AddEscape(value.ToString()) + "'";
//                    break;
//                case ETypeCode.DateTime:
//                case ETypeCode.Time:
//                    //sqlite does not have date fields, so convert to format that will work for greater/less compares
//                    if (value is DateTime)
//                        returnValue = "'" + AddEscape(((DateTime) value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "'";
//                    else if (value is TimeSpan)
//                        returnValue = "'" + AddEscape(((TimeSpan) value).ToString()) + "'";
//                    else
//                        returnValue = "'" + AddEscape((string) value) + "'";
//                    break;
//                default:
//                    throw new Exception($"The datatype {typeCode} is not compatible with the sql insert statement.");
//            }
//
//            return returnValue;
//        }


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

                var connection = new SqliteConnection(connectionString);
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
                    await command.ExecuteNonQueryAsync();
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
                using (var connection = await NewConnection())
                using (var cmd = CreateCommand(connection, "SELECT name FROM sqlite_master WHERE type='table';"))
                {
                    var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                    using (reader)
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
            if (originalTable.UseQuery)
            {
                return await GetQueryTable(originalTable, cancellationToken);
            }

            try
            {

                using (var connection = await NewConnection())
                {

                    var table = new Table(originalTable.Name)
                    {
                        //sqllite doesn't have table descriptions.
                        Description = ""
                    };

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"PRAGMA table_info('" + table.Name + "')"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
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
                            if (col.DataType == DataType.ETypeCode.Unknown)
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

                            if (col.DataType == DataType.ETypeCode.String)
                            {
                                if (dataType.Length > 1)
                                    col.MaxLength = Convert.ToInt32(dataType[1]);
                            }
                            else if (col.DataType == DataType.ETypeCode.Double || col.DataType == DataType.ETypeCode.Decimal)
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

        public DataType.ETypeCode ConvertSqlToTypeCode(string sqlType)
        {
            switch (sqlType)
            {
                case "INT":
                    return DataType.ETypeCode.Int32;
                case "INTEGER":
                    return DataType.ETypeCode.Int32;
                case "TINYINT":
                    return DataType.ETypeCode.Int16;
                case "SMALLINT":
                    return DataType.ETypeCode.Int16;
                case "MEDIUMINT":
                    return DataType.ETypeCode.Int32;
                case "BIGINT":
                    return DataType.ETypeCode.Int64;
                case "UNSIGNED BIG INT":
                    return DataType.ETypeCode.UInt64;
                case "INT2":
                    return DataType.ETypeCode.UInt16;
                case "INT8":
                    return DataType.ETypeCode.UInt32;
                case "CHARACTER":
                    return DataType.ETypeCode.String;
                case "CHAR":
                    return DataType.ETypeCode.String;
                case "CURRENCY":
                    return DataType.ETypeCode.Decimal;
                case "VARCHAR":
                    return DataType.ETypeCode.String;
                case "VARYING CHARACTER":
                    return DataType.ETypeCode.String;
                case "NCHAR":
                    return DataType.ETypeCode.String;
                case "NATIVE CHARACTER":
                    return DataType.ETypeCode.String;
                case "NVARCHAR":
                    return DataType.ETypeCode.String;
                case "TEXT":
                    return DataType.ETypeCode.Text;
                case "CLOB":
                    return DataType.ETypeCode.Text;
                case "BLOB":
                    return DataType.ETypeCode.Unknown;
                case "REAL":
                    return DataType.ETypeCode.Double;
                case "DOUBLE":
                    return DataType.ETypeCode.Double;
                case "DOUBLE PRECISION":
                    return DataType.ETypeCode.Double;
                case "FLOAT":
                    return DataType.ETypeCode.Double;
                case "NUMERIC":
                    return DataType.ETypeCode.Decimal;
                case "DECIMAL":
                    return DataType.ETypeCode.Decimal;
                case "BOOLEAN":
                    return DataType.ETypeCode.Boolean;
                case "DATE":
                    return DataType.ETypeCode.DateTime;
                case "DATETIME":
                    return DataType.ETypeCode.DateTime;
            }
            return DataType.ETypeCode.Unknown;
        }

//        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries,
//            CancellationToken cancellationToken = default)
//        {
//            try
//            {
//
//                var autoIncrementSql = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement) == null
//                    ? ""
//                    : " select last_insert_rowid() from [" + table.Name + "]";
//                long identityValue = 0;
//
//                using (var connection = await NewConnection())
//                {
//                    var insert = new StringBuilder();
//                    var values = new StringBuilder();
//
//                    using (var transaction = connection.BeginTransaction())
//                    {
//                        foreach (var query in queries)
//                        {
//                            cancellationToken.ThrowIfCancellationRequested();
//
//                            insert.Clear();
//                            values.Clear();
//
//                            insert.Append("INSERT INTO " + AddDelimiter(table.Name) + " (");
//                            values.Append("VALUES (");
//
//                            for (var i = 0; i < query.InsertColumns.Count; i++)
//                            {
//                                insert.Append("[" + query.InsertColumns[i].Column.Name + "],");
//                                values.Append("@col" + i.ToString() + ",");
//                            }
//
//                            var insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " +
//                                                   values.Remove(values.Length - 1, 1).ToString() + "); " +
//                                                   autoIncrementSql;
//
//                            try
//                            {
//                                using (var cmd = connection.CreateCommand())
//                                {
//                                    cmd.CommandText = insertCommand;
//                                    cmd.Transaction = transaction;
//
//                                    for (var i = 0; i < query.InsertColumns.Count; i++)
//                                    {
//                                        var param = new SqliteParameter
//                                        {
//                                            ParameterName = "@col" + i.ToString(),
//                                            Value = ConvertParameterType(query.InsertColumns[i].Column.DataType, query.InsertColumns[i].Column.Rank, query.InsertColumns[i].Value),
//                                            DbType = GetDbType(query.InsertColumns[i].Column.DataType)
//                                        };
//
//                                        cmd.Parameters.Add(param);
//                                    }
//
//                                    var identity = await cmd.ExecuteScalarAsync(cancellationToken);
//                                    identityValue = Convert.ToInt64(identity);
//
//                                }
//                            }
//                            catch (Exception ex)
//                            {
//                                throw new ConnectionException($"The insert query failed.  {ex.Message}");
//                            }
//                        }
//                        transaction.Commit();
//                    }
//
//                    return identityValue; //sometimes reader returns -1, when we want this to be error condition.
//                }
//            }
//            catch(Exception ex)
//            {
//                throw new ConnectionException($"Insert into table {table.Name} failed. {ex.Message}", ex);
//            }
//        }
    }

}
