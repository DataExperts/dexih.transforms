using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
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
    public class ConnectionPostgreSql : ConnectionSql
    {

        public override string ServerHelp => "Server:Port Name";
        public override string DefaultDatabaseHelp => "Database";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override string DatabaseTypeName => "PostgreSQL";
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

		public override object ConvertParameterType(object value)
		{
            switch (value)
            {
                case UInt16 uint16:
                    return (Int32)uint16;
                case UInt32 uint32:
                    return (Int64)uint32;
				case UInt64 uint64:
					return (Int64)uint64;
				default:
                    return value;
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
            using (DbCommand cmd = CreateCommand(connection, "select table_name from information_schema.tables where table_name = @NAME"))
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
					if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
						createSql.Append(AddDelimiter(col.ColumnName) + " SERIAL"); //TODO autoincrement for postgresql
					else
					{
						createSql.Append(AddDelimiter(col.ColumnName) + " " + GetSqlType(col.Datatype, col.MaxLength, col.Scale, col.Precision));
						if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
							createSql.Append(" IDENTITY(1,1)"); //TODO autoincrement for postgresql
						if (col.AllowDbNull == false)
							createSql.Append(" NOT NULL");
						else
							createSql.Append(" NULL");
					}
                    createSql.Append(",");
                }

				//Add the primary key using surrogate key or autoincrement.
				TableColumn key = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
				if (key == null)
				{
					key = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
				}

				if (key != null)
					createSql.Append("CONSTRAINT \"PK_" + AddEscape(table.TableName) + "\" PRIMARY KEY (" + AddDelimiter(key.ColumnName) + "),");


				//remove the last comma
				createSql.Remove(createSql.Length - 1, 1);
                createSql.Append(")");


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
				case ETypeCode.Int16:
                case ETypeCode.SByte:
                    sqlType = "smallint";
                    break;
                case ETypeCode.Int64:
                case ETypeCode.UInt32:
				case ETypeCode.UInt64:
					sqlType = "bigint";
                    break;
                case ETypeCode.String:
                    if (length == null)
                        sqlType = "varchar(10485760)";
                    else
                        sqlType = "varchar(" + length.ToString() + ")";
                    break;
                case ETypeCode.Single:
                    sqlType = "real";
                    break;
                case ETypeCode.Double:
                    sqlType = "double precision";
                    break;
                case ETypeCode.Boolean:
                    sqlType = "bool";
                    break;
                case ETypeCode.DateTime:
                    sqlType = "timestamp";
                    break;
                case ETypeCode.Time:
                    sqlType = "time";
                    break;
                case ETypeCode.Guid:
                    sqlType = "text";
                    break;
                case ETypeCode.Binary:
                    sqlType = "bytea";
                    break;
                //case TypeCode.TimeSpan:
                //    SQLType = "time(7)";
                //    break;
                case ETypeCode.Unknown:
                    sqlType = "varchar(10485760)";
                    break;
                case ETypeCode.Decimal:
                    if (precision.ToString() == "")
                        precision = 28;
                    if (scale.ToString() == "")
                        scale = 0;
                    sqlType = "numeric (" + precision.ToString() + "," + scale.ToString() + ")";
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
                        returnValue = "to_timestamp('" + AddEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    else
                        returnValue = "to_timestamp('"+ AddEscape((string)value) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    break;
                case ETypeCode.Time:
                    if (value is TimeSpan)
						returnValue = "to_timestamp('" + AddEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "', 'YYYY-MM-DD HH24:MI:SS')";
					else
						returnValue = "to_timestamp('" + AddEscape((string)value) + "', 'YYYY-MM-DD HH24:MI:SS')";
					break;
                default:
                    throw new Exception("The datatype " + type.ToString() + " is not compatible with the sql insert statement.");
            }

            return returnValue;
        }

        public override async Task<ReturnValue<DbConnection>> NewConnection()
        {
            NpgsqlConnection connection = null;

            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
                    var hostport = Server.Split(':');
                    string port;
                    var host = hostport[0];
                    if(hostport.Count() == 1) 
                    {
                        port = "5432";
                    } else 
                    {
                        port = hostport[1];
                    }

					if (Ntauth == false)
						connectionString = "Host=" + host + "; Port=" + port + "; User Id=" + Username + "; Password=" + Password + "; ";
					else
						connectionString = "Host=" + host + "; Port=" + port + "; Integrated Security=true; ";

					if(!string.IsNullOrEmpty(DefaultDatabase)) 
					{
						connectionString += "Database = " + DefaultDatabase;
					}
                }

                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    return new ReturnValue<DbConnection>(false, "The PostgreSQL connection failed to open with a state of : " + connection.State.ToString(), null, null);
                }
                return new ReturnValue<DbConnection>(true, "", null, connection);
            }
            catch (Exception ex)
            {
                if(connection != null)
                    connection.Dispose();
                return new ReturnValue<DbConnection>(false, "The PostgreSQL connection failed with the following message: " + ex.Message, null, null);
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
                using (DbCommand cmd = CreateCommand(connection, "SELECT datname FROM pg_database WHERE datistemplate = false order by datname"))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        list.Add((string)reader["datname"]);
                    }
                }
                return new ReturnValue<List<string>>(true, "", null, list);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The databases could not be listed due to the following error: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<List<Table>>> GetTableList()
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

                    using (DbCommand cmd = CreateCommand(connection, "select table_catalog, table_schema, table_name from information_schema.tables where table_schema not in ('pg_catalog', 'information_schema')"))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
							var table = new Table()
							{
								TableName = reader["table_name"].ToString(),
								TableSchema = reader["table_schema"].ToString(),
							};
							tableList.Add(table);;
                        }
                    }

                }
                return new ReturnValue<List<Table>>(true, tableList);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<Table>>(false, "The database tables could not be listed due to the following error: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable)
        {
            try
            {
				var schema = string.IsNullOrEmpty(originalTable.TableSchema) ? "public" : originalTable.TableSchema;
                Table table = new Table(originalTable.TableName, originalTable.TableSchema);

                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<Table>(connectionResult.Success, connectionResult.Message, connectionResult.Exception);
                }

                using (var connection = connectionResult.Value)
                {

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"
                         select * from information_schema.columns where table_schema = '" + schema +  "' and table_name = '" + table.TableName + "'"
                            ))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {

                        //for the logical, just trim out any "
                        table.LogicalName = table.TableName.Replace("\"", "");

                        while (await reader.ReadAsync())
                        {
                            TableColumn col = new TableColumn();

                            //add the basic properties
                            col.ColumnName = reader["column_name"].ToString();
                            col.LogicalName = reader["column_name"].ToString();
                            col.IsInput = false;
                            col.Datatype = ConvertSqlToTypeCode(reader["data_type"].ToString());
                            if (col.Datatype == ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                //add the primary key
                                //if (Convert.ToBoolean(reader["PrimaryKey"]) == true)
                                //    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                                //else
                                col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            }

							if (col.Datatype == ETypeCode.String)
							{
								col.MaxLength = ConvertNullableToInt(reader["character_maximum_length"]);
							}
							else if (col.Datatype == ETypeCode.Double || col.Datatype == ETypeCode.Decimal)
							{
								col.Precision = ConvertNullableToInt(reader["numeric_precision_radix"]);
								col.Scale = ConvertNullableToInt(reader["numeric_scale"]);
							}

                            //make anything with a large string unlimited.  This will be created as varchar(max)
                            if (col.MaxLength > 4000)
                                col.MaxLength = null;

                            //col.Description = reader["Description"].ToString();
                            col.AllowDbNull = Convert.ToBoolean((string)reader["is_nullable"] == "YES");
                            //col.IsUnique = Convert.ToBoolean(reader["IsUnique"]);


                            table.Columns.Add(col);
                        }
                    }
                }
                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The source postgreSql table + " + originalTable.TableName + " could not be read due to the following error: " + ex.Message, ex);
            }
        }

		private Int32? ConvertNullableToInt(object value)
		{
			if(value == null || value is DBNull)
			{
				return null;
			}
			else 
			{
				var parsed = Int32.TryParse(value.ToString(), out int result);
				if(parsed) 
				{
					return result;
				}
				else 
				{
					return null;
				}
			}
		}


        public override ETypeCode ConvertSqlToTypeCode(string SqlType)
        {
            switch (SqlType)
            {
                case "bit": return ETypeCode.Boolean;
				case "varbit": return ETypeCode.Binary;
				case "bytea": return ETypeCode.Binary;

				case "smallint": return ETypeCode.Int16;
				case "int": return ETypeCode.Int32;
				case "integer": return ETypeCode.Int32;
				case "bigint": return ETypeCode.Int64;
				case "smallserial": return ETypeCode.Int16;
				case "serial": return ETypeCode.Int32;
				case "bigserial": return ETypeCode.Int64;
				case "numeric": return ETypeCode.Decimal;
				case "double precision": return ETypeCode.Double;
				case "real": return ETypeCode.Double;
				case "money": return ETypeCode.Decimal;
				case "bool": return ETypeCode.Boolean;
				case "boolean": return ETypeCode.Boolean;
				case "date": return ETypeCode.DateTime;
				case "timestamp": return ETypeCode.DateTime;
				case "timestamp without time zone": return ETypeCode.DateTime;
				case "timestamp with time zone": return ETypeCode.DateTime;
                case "interval": return ETypeCode.Time;
				case "time": return ETypeCode.Time;
				case "time without time zone": return ETypeCode.Time;
                case "time with time zone": return ETypeCode.Time;
				case "character varying": return ETypeCode.String;
				case "varchar": return ETypeCode.String;
				case "character": return ETypeCode.String;
				case "text": return ETypeCode.String;
            }
            return ETypeCode.Unknown;
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
                    cmd.CommandText = "delete from " + AddDelimiter(table.TableName);
                    try
                    {
                        await cmd.ExecuteNonQueryAsync(cancelToken);
                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue(false, "Delete table cancelled", null);
                    }
                    catch(Exception ex2)
                    {
                        return new ReturnValue(false, "The truncate and delete table query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex2);
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

			var autoIncrementSql = "";
			var deltaColumn = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
			if(deltaColumn != null) 
			{
				autoIncrementSql = "SELECT max(" + AddDelimiter(deltaColumn.ColumnName) + ") from " + AddDelimiter(table.TableName);
			}

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
							insert.Append(AddDelimiter( query.InsertColumns[i].Column.ColumnName) + ",");
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

        public static NpgsqlTypes.NpgsqlDbType GetSqlDbType(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return NpgsqlTypes.NpgsqlDbType.Smallint;
                case ETypeCode.SByte:
                    return NpgsqlTypes.NpgsqlDbType.Smallint;
                case ETypeCode.UInt16:
                    return NpgsqlTypes.NpgsqlDbType.Integer;
                case ETypeCode.UInt32:
                    return NpgsqlTypes.NpgsqlDbType.Bigint;
                case ETypeCode.UInt64:
                    return NpgsqlTypes.NpgsqlDbType.Bigint;
                case ETypeCode.Int16:
                    return NpgsqlTypes.NpgsqlDbType.Smallint;
                case ETypeCode.Int32:
                    return NpgsqlTypes.NpgsqlDbType.Integer;
                case ETypeCode.Int64:
                    return NpgsqlTypes.NpgsqlDbType.Bigint;
                case ETypeCode.Decimal:
                    return NpgsqlTypes.NpgsqlDbType.Numeric;
                case ETypeCode.Double:
                    return NpgsqlTypes.NpgsqlDbType.Double;
                case ETypeCode.Single:
                    return NpgsqlTypes.NpgsqlDbType.Double;
                case ETypeCode.String:
                    return NpgsqlTypes.NpgsqlDbType.Varchar;
                case ETypeCode.Boolean:
                    return NpgsqlTypes.NpgsqlDbType.Boolean;
                case ETypeCode.DateTime:
                    return NpgsqlTypes.NpgsqlDbType.Timestamp;
                case ETypeCode.Time:
                    return NpgsqlTypes.NpgsqlDbType.Time;
                case ETypeCode.Guid:
                    return NpgsqlTypes.NpgsqlDbType.Varchar;
                case ETypeCode.Binary:
                    return NpgsqlTypes.NpgsqlDbType.Bytea;
                default:
                    return NpgsqlTypes.NpgsqlDbType.Varchar;
            }
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<long>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, -1);
            }

            using (var connection = (NpgsqlConnection)connectionResult.Value)
            {

                StringBuilder sql = new StringBuilder();

                int rows = 0;

                var timer = Stopwatch.StartNew();

                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        sql.Clear();

                        sql.Append("update " + AddDelimiter(table.TableName) + " set ");

                        int count = 0;
                        foreach (QueryColumn column in query.UpdateColumns)
                        {
                            sql.Append(AddDelimiter(column.Column.ColumnName) + " = @col" + count.ToString() + ","); // cstr(count)" + GetSqlFieldValueQuote(column.Column.DataType, column.Value) + ",");
                            count++;
                        }
                        sql.Remove(sql.Length - 1, 1); //remove last comma
                        sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                        //  Retrieving schema for columns from a single table
                        using (NpgsqlCommand cmd = connection.CreateCommand())
                        {
                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            NpgsqlParameter[] parameters = new NpgsqlParameter[query.UpdateColumns.Count];
                            for (int i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                NpgsqlParameter param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i.ToString();
                                param.NpgsqlDbType = GetSqlDbType(query.UpdateColumns[i].Column.Datatype);
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
                                return new ReturnValue<long>(false, "The update query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, timer.ElapsedTicks);
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
