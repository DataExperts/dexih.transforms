using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using dexih.functions;
using System.Data.Common;
using static dexih.functions.DataType;
using System.Threading;
using System.Diagnostics;
using MySql.Data.MySqlClient;

namespace dexih.connections.sql
{
    public class ConnectionMySql : ConnectionSql
    {

        public override string ServerHelp => "Server";
        public override string DefaultDatabaseHelp => "Database";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override string DatabaseTypeName => "MySql";
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

        public override string SqlDelimiterOpen { get; } = "`";
        public override string SqlDelimiterClose { get; } = "`";

//		public override object ConvertParameterType(object value)
//		{
//            switch (value)
//            {
//                case UInt16 uint16:
//                    return (Int32)uint16;
//                case UInt32 uint32:
//                    return (Int64)uint32;
//				case UInt64 uint64:
//					return (Int64)uint64;
//				default:
//                    return value;
//            }
//		}
        
        public override object GetDataTypeMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(9999,12,31);
                case ETypeCode.Time:
                    return TimeSpan.FromDays(1) - TimeSpan.FromSeconds(1); //mysql doesn't support milliseconds

                default:
                    return DataType.GetDataTypeMaxValue(typeCode, length);
            }
        }
	    
        public override object GetDataTypeMinValue(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.DateTime:
                    return new DateTime(1000,1,1);
                default:
                    return DataType.GetDataTypeMinValue(typeCode);
            }
		    
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<bool>(connectionResult);
            }

            using (var connection = connectionResult.Value)
            using (var cmd = CreateCommand(connection, "SHOW TABLES LIKE @NAME"))
            {
                cmd.Parameters.Add(CreateParameter(cmd, "@NAME", table.Name));

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

                var createSql = new StringBuilder();

                //Create the table
                createSql.Append("create table " + AddDelimiter(table.Name) + " ( ");
                foreach (var col in table.Columns)
                {
                    createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col.Datatype, col.MaxLength, col.Scale, col.Precision));
                    
                    if (col.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                        createSql.Append(" auto_increment");

                    createSql.Append(col.AllowDbNull == false ? " NOT NULL" : " NULL");
                    createSql.Append(",");
                }

				//Add the primary key using surrogate key or autoincrement.
				var key = table.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey) ?? table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);

                if (key != null)
					createSql.Append("PRIMARY KEY (" + AddDelimiter(key.Name) + "),");


				//remove the last comma
				createSql.Remove(createSql.Length - 1, 1);
                createSql.Append(")");


				var connectionResult = await NewConnection();
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
						await command.ExecuteNonQueryAsync(cancelToken);
					}
					catch (Exception ex)
					{
						return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.Name + ".  " + ex.Message, ex);
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
                case ETypeCode.Byte:
                    sqlType = "tinyint unsigned";
                    break;
                case ETypeCode.SByte:
                    sqlType = "tinyint";
                    break;
                case ETypeCode.UInt16:
                    sqlType = "smallint unsigned";
                    break;
				case ETypeCode.Int16:
                    sqlType = "smallint";
                    break;
                case ETypeCode.UInt32:
                    sqlType = "int unsigned";
                    break;
                case ETypeCode.Int32:
                    sqlType = "int";
                    break;
                case ETypeCode.Int64:
                    sqlType = "bigint";
                    break;
				case ETypeCode.UInt64:
					sqlType = "bigint unsigned";
                    break;
                case ETypeCode.String:
                    if (length == null)
                        sqlType = "varchar(4000)";
                    else
                        sqlType = "varchar(" + length.ToString() + ")";
                    break;
                case ETypeCode.Single:
                    sqlType = "real";
                    break;
                case ETypeCode.Double:
                    sqlType = "double";
                    break;
                case ETypeCode.Boolean:
                    sqlType = "bit(1)";
                    break;
                case ETypeCode.DateTime:
                    sqlType = "DateTime";
                    break;
                case ETypeCode.Time:
                    sqlType = "time";
                    break;
                case ETypeCode.Guid:
                    sqlType = "char(40)";
                    break;
                case ETypeCode.Binary:
                    sqlType = "blob";
                    break;
                case ETypeCode.Unknown:
                    sqlType = "text";
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
                case ETypeCode.Boolean:
                    returnValue = AddEscape(value.ToString());
                    break;
                case ETypeCode.String:
                case ETypeCode.Guid:
                case ETypeCode.Unknown:
                    returnValue = "'" + AddEscape(value.ToString()) + "'";
                    break;
                case ETypeCode.DateTime:
                    if (value is DateTime)
                        returnValue = "STR_TO_DATE('" + AddEscape(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "', '%Y-%m-%d %H:%i:%s.%f')";
                    else
                        returnValue = "STR_TO_DATE('"+ AddEscape((string)value) + "', 'YYYY-MM-DD HH24:MI:SS')";
                    break;
                case ETypeCode.Time:
                    if (value is TimeSpan)
						returnValue = "STR_TO_DATE('" + AddEscape(((TimeSpan)value).ToString("yyyy-MM-dd HH:mm:ss.ff")) + "', '%Y-%m-%d %H:%i:%s.%f')";
					else
						returnValue = "STR_TO_DATE('" + AddEscape((string)value) + "', '%Y-%m-%d %H:%i:%s.%f')";
					break;
                default:
                    throw new Exception("The datatype " + type.ToString() + " is not compatible with the sql insert statement.");
            }

            return returnValue;
        }

        public override async Task<ReturnValue<DbConnection>> NewConnection()
        {
            MySqlConnection connection = null;

            try
            {
                string connectionString;
                if (UseConnectionString)
                    connectionString = ConnectionString;
                else
                {
					if (UseWindowsAuth == false)
						connectionString = "Server=" + Server + "; uid=" + Username + "; pwd=" + Password + "; ";
					else
						connectionString = "Server=" + Server + "; IntegratedSecurity=yes; Uid=auth_windows;";

					if(!string.IsNullOrEmpty(DefaultDatabase)) 
					{
						connectionString += "Database = " + DefaultDatabase;
					}
                }

                connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();
                State = (EConnectionState)connection.State;

                if (connection.State != ConnectionState.Open)
                {
                    connection.Dispose();
                    return new ReturnValue<DbConnection>(false, "The MySql connection failed to open with a state of : " + connection.State.ToString(), null, null);
                }
                return new ReturnValue<DbConnection>(true, "", null, connection);
            }
            catch (Exception ex)
            {
                if(connection != null)
                    connection.Dispose();
                return new ReturnValue<DbConnection>(false, "The MySql connection failed with the following message: " + ex.Message, null, null);
            }
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            try
            {
                DefaultDatabase = "";
                var connectionResult = await NewConnection();

                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<string>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                using (var connection = connectionResult.Value)
                using (var cmd = CreateCommand(connection, "create database " + AddDelimiter(databaseName)))
                {
                    var value = await cmd.ExecuteNonQueryAsync(cancelToken);
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
                var connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<string>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                var list = new List<string>();

                using (var connection = connectionResult.Value)
                using (var cmd = CreateCommand(connection, "SHOW DATABASES"))
                using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                {
                    while (await reader.ReadAsync(cancelToken))
                    {
                        list.Add((string)reader["Database"]);
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
                var connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<List<Table>>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, null);
                }

                var tableList = new List<Table>();

                using (var connection = connectionResult.Value)
                {

                    using (var cmd = CreateCommand(connection, "SHOW TABLES"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                    {
                        while (await reader.ReadAsync(cancelToken))
                        {
							var table = new Table()
							{
								Name = reader[0].ToString(),
								Schema = "",
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

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            if (originalTable.UseQuery)
            {
                return await GetQueryTable(originalTable, cancelToken);
            }
            
            try
            {
				var schema = string.IsNullOrEmpty(originalTable.Schema) ? "public" : originalTable.Schema;
                var table = new Table(originalTable.Name, originalTable.Schema);

                var connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<Table>(connectionResult.Success, connectionResult.Message, connectionResult.Exception);
                }

                using (var connection = connectionResult.Value)
                {

                    //The new datatable that will contain the table schema
                    table.Columns.Clear();

                    // The schema table 
                    using (var cmd = CreateCommand(connection, @"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + DefaultDatabase + "' AND TABLE_NAME='" + table.Name + "'"))
                    using (var reader = await cmd.ExecuteReaderAsync(cancelToken))
                    {

                        //for the logical, just trim out any "
                        table.LogicalName = table.Name.Replace("\"", "");

                        while (await reader.ReadAsync(cancelToken))
                        {
                            var isSigned = reader["COLUMN_TYPE"].ToString().IndexOf("unsigned", StringComparison.Ordinal) > 0;
                            var col = new TableColumn
                            {
                                Name = reader["COLUMN_NAME"].ToString(),
                                LogicalName = reader["COLUMN_NAME"].ToString(),
                                IsInput = false,
                                Datatype = ConvertSqlToTypeCode(reader["DATA_TYPE"].ToString(), isSigned),
                                AllowDbNull = reader["IS_NULLABLE"].ToString() != "NO" 
                            };

                            if (reader["COLUMN_KEY"].ToString() == "PRI")
                            {
                                col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                            }
                            else if (col.Datatype == ETypeCode.Unknown)
                            {
                                col.DeltaType = TableColumn.EDeltaType.IgnoreField;
                            }
                            else
                            {
                                col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            }

							switch (col.Datatype)
							{
							    case ETypeCode.String:
							        col.MaxLength = ConvertNullableToInt(reader["CHARACTER_MAXIMUM_LENGTH"]);
							        break;
							    case ETypeCode.Double:
							    case ETypeCode.Decimal:
							        col.Precision = ConvertNullableToInt(reader["NUMERIC_PRECISION"]);
							        col.Scale = ConvertNullableToInt(reader["NUMERIC_SCALE"]);
							        break;
							}

                            //make anything with a large string unlimited.  This will be created as varchar(max)
                            if (col.MaxLength > 4000)
                                col.MaxLength = null;

                            col.Description = reader["COLUMN_COMMENT"].ToString();
                            //col.IsUnique = Convert.ToBoolean(reader["IsUnique"]);


                            table.Columns.Add(col);
                        }
                    }
                }
                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The source MySql table + " + originalTable.Name + " could not be read due to the following error: " + ex.Message, ex);
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


        public ETypeCode ConvertSqlToTypeCode(string sqlType, bool isSigned)
        {
            switch (sqlType)
            {
				case "bit": 
				    return ETypeCode.Boolean;
				case "tinyint": 
				    return isSigned ? ETypeCode.SByte : ETypeCode.Byte;
				case "year": 
				    return  ETypeCode.Int16;				       
				case "smallint": 
                    return isSigned ? ETypeCode.Int16 : ETypeCode.UInt16;
                case "mediumint":
				case "int": 
                    return isSigned ? ETypeCode.Int32 : ETypeCode.UInt32;
				case "bigint": 
                    return isSigned ? ETypeCode.Int64 : ETypeCode.UInt64;
				case "numeric": 
                case "decimal": 
                    return ETypeCode.Decimal;
				case "float":
                case "real":
                case "double precicion":
				    return ETypeCode.Double;
				case "bool": 
				case "boolean": 
				    return ETypeCode.Boolean;
				case "date":
				case "datetime":
				case "timestamp":
				    return ETypeCode.DateTime;
				case "time": 
				    return ETypeCode.Time;
				case "char": 
				case "varchar": 
				case "enum": 
				case "set": 
				case "text": 
				case "tinytext": 
				case "mediumtext": 
				case "longtext": 
				    return ETypeCode.String;
                case "binary": 
                case "varbinary": 
                case "tinyblob": 
                case "blob": 
                case "mediumblob": 
                case "longblob":
                    return ETypeCode.Binary;
            }
            return ETypeCode.Unknown;
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return connectionResult;
            }

            using (var connection = connectionResult.Value)
            using (var cmd = connection.CreateCommand())
            {

                cmd.CommandText = "truncate table " + AddDelimiter(table.Name);

                try
                {
                    await cmd.ExecuteNonQueryAsync(cancelToken);
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue(false, "Truncate cancelled", null);
                }
                catch (Exception ex)
                {
                    cmd.CommandText = "delete from " + AddDelimiter(table.Name);
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
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<Tuple<long, long>>(false, connectionResult.Message, connectionResult.Exception);
            }

			var autoIncrementSql = "";
			var deltaColumn = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
			if(deltaColumn != null) 
			{
				autoIncrementSql = "SELECT max(" + AddDelimiter(deltaColumn.Name) + ") from " + AddDelimiter(table.Name);
			}

            long identityValue = 0;

            using (var connection = connectionResult.Value)
            {
                var insert = new StringBuilder();
                var values = new StringBuilder();

                var timer = Stopwatch.StartNew();
                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        insert.Clear();
                        values.Clear();

                        insert.Append("INSERT INTO " + AddDelimiter(table.Name) + " (");
                        values.Append("VALUES (");

                        for (var i = 0; i < query.InsertColumns.Count; i++)
                        {
							insert.Append(AddDelimiter( query.InsertColumns[i].Column.Name) + ",");
                            values.Append("@col" + i.ToString() + ",");
                        }

                        var insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " +
                            values.Remove(values.Length - 1, 1).ToString() + "); " + autoIncrementSql;

                        try
                        {
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = insertCommand;
                                cmd.Transaction = transaction;

                                for (var i = 0; i < query.InsertColumns.Count; i++)
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

//        public static MySqlDbType GetSqlDbType(ETypeCode typeCode)
//        {
//            switch (typeCode)
//            {
//                case ETypeCode.Byte:
//                    return MySqlDbType.Byte;
//                case ETypeCode.SByte:
//                    return MySqlDbType.Byte;
//                case ETypeCode.UInt16:
//                    return MySqlDbType.UInt16;
//                case ETypeCode.UInt32:
//                    return MySqlDbType.UInt32;
//                case ETypeCode.UInt64:
//                    return MySqlDbType.UInt64;
//                case ETypeCode.Int16:
//                    return MySqlDbType.Int16;
//                case ETypeCode.Int32:
//                    return MySqlDbType.Int32;
//                case ETypeCode.Int64:
//                    return MySqlDbType.Int64;
//                case ETypeCode.Decimal:
//                    return MySqlDbType.Decimal;
//                case ETypeCode.Double:
//                    return MySqlDbType.Double;
//                case ETypeCode.Single:
//                    return MySqlDbType.Double;
//                case ETypeCode.String:
//                    return MySqlDbType.VarChar;
//                case ETypeCode.Boolean:
//                    return MySqlDbType.Bit;
//                case ETypeCode.DateTime:
//                    return MySqlDbType.DateTime;
//                case ETypeCode.Time:
//                    return MySqlDbType.Time;
//                case ETypeCode.Guid:
//                    return MySqlDbType.VarChar;
//                case ETypeCode.Binary:
//                    return MySqlDbType.Binary;
//                default:
//                    return MySqlDbType.VarChar;
//            }
//        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            try
            {

            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<long>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, -1);
            }

            using (var connection = (MySqlConnection)connectionResult.Value)
            {

                var sql = new StringBuilder();

                var rows = 0;

                var timer = Stopwatch.StartNew();

                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        sql.Clear();
                        sql.Append("update " + AddDelimiter(table.Name) + " set ");

                        var count = 0;
                        foreach (var column in query.UpdateColumns)
                        {
                            sql.Append(AddDelimiter(column.Column.Name) + " = @col" + count + ",");
                            count++;
                        }
                        sql.Remove(sql.Length - 1, 1); //remove last comma
                        sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                        //  Retrieving schema for columns from a single table
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            var parameters = new MySqlParameter[query.UpdateColumns.Count];
                            for (var i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                var param = new MySqlParameter
                                {
                                    ParameterName = "@col" + i,
                                    Value = query.UpdateColumns[i].Value == null
                                        ? DBNull.Value
                                        : query.UpdateColumns[i].Value
                                };
                                // param.MySqlDbType = GetSqlDbType(query.UpdateColumns[i].Column.Datatype);
                                // param.Size = -1;
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
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

        }
    }
}
