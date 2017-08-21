using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using static dexih.functions.DataType;
using dexih.transforms;
using System.Threading;
using System.Diagnostics;

namespace dexih.connections.sql
{
    public abstract class ConnectionSql : Connection
    {
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

        public override bool CanBulkLoad => true;
        public override bool CanSort => true;
        public override bool CanFilter => true;
        public override bool CanAggregate => true;
        public override bool CanUseBinary => true;
        public override bool CanUseSql => true;

        //These properties can be overridden for different databases
        public virtual string SqlDelimiterOpen { get; } = "\"";
        public virtual string SqlDelimiterClose { get; } = "\"";
        public virtual string SqlValueOpen { get; } = "'";
        public virtual string SqlValueClose { get; } = "'";
        public virtual string SqlFromAttribute(Table table) => "";

		public long MaxSqlSize { get; set; } = 4000000; // the largest size the sql command can be (default 4mb)


		public string AddDelimiter(string name)
        {
            var newName = AddEscape(name);

            if (newName.Substring(0, SqlDelimiterOpen.Length) != SqlDelimiterOpen)
                newName = SqlDelimiterOpen + newName;

            if (newName.Substring(newName.Length - SqlDelimiterClose.Length, SqlDelimiterClose.Length) != SqlDelimiterClose)
                newName = newName + SqlDelimiterClose;

            return newName;
        }

        public virtual string SqlTableName(Table table)
        {
            if(!string.IsNullOrEmpty(table.Schema))
            {
                return AddDelimiter(table.Schema) + "." + AddDelimiter(table.Name);
            }
            else
            {
                return AddDelimiter(table.Name);
            }
        }

        public string AddEscape(string value) => value.Replace("'", "''");


        public abstract Task<ReturnValue<DbConnection>> NewConnection();

        public abstract string GetSqlType(ETypeCode dataType, int? length, int? scale, int? precision);
        public abstract string GetSqlFieldValueQuote(ETypeCode type, object value);


        /// <summary>
        /// This is used to convert any datatypes that are not compatible with the target database.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public virtual object ConvertParameterType(object value)
        {
            return value;
        }


        public DbCommand CreateCommand(DbConnection connection, string commandText, DbTransaction transaction = null)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = commandText;
            cmd.Transaction = transaction;

            return cmd;
        }

        public DbParameter CreateParameter(DbCommand cmd, string parameterName, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = parameterName;
            param.Value = value;

            return param;
        }

         public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                var connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return new ReturnValue<long>(connectionResult);
                }

                using (var connection = connectionResult.Value)
                {
                    var fieldCount = reader.FieldCount;
                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    insert.Append("INSERT INTO " + SqlTableName(table) + " (");
                    values.Append("VALUES (");

                    for (var i = 0; i < fieldCount; i++)
                    {
						insert.Append(AddDelimiter(reader.GetName(i)) + ",");
                        values.Append("@col" + i.ToString() + ",");
                    }

                    var insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " + values.Remove(values.Length - 1, 1).ToString() + ");";

                    var timer = new Stopwatch();
                    timer.Start();
                    using (var transaction = connection.BeginTransaction())
                    {
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.CommandText = insertCommand;
                            //cmd.Transaction = transaction;

                            var parameters = new DbParameter[fieldCount];
                            for (var i = 0; i < fieldCount; i++)
                            {
                                var param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i.ToString();
                                cmd.Parameters.Add(param);
                                parameters[i] = param;
                            }

                            while (await reader.ReadAsync(cancelToken))
                            {
                                for (var i = 0; i < fieldCount; i++)
                                {
                                    parameters[i].Value = ConvertParameterType(reader[i]);
                                }
                                await cmd.ExecuteNonQueryAsync(cancelToken);
                                if (cancelToken.IsCancellationRequested)
                                {
                                    transaction.Rollback();
                                    return new ReturnValue<long>(false, "Insert rows cancelled.", null, timer.ElapsedTicks);
                                }
                            }
                        }
                        transaction.Commit();
                    }
                    timer.Stop();

                    return new ReturnValue<long>(true, timer.ElapsedTicks);
                }
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, "The following error occurred in the bulkload processing: " + ex.Message, ex);
            }
        }


        public virtual async Task<ReturnValue> DropTable(Table table)
        {
            try
            {
                var connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }

                using (var connection = connectionResult.Value)
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "drop table " + SqlTableName(table);

                    try
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue(false, "The following error occurred when attempting to drop the table " + table.Name + ".  " + ex.Message, ex);
                    }

                    return new ReturnValue(true);
                }
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when attempting to drop the table " + table.Name + ".  " + ex.Message, ex);
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
                var connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }
                using (var connection = connectionResult.Value)
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
                    createSql.Append("create table " + SqlTableName(table) + " ");

                    //sqlite does not support table/column comments, so add a comment string into the ddl.
                    if (!string.IsNullOrEmpty(table.Description))
                        createSql.Append(" -- " + table.Description);

                    createSql.AppendLine("");
                    createSql.Append("(");

                    for (var i = 0; i < table.Columns.Count; i++)
                    {
                        var col = table.Columns[i];

                        createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col.Datatype, col.MaxLength, col.Scale, col.Precision) + " ");
                        if (col.AllowDbNull == false)
                            createSql.Append("NOT NULL ");
                        else
                            createSql.Append("NULL ");

                        if (col.DeltaType == TableColumn.EDeltaType.SurrogateKey)
                            createSql.Append("PRIMARY KEY ASC ");

                        if (i < table.Columns.Count - 1)
                            createSql.Append(",");

                        if (!string.IsNullOrEmpty(col.Description))
                            createSql.Append(" -- " + col.Description);

                        createSql.AppendLine();
                    }

                    createSql.AppendLine(")");

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = createSql.ToString();
                        try
                        {
                            await command.ExecuteNonQueryAsync();
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.Name + ".  " + ex.Message, ex);
                        }
                    }

                    return new ReturnValue(true, "", null);
                }
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "An error occurred creating the table " + table.Name + ".  " + ex.Message, ex);
            }
        }


        public string GetSqlCompare(Filter.ECompare compare)
        {
            switch (compare)
            {
                case Filter.ECompare.IsEqual: return "=";
                case Filter.ECompare.GreaterThan: return ">";
                case Filter.ECompare.GreaterThanEqual: return ">=";
                case Filter.ECompare.LessThan: return "<";
                case Filter.ECompare.LessThanEqual: return "<=";
                case Filter.ECompare.NotEqual: return "!=";
                case Filter.ECompare.IsIn: return "IN";
                default:
                    return "";
            }
        }

        public virtual string AggregateFunction(SelectColumn column)
        {
            switch (column.Aggregate)
            {
                case SelectColumn.EAggregate.None: return AddDelimiter(column.Column.Name);
                case SelectColumn.EAggregate.Sum: return "sum(" + AddDelimiter(column.Column.Name) + ")";
                case SelectColumn.EAggregate.Average: return "avg(" + AddDelimiter(column.Column.Name) + ")";
                case SelectColumn.EAggregate.Min: return "min(" + AddDelimiter(column.Column.Name) + ")";
                case SelectColumn.EAggregate.Max: return "max(" + AddDelimiter(column.Column.Name) + ")";
                case SelectColumn.EAggregate.Count: return "count(" + AddDelimiter(column.Column.Name) + ")";
            }

            return ""; //not possible to get here.
        }

        private string BuildSelectQuery(Table table, SelectQuery query)
        {
            var sql = new StringBuilder();

            //if the query doesn't have any columns, then use all columns from the table.
            string columns;
            if (query?.Columns?.Count > 0)
                columns = string.Join(",", query.Columns.Select(AggregateFunction).ToArray());
            else
                columns = string.Join(",", table.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.IgnoreField).Select(c => AddDelimiter(c.Name)).ToArray());

            sql.Append("select ");
            sql.Append(columns + " ");
            sql.Append("from " + SqlTableName(table) + " ");
            sql.Append(" " + SqlFromAttribute(table) + " ");

            if (query?.Filters != null)
                sql.Append(BuildFiltersString(query.Filters));

            if (query?.Groups?.Count > 0)
            {
                sql.Append("group by ");
                sql.Append(string.Join(",", query.Groups.Select(c => AddDelimiter(c.Name)).ToArray()));
            }
            if (query?.Sorts?.Count > 0)
            {
                sql.Append("order by ");
                sql.Append(String.Join(",", query.Sorts.Select(c => AddDelimiter(c.Column.Name) + " " + (c.Direction == Sort.EDirection.Descending ? " desc" : "")).ToArray()));
            }

            return sql.ToString();
        }

        public virtual string BuildFiltersString(List<Filter> filters)
        {
            if (filters == null || filters.Count == 0)
                return "";
            else
            {

                var sql = new StringBuilder();
                sql.Append("where ");

                foreach (var filter in filters)
                {
                    if (filter.Column1 != null)
                        sql.Append(" " + AddDelimiter(filter.Column1.Name) + " ");
                    else
                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value1) + " ");

                    sql.Append(GetSqlCompare(filter.Operator));

                    if (filter.Column2 != null)
                        sql.Append(" " + AddDelimiter(filter.Column2.Name) + " ");
                    else
                    {
                        if(filter.Value2.GetType().IsArray)
                        {
                            var array = new List<string>();
                            foreach (var value in (Array)filter.Value2)
                                array.Add(value.ToString());
                            sql.Append(" (" + string.Join(",", array.Select(c => GetSqlFieldValueQuote(filter.CompareDataType, c))) + ") ");
                        }
                        else
                            sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value2) + " ");
                    }

                    sql.Append(filter.AndOr.ToString());
                }

                sql.Remove(sql.Length - 3, 3); //remove last or/and

                return sql.ToString();
            }
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<long>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, -1);
            }

            using (var connection = connectionResult.Value)
            {

                var sql = new StringBuilder();

                var rows = 0;

                var timer = Stopwatch.StartNew();

                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        sql.Clear();

                        sql.Append("update " + SqlTableName(table) + " set ");

                        var count = 0;
                        foreach (var column in query.UpdateColumns)
                        {
                            sql.Append(AddDelimiter(column.Column.Name) + " = @col" + count.ToString() + ","); // cstr(count)" + GetSqlFieldValueQuote(column.Column.DataType, column.Value) + ",");
                            count++;
                        }
                        sql.Remove(sql.Length - 1, 1); //remove last comma
                        sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                        //  Retrieving schema for columns from a single table
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            var parameters = new DbParameter[query.UpdateColumns.Count];
                            for (var i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                var param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i.ToString();
                                param.DbType = GetDbType(query.UpdateColumns[i].Column.Datatype);
                                param.Size = -1;

                                // GUID's get parameterized as binary.  So need to explicitly convert to string.
                                if (query.UpdateColumns[i].Column.Datatype == ETypeCode.Guid)
                                {
                                    param.Value = query.UpdateColumns[i].Value == null ? (object)DBNull.Value : query.UpdateColumns[i].Value.ToString();
                                }
                                else
                                {
                                    param.Value = query.UpdateColumns[i].Value == null ? DBNull.Value
                                        : query.UpdateColumns[i].Value;
                                }
                                
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

        public override async Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<long>(connectionResult.Success, connectionResult.Message, connectionResult.Exception, -1);
            }

            using (var connection = connectionResult.Value)
            {
                var sql = new StringBuilder();
                var rows = 0;

                var timer = Stopwatch.StartNew();

                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var query in queries)
                    {
                        sql.Clear();
                        sql.Append("delete from " + SqlTableName(table) + " ");
                        sql.Append(BuildFiltersString(query.Filters));


                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.Transaction = transaction;
                            cmd.CommandText = sql.ToString();

                            try
                            {
                                rows += await cmd.ExecuteNonQueryAsync(cancelToken);

                                if (cancelToken.IsCancellationRequested)
                                {
                                    return new ReturnValue<long>(false, "Delete rows cancelled.", null, timer.ElapsedTicks);
                                }
                            }
                            catch (Exception ex)
                            {
                                return new ReturnValue<long>(false, "The delete query for " + table.Name + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, timer.ElapsedTicks);
                            }
                        }
                    }
                    transaction.Commit();
                }

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks); //sometimes reader returns -1, when we want this to be error condition.
            }
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<object>(connectionResult);
            }

            using (var connection = connectionResult.Value)
            {
                var sql = BuildSelectQuery(table, query);

                //  Retrieving schema for columns from a single table
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;

                    object value;
                    try
                    {
                        value = await cmd.ExecuteScalarAsync(cancelToken);

                        if (cancelToken.IsCancellationRequested)
                            return new ReturnValue<object>(false, "Execute scalar cancelled.", null);

                        var returnValue = DataType.TryParse(table.Columns[query.Columns[0].Column].Datatype, value);
                        if (!returnValue.Success)
                            return new ReturnValue<object>(returnValue);
                        return new ReturnValue<object>(true, returnValue.Value);
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<object>(false, "The select query for " + table.Name + " could not be run due to the following error: " + ex.Message + ".  Sql command was: " + sql, ex, null);
                    }
                }
            }
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
                cmd.CommandText = "delete from " + SqlTableName(table);

                try
                {
                    await cmd.ExecuteNonQueryAsync(cancelToken);

                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<int>(false, "Truncate cancelled.", null);
                }
                catch (Exception ex)
                {
                    return new ReturnValue(false, "The truncate table query for " + table.Name + " could not be run due to the following error: " + ex.Message, ex);
                }
            }

            return new ReturnValue(true, "", null);
        }

        public override async Task<ReturnValue<Table>> InitializeTable(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue<Table>(true, table));
        }

        public async Task<ReturnValue<Table>> GetQueryTable(Table table, CancellationToken cancelToken)
        {
            var connectionResult = await NewConnection();
            if (connectionResult.Success == false)
            {
                return new ReturnValue<Table>(connectionResult);
            }
            
            using (var connection = connectionResult.Value)
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = table.QueryString;

                try
                {
                    var reader = await cmd.ExecuteReaderAsync(cancelToken);
                    var newTable = new Table();
                    table.CopyProperties(newTable, true);

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var col = new TableColumn()
                        {
                            Name = reader.GetName(i),
                            LogicalName = reader.GetName(i),
                            Datatype = GetTypeCode(reader.GetFieldType(i)),
                            DeltaType = TableColumn.EDeltaType.TrackingField,
                        };
                        newTable.Columns.Add(col);
                    }
                    
                    return  new ReturnValue<Table>(true, newTable);
                }
                
                catch (Exception ex)
                {
                    return new ReturnValue<Table>(false, "The query " + table.QueryString + " could not be run due to the following error: " + ex.Message, ex);
                }
            }
        }

        public override async Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            try
            {
                var cmd = connection.CreateCommand();

                cmd.CommandText = table.UseQuery ? table.QueryString : BuildSelectQuery(table, query);
                
                DbDataReader reader;
                try
                {
                    reader = await cmd.ExecuteReaderAsync(cancelToken);
                }
                catch (Exception ex)
                {
                    return new ReturnValue<DbDataReader>(false, "The connection reader for the table " + table.Name + " failed due to the following error: " + ex.Message + ".  The sql command was: " + cmd.CommandText, ex);
                }

                if (reader == null)
                {
                    return new ReturnValue<DbDataReader>(false, "The connection reader for the table " + table.Name + " return null for an unknown reason.  The sql command was: " + cmd.CommandText, null);
                }
                
                return new ReturnValue<DbDataReader>(true, reader);
            }
            catch (Exception ex)
            {
                return new ReturnValue<DbDataReader>(false, "The following error was encountered starting the connection reader for the table " + table.Name + ": " + ex.Message, ex);
            }
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderSql(this, table);
            return reader;
        }
        
    }
}
