using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Exceptions;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.sql
{
    public abstract class ConnectionSql : Connection
    {
        public override EConnectionCategory DatabaseConnectionCategory => EConnectionCategory.SqlDatabase;

        public override bool CanBulkLoad => true;
        public override bool CanSort => true;
        public override bool CanFilter => true;
        public override bool CanDelete => true;
        public override bool CanUpdate => true;
        public override bool CanAggregate => true;
        public override bool CanUseBinary => true;
        public override bool CanUseSql => true;
        public override bool DynamicTableCreation => false;


        //These properties can be overridden for different databases
        protected virtual string SqlDelimiterOpen { get; } = "\"";

        protected virtual string SqlDelimiterClose { get; } = "\"";
        public virtual string SqlValueOpen { get; } = "'";
        public virtual string SqlValueClose { get; } = "'";
        protected virtual string SqlFromAttribute(Table table) => "";

        protected long MaxSqlSize { get; set; } = 4000000; // the largest size the sql command can be (default 4mb)


        protected string AddDelimiter(string name)
        {
            var newName = AddEscape(name);

            if (newName.Substring(0, SqlDelimiterOpen.Length) != SqlDelimiterOpen)
                newName = SqlDelimiterOpen + newName;

            if (newName.Substring(newName.Length - SqlDelimiterClose.Length, SqlDelimiterClose.Length) != SqlDelimiterClose)
                newName = newName + SqlDelimiterClose;

            return newName;
        }

        protected virtual string SqlTableName(Table table)
        {
            if (!string.IsNullOrEmpty(table.Schema))
            {
                return AddDelimiter(table.Schema) + "." + AddDelimiter(table.Name);
            }

            return AddDelimiter(table.Name);
        }

         protected string AddEscape(string value) => value.Replace("'", "''");


        public abstract Task<DbConnection> NewConnection();

        protected abstract string GetSqlType(TableColumn column);
        protected abstract string GetSqlFieldValueQuote(ETypeCode type, object value);


        /// <summary>
        /// This is used to convert any datatypes that are not compatible with the target database.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public virtual object ConvertParameterType(object value)
        {
            return value;
        }


        protected DbCommand CreateCommand(DbConnection connection, string commandText, DbTransaction transaction = null)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = commandText;
            cmd.Transaction = transaction;

            return cmd;
        }

        protected DbParameter CreateParameter(DbCommand cmd, string parameterName, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = parameterName;
            param.Value = value;

            return param;
        }

        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                {
                    var fieldCount = reader.FieldCount;
                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    insert.Append("INSERT INTO " + SqlTableName(table) + " (");
                    values.Append("VALUES (");

                    for (var i = 0; i < fieldCount; i++)
                    {
                        insert.Append(AddDelimiter(reader.GetName(i)) + ",");
                        values.Append("@col" + i + ",");
                    }

                    var insertCommand = insert.Remove(insert.Length - 1, 1) + ") " + values.Remove(values.Length - 1, 1) + ");";

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
                                param.ParameterName = "@col" + i;
                                cmd.Parameters.Add(param);
                                parameters[i] = param;
                            }

                            while (await reader.ReadAsync(cancellationToken))
                            {
                                for (var i = 0; i < fieldCount; i++)
                                {
                                    parameters[i].Value = ConvertParameterType(reader[i]);
                                }
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                                if (cancellationToken.IsCancellationRequested)
                                {
                                    transaction.Rollback();
                                    cancellationToken.ThrowIfCancellationRequested();
                                }
                            }
                        }
                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Bulk insert failed.  {ex.Message}", ex);
            }
        }


        public virtual async Task<bool> DropTable(Table table)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "drop table " + SqlTableName(table);
                    await command.ExecuteNonQueryAsync();

                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Drop table {table.Name} failed.  {ex.Message}", ex);
            }
        }

        /// <summary>
        /// This creates a table in a managed database.  Only works with tables containing a surrogate key.
        /// </summary>
        /// <returns></returns>
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                {
                    var tableExists = await TableExists(table, cancellationToken);

                    //if table exists, and the dropTable flag is set to false, then error.
                    if (tableExists && dropTable == false)
                    {
                        throw new ConnectionException($"The table {table.Name} already exists on {Name} and the drop table option is set to false.");
                    }

                    //if table exists, then drop it.
                    if (tableExists)
                    {
                        await DropTable(table);
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

                        createSql.Append(AddDelimiter(col.Name) + " " + GetSqlType(col) + " ");
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
                        await command.ExecuteNonQueryAsync(cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Create table {table.Name} failed.  {ex.Message}", ex);
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

        protected string AggregateFunction(SelectColumn column)
        {
            switch (column.Aggregate)
            {
                case null: return AddDelimiter(column.Column.Name);
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
                sql.Append(string.Join(",", query.Sorts.Select(c => AddDelimiter(c.Column.Name) + " " + (c.Direction == Sort.EDirection.Descending ? " desc" : "")).ToArray()));
            }

            return sql.ToString();
        }

        protected string BuildFiltersString(List<Filter> filters)
        {
            if (filters == null || filters.Count == 0)
                return "";
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
                    if (filter.Value2.GetType().IsArray)
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

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                {

                    var sql = new StringBuilder();

                    var rows = 0;

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var query in queries)
                        {
                            sql.Clear();

                            sql.Append("update " + SqlTableName(table) + " set ");

                            var count = 0;
                            foreach (var column in query.UpdateColumns)
                            {
                                sql.Append(AddDelimiter(column.Column.Name) + " = @col" + count + ","); // cstr(count)" + GetSqlFieldValueQuote(column.Column.DataType, column.Value) + ",");
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
                                    param.ParameterName = "@col" + i;
                                    param.DbType = GetDbType(query.UpdateColumns[i].Column.DataType);
                                    // param.Size = -1;

                                    // GUID's get parameterized as binary.  So need to explicitly convert to string.
                                    if (query.UpdateColumns[i].Column.DataType == ETypeCode.Guid)
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

                                cancellationToken.ThrowIfCancellationRequested();

                                try
                                {
                                    rows += await cmd.ExecuteNonQueryAsync(cancellationToken);
                                }
                                catch (Exception ex)
                                {
                                    throw new ConnectionException($"The update query failed. {ex.Message}", ex);
                                }
                            }
                        }
                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Update table {table.Name} failed.  {ex.Message}", ex);
            }
        }

        public override async Task ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
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

                            cancellationToken.ThrowIfCancellationRequested();

                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.Transaction = transaction;
                                cmd.CommandText = sql.ToString();

                                try
                                {
                                    rows += await cmd.ExecuteNonQueryAsync(cancellationToken);
                                }
                                catch (Exception ex)
                                {
                                    throw new ConnectionException($"The delete query failed. {ex.Message}", ex);
                                }
                            }
                        }
                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Delete rows from table {table.Name} failed.  {ex.Message}", ex);
            }

        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                {
                    var sql = BuildSelectQuery(table, query);

                    //  Retrieving schema for columns from a single table
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = sql;

                        object value;
                        try
                        {
                            value = await cmd.ExecuteScalarAsync(cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"The database query failed.  {ex.Message}", ex);
                        }

                        try
                        {
                            return TryParse(table.Columns[query.Columns[0].Column].DataType, value);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"The value in column {query.Columns[0].Column.Name} was incompatible with data type {query.Columns[0].Column.DataType}.  {ex.Message}", ex);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get value from table {table.Name} failed.  {ex.Message}", ex);
            }

        }

        public override async Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            try
            {

                using (var connection = await NewConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "delete from " + SqlTableName(table);

                    try
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"The truncate query failed. {ex.Message}", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Truncate table {table.Name} failed.  {ex.Message}", ex);
            }

        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            return Task.FromResult(table);
        }

        public async Task<Table> GetQueryTable(Table table, CancellationToken cancellationToken)
        {
            try
            {

                using (var connection = await NewConnection())
                using (var cmd = connection.CreateCommand())
                {
                    DbDataReader reader;

                    try
                    {
                        cmd.CommandText = table.QueryString;
                        reader = await cmd.ExecuteReaderAsync(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"The query [{table.QueryString}] failed. {ex.Message}", ex);
                    }

                    var newTable = new Table();
                    table.CopyProperties(newTable, true);

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var col = new TableColumn
                        {
                            Name = reader.GetName(i),
                            LogicalName = reader.GetName(i),
                            DataType = GetTypeCode(reader.GetFieldType(i)),
                            DeltaType = TableColumn.EDeltaType.TrackingField
                        };
                        newTable.Columns.Add(col);
                    }

                    return newTable;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Query table {table.Name} failed.  {ex.Message}", ex);
            }

        }

        public override async Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
            try
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = table.UseQuery ? table.QueryString : BuildSelectQuery(table, query);
                DbDataReader reader;

                try
                {
                    reader = await cmd.ExecuteReaderAsync(cancellationToken);
                }
                catch (Exception ex)
                {
#if DEBUG
                    throw new ConnectionException($"The reader for table {table.Name} returned failed.  {ex.Message}.  The command was: {cmd.CommandText}", ex);
#else
                        throw new ConnectionException($"The reader for table {table.Name} returned failed.  {ex.Message}", ex);
#endif
                }

                if (reader == null)
                {
                    throw new ConnectionException($"The reader for table {table.Name} returned null.");
                }
                return reader;

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get database reader {table.Name} failed.  {ex.Message}", ex);
            }

        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderSql(this, table);
            return reader;
        }

    }
}
