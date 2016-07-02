using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Microsoft.Data.Sqlite;
using dexih.functions;
using Newtonsoft.Json;
using System.IO;
using System.Data.Common;
using static dexih.functions.DataType;
using dexih.transforms;
using System.Threading;

namespace dexih.connections
{
    public abstract class ConnectionSql : Connection
    {
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

        public override bool CanBulkLoad => true;
        public override bool CanSort => true;
        public override bool CanFilter => true;
        public override bool CanAggregate => true;



        //These properties can be overridden for different databases
        public virtual string SqlDelimiterOpen { get; } = "\"";
        public virtual string SqlDelimiterClose { get; } = "\"";
        public virtual string SqlValueOpen { get; } = "'";
        public virtual string SqlValueClose { get; } = "'";
        public virtual string SqlSelectNoLock { get; } = "";

        public string AddDelimiter(string name)
        {
            string newName = AddEscape(name);

            if (newName.Substring(0, SqlDelimiterOpen.Length) != SqlDelimiterOpen)
                newName = SqlDelimiterOpen + newName;

            if(newName.Substring(newName.Length - SqlDelimiterClose.Length, SqlDelimiterClose.Length) != SqlDelimiterClose)
                newName = newName + SqlDelimiterClose;

            return newName;

        }

        public string AddEscape(string value) => value.Replace("'", "''");


        public abstract Task<ReturnValue<DbConnection>> NewConnection();

        public abstract string GetSqlType(ETypeCode dataType, int? length, int? scale, int? precision);
        public abstract ETypeCode ConvertSqlToTypeCode(string SqlType);
        public abstract string GetSqlFieldValueQuote(ETypeCode type, object value);

        public abstract Task<ReturnValue<Boolean>> TableExists(Table table);

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

        public override async Task<ReturnValue<int>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                ReturnValue<DbConnection> connection = await NewConnection();
                if (connection.Success == false)
                {
                    return new ReturnValue<int>(connection);
                }

                StringBuilder insert = new StringBuilder();
                StringBuilder values = new StringBuilder();

                insert.Append("INSERT INTO " + AddDelimiter(table.TableName) + " (");
                values.Append("VALUES (");

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    insert.Append("[" + reader.GetName(i) + "],");
                    values.Append("@col" + i.ToString() + ",");
                }

                string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " + values.Remove(values.Length - 1, 1).ToString() + ");";

                using (var transaction = connection.Value.BeginTransaction())
                {
                    using (var cmd = connection.Value.CreateCommand())
                    {
                        cmd.CommandText = insertCommand;
                        cmd.Transaction = transaction;
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            DbParameter param = cmd.CreateParameter();
                            param.ParameterName = "@col" + i.ToString();
                            param.Value = "";
                            cmd.Parameters.Add(param);
                        }
                        cmd.Prepare();

                        while (await reader.ReadAsync(cancelToken))
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                cmd.Parameters[i].Value = ConvertParameterType(reader[i]);
                            }
                            await cmd.ExecuteNonQueryAsync(cancelToken);
                            if (cancelToken.IsCancellationRequested)
                                return new ReturnValue<int>(false, "Insert rows cancelled.", null);
                        }
                    }
                    transaction.Commit();
                }

                connection.Value.Dispose();

                return new ReturnValue<int>(true, 0);
            }
            catch (Exception ex)
            {
                return new ReturnValue<int>(false, "The following error occurred in the bulkload processing: " + ex.Message, ex);
            }
        }


        public virtual async Task<ReturnValue> DropTable(Table table)
        {
            try
            {
                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }

                DbConnection connection = connectionResult.Value;
                DbCommand command = connection.CreateCommand();
                command.CommandText = "drop table " + AddDelimiter(table.TableName);

                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    return new ReturnValue(false, "The following error occurred when attempting to drop the table " + table.TableName + ".  " + ex.Message, ex);
                }

                return new ReturnValue(true);
            }
            catch(Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when attempting to drop the table " + table.TableName + ".  " + ex.Message, ex);
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
                ReturnValue<DbConnection> connectionResult = await NewConnection();
                if (connectionResult.Success == false)
                {
                    return connectionResult;
                }

                DbConnection connection = connectionResult.Value;

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
                createSql.Append("create table " + AddDelimiter(table.TableName) + " ");

                //sqlite does not support table/column comments, so add a comment string into the ddl.
                if(!string.IsNullOrEmpty(table.Description))
                    createSql.Append(" -- " + table.Description);

                createSql.AppendLine("");
                createSql.Append("(");

                for(int i = 0; i< table.Columns.Count; i++)
                {
                    TableColumn col = table.Columns[i];

                    createSql.Append(AddDelimiter(col.ColumnName) + " " + GetSqlType(col.DataType, col.MaxLength, col.Scale, col.Precision) + " ");
                    if (col.AllowDbNull == false)
                        createSql.Append("NOT NULL ");
                    else
                        createSql.Append("NULL ");

                    if (col.DeltaType == TableColumn.EDeltaType.SurrogateKey)
                        createSql.Append("PRIMARY KEY ASC ");

                    if(i < table.Columns.Count -1)
                        createSql.Append(",");

                    if (!string.IsNullOrEmpty(col.Description))
                        createSql.Append(" -- " + col.Description);

                    createSql.AppendLine();
                }

                createSql.AppendLine(")");

                DbCommand command = connection.CreateCommand();
                command.CommandText =createSql.ToString();
                try
                {
                    await command.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    connectionResult.Value.Close();
                    return new ReturnValue(false, "The following error occurred when attempting to create the table " + table.TableName + ".  " + ex.Message, ex);
                }

                connectionResult.Value.Close();

                return new ReturnValue(true, "", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "An error occurred creating the table " + table.TableName + ".  " + ex.Message, ex);
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
                default:
                    return "";
            }
        }

        public virtual string AggregateFunction(SelectColumn column)
        {
            switch (column.Aggregate)
            {
                case SelectColumn.EAggregate.None: return AddDelimiter(column.Column);
                case SelectColumn.EAggregate.Sum: return "sum(" + AddDelimiter(column.Column) + ")";
                case SelectColumn.EAggregate.Average: return "avg(" + AddDelimiter(column.Column) + ")";
                case SelectColumn.EAggregate.Min: return "min(" + AddDelimiter(column.Column) + ")";
                case SelectColumn.EAggregate.Max: return "max(" + AddDelimiter(column.Column) + ")";
                case SelectColumn.EAggregate.Count: return "count(" + AddDelimiter(column.Column) + ")";
            }

            return ""; //not possible to get here.
        }

        private string BuildSelectQuery(Table table, SelectQuery query)
        {
            StringBuilder sql = new StringBuilder();

            //if the query doesn't have any columns, then use all columns from the table.
            string columns;
            if (query?.Columns?.Count > 0)
                columns = String.Join(",", query.Columns.Select(c => AggregateFunction(c)).ToArray());
            else
                columns = string.Join(",", table.Columns.Where(c=>c.DeltaType != TableColumn.EDeltaType.IgnoreField).Select(c => AddDelimiter(c.ColumnName)).ToArray());

            sql.Append("select ");
            sql.Append(columns + " ");
            sql.Append("from " + AddDelimiter(table.TableName) + " ");
            sql.Append(" " + SqlSelectNoLock + " ");

            if (query?.Filters != null)
                sql.Append(BuildFiltersString(query.Filters));

            if (query?.Groups?.Count > 0)
            {
                sql.Append("group by ");
                sql.Append(String.Join(",", query.Groups.Select(c => AddDelimiter(c)).ToArray()));
            }
            if (query?.Sorts?.Count > 0)
            {
                sql.Append("order by ");
                sql.Append(String.Join(",", query.Sorts.Select(c => AddDelimiter(c.Column) + " " + (c.Direction == Sort.EDirection.Descending ? " desc" : "")).ToArray()));
            }

            return sql.ToString();
        }

        private string BuildFiltersString(List<Filter> filters)
        {
            if (filters == null || filters.Count == 0)
                return "";
            else
            {

                StringBuilder sql = new StringBuilder();
                sql.Append("where ");

                foreach (var filter in filters)
                {
                    if (filter.Column1 != null)
                        sql.Append(" " + AddDelimiter(filter.Column1) + " ");
                    else
                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value1) + " ");

                    sql.Append(GetSqlCompare(filter.Operator));

                    if (filter.Column2 != null)
                        sql.Append(" " + AddDelimiter(filter.Column2) + " ");
                    else
                        sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, filter.Value2) + " ");

                    sql.Append(filter.AndOr.ToString());
                }

                sql.Remove(sql.Length - 3, 3); //remove last or/and

                return sql.ToString();
            }
        }

        public override async Task<ReturnValue<int>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
            }

            StringBuilder sql = new StringBuilder();

            int rows = 0;

            using (var transaction = connection.Value.BeginTransaction())
            {
                foreach (var query in queries)
                {
                    sql.Clear();

                    sql.Append("update " + AddDelimiter(table.TableName) + " set ");

                    int count = 0;
                    foreach (QueryColumn column in query.UpdateColumns)
                    {
                        sql.Append(AddDelimiter(column.Column) + " = " + GetSqlFieldValueQuote(column.ColumnType, column.Value) + ",");
                        count++;
                    }
                    sql.Remove(sql.Length - 1, 1); //remove last comma
                    sql.Append(" " + BuildFiltersString(query.Filters) + ";");

                    //  Retrieving schema for columns from a single table
                    DbCommand cmd = connection.Value.CreateCommand();

                    //for (int i = 0; i < query.UpdateColumns.Count; i++)
                    //{
                    //    DbParameter param = cmd.CreateParameter();
                    //    param.ParameterName = "@col" + i.ToString();
                    //    param.Value = query.UpdateColumns[i].Value;
                    //    cmd.Parameters.Add(param);
                    //}

                    cmd.Transaction = transaction;
                    cmd.CommandText = sql.ToString();

                    try
                    {
                        rows += await cmd.ExecuteNonQueryAsync(cancelToken);

                        if (cancelToken.IsCancellationRequested)
                        {
                            connection.Value.Close();
                            return new ReturnValue<int>(false, "Update rows cancelled.", null);
                        }
                    }
                    catch (Exception ex)
                    {
                        connection.Value.Close();
                        return new ReturnValue<int>(false, "The update query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, -1);
                    }
                }
                transaction.Commit();
            }

            connection.Value.Close();
            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
        }

        public override async Task<ReturnValue<int>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
            }

            StringBuilder sql = new StringBuilder();
            int rows = 0;

            using (var transaction = connection.Value.BeginTransaction())
            {
                foreach (var query in queries)
                {
                    sql.Clear();
                    sql.Append("delete from " + AddDelimiter(table.TableName) + " ");
                    sql.Append(BuildFiltersString(query.Filters));


                    DbCommand cmd = connection.Value.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = sql.ToString();

                    try
                    {
                        rows += await cmd.ExecuteNonQueryAsync(cancelToken);

                        if (cancelToken.IsCancellationRequested)
                        {
                            connection.Value.Close();
                            return new ReturnValue<int>(false, "Delete rows cancelled.", null);
                        }
                    }
                    catch (Exception ex)
                    {
                        connection.Value.Close();
                        return new ReturnValue<int>(false, "The delete query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + sql.ToString(), ex, -1);
                    }
                }
                transaction.Commit();
            }

            connection.Value.Close();
            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
        }

        public override async Task<ReturnValue<int>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<int>(connection.Success, connection.Message, connection.Exception, -1);
            }

            StringBuilder insert = new StringBuilder();
            StringBuilder values = new StringBuilder();
            int rows = 0;

            using (var transaction = connection.Value.BeginTransaction())
            {
                foreach (var query in queries)
                {
                    insert.Clear();
                    values.Clear();

                    insert.Append("INSERT INTO " + AddDelimiter(table.TableName) + " (");
                    values.Append("VALUES (");

                    for (int i = 0; i < query.InsertColumns.Count; i++)
                    {
                        insert.Append("[" + query.InsertColumns[i].Column + "],");
                        values.Append("@col" + i.ToString() + ",");
                    }

                    string insertCommand = insert.Remove(insert.Length - 1, 1).ToString() + ") " + values.Remove(values.Length - 1, 1).ToString() + ");";

                    try
                    {
                        using (var cmd = connection.Value.CreateCommand())
                        {
                            cmd.CommandText = insertCommand;
                            cmd.Transaction = transaction;

                            for (int i = 0; i < query.InsertColumns.Count; i++)
                            {
                                var param = cmd.CreateParameter();
                                param.ParameterName = "@col" + i.ToString();
                                param.Value = query.InsertColumns[i].Value;
                                cmd.Parameters.Add(param);
                            }
                            rows += await cmd.ExecuteNonQueryAsync(cancelToken);

                            if (cancelToken.IsCancellationRequested)
                            {
                                connection.Value.Close();
                                return new ReturnValue<int>(false, "Insert rows cancelled.", null);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        connection.Value.Close();
                        return new ReturnValue<int>(false, "The insert query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  The sql command was " + insertCommand?.ToString(), ex, -1);
                    }
                }
                transaction.Commit();
            }

            connection.Value.Close();
            return new ReturnValue<int>(true, "", null, rows == -1 ? 0 : rows); //sometimes reader returns -1, when we want this to be error condition.
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return new ReturnValue<object>(connection);
            }

            string sql = BuildSelectQuery(table, query);

            //  Retrieving schema for columns from a single table
            DbCommand cmd = connection.Value.CreateCommand();
            cmd.CommandText = sql;

            object value;
            try
            {
                value = await cmd.ExecuteScalarAsync(cancelToken);
                connection.Value.Close();

                if (cancelToken.IsCancellationRequested)
                    return new ReturnValue<object>(false, "Execute scalar cancelled.", null);

                var returnValue = DataType.TryParse(table.Columns[query.Columns[0].Column].DataType, value);
                if (!returnValue.Success)
                    return new ReturnValue<object>(returnValue);
                return new ReturnValue<object>(true, returnValue.Value);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object>(false, "The select query for " + table.TableName + " could not be run due to the following error: " + ex.Message + ".  Sql command was: " + sql, ex, null);
            }

        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            ReturnValue<DbConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return connection;
            }

            DbCommand cmd = connection.Value.CreateCommand();
            cmd.CommandText = "delete from " + AddDelimiter(table.TableName);

            try
            {
                await cmd.ExecuteNonQueryAsync(cancelToken);
                connection.Value.Close();

                if (cancelToken.IsCancellationRequested)
                    return new ReturnValue<int>(false, "Truncate cancelled.", null);
            }
            catch (Exception ex)
            {
                connection.Value.Close();
                return new ReturnValue(false, "The truncate table query for " + table.TableName + " could not be run due to the following error: " + ex.Message, ex);
            }

            return new ReturnValue(true, "", null);
        }

        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override async Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, SelectQuery query = null)
        {
            try
            {
                var connection = await NewConnection();
                if (connection.Success == false)
                {
                    return new ReturnValue<DbDataReader>(connection);
                }

                DbCommand cmd = connection.Value.CreateCommand();
                cmd.CommandText = BuildSelectQuery(table, query);

                DbDataReader reader;

                try
                {
                    reader = await cmd.ExecuteReaderAsync();
                }
                catch (Exception ex)
                {
                    return new ReturnValue<DbDataReader>(false, "The connection reader for the sqlserver table " + table.TableName + " could failed due to the following error: " + ex.Message + ".  The sql command was: " + cmd.CommandText, ex);
                }

                if (reader == null)
                {
                    return new ReturnValue<DbDataReader>(false, "The connection reader for the sqlserver table " + table.TableName + " return null for an unknown reason.", null);
                }
                else
                {
                    return new ReturnValue<DbDataReader>(true, reader);
                }
            }
            catch(Exception ex)
            {
                return new ReturnValue<DbDataReader>(false, "The following error was encountered starting the connection reader for the sqlserver table " + table.TableName, ex);
            }
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null)
        {
            var reader = new ReaderSQL(this,table);
            return reader;
        }


    }
}
