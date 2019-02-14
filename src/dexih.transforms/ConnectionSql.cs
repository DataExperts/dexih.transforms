using System;
using System.Collections.Generic;
using System.Data;
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
using Dexih.Utils.DataType;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.sql
{
    public abstract class ConnectionSql : Connection
    {
        public override bool CanBulkLoad => true;
        public override bool CanSort => true;
        public override bool CanFilter => true;
        public override bool CanDelete => true;
        public override bool CanUpdate => true;
        public override bool CanAggregate => true;
        public override bool CanUseBinary => true;
        public override bool CanUseArray => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseJson => false;
        public override bool CanUseXml => false;
        public override bool CanUseSql => true;
        public override bool CanUseAutoIncrement => false;
        public override bool DynamicTableCreation => false;


        //These properties can be overridden for different databases
        protected virtual string SqlDelimiterOpen { get; } = "\"";

        protected virtual string SqlDelimiterClose { get; } = "\"";
        public virtual string SqlValueOpen { get; } = "'";
        public virtual string SqlValueClose { get; } = "'";
        protected virtual string SqlFromAttribute(Table table) => "";

        protected virtual char SqlParameterIdentifier => '@';

        protected virtual long MaxSqlSize { get; set; } = 4000000; // the largest size the sql command can be (default 4mb)


        protected string AddDelimiter(string name)
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
            if (!string.IsNullOrEmpty(table.Schema))
            {
                return AddDelimiter(table.Schema) + "." + AddDelimiter(table.Name);
            }

            return AddDelimiter(table.Name);
        }

         protected string AddEscape(string value) => value.Replace("'", "''");


        public abstract Task<DbConnection> NewConnection();

        protected abstract string GetSqlType(TableColumn column);
        // protected abstract string GetSqlFieldValueQuote(ETypeCode typeCode, int rank, object value);


        protected virtual DbCommand CreateCommand(DbConnection connection, string commandText, DbTransaction transaction = null)
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

                    var columns = table.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.AutoIncrement).ToArray();
                    var ordinals = new int[columns.Length];
                    
                    for(var i = 0; i< columns.Length; i++)
                    {
                        insert.Append(AddDelimiter(columns[i].Name) + ",");
                        values.Append($"{SqlParameterIdentifier}col{i},");
                        ordinals[i] = reader.GetOrdinal(columns[i].Name);
                    }

                    var insertCommand = insert.Remove(insert.Length - 1, 1) + ") " + values.Remove(values.Length - 1, 1) + ") ";

                    using (var transaction = connection.BeginTransaction())
                    {
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.CommandText = insertCommand;
                            cmd.Transaction = transaction;

                            var parameters = new DbParameter[fieldCount];

                            for (var i = 0; i < columns.Count(); i++)
                            {
                                var param = cmd.CreateParameter();
                                param.ParameterName = $"{SqlParameterIdentifier}col{i}";
                                cmd.Parameters.Add(param);
                                parameters[i] = param;
                            }


                            while (await reader.ReadAsync(cancellationToken))
                            {
                                for (var i = 0; i < columns.Count(); i++)
                                {
                                    parameters[i].Value = reader[ordinals[i]];
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
                case Filter.ECompare.IsNull: return "is null";
                case Filter.ECompare.IsNotNull: return "is not null";
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

        private string BuildSelectQuery(Table table, SelectQuery query, DbCommand cmd)
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
            {
                var filters = new List<Filter>();
                if (query?.Filters != null && query.Filters.Any() )
                {
                    filters.AddRange(query.Filters);
                }

                var inputColumns = table.Columns.Where(c => c.IsInput && !c.DefaultValue.ObjectIsNullOrBlank()).ToArray();
                if (inputColumns.Any())
                {
                    foreach (var inputColumn in inputColumns)
                    {
                        var filter = new Filter(inputColumn, Filter.ECompare.IsEqual, inputColumn.DefaultValue);
                        filters.Add(filter);
                    }
                    sql.Append(BuildFiltersString(filters, cmd));
                }

                sql.Append(BuildFiltersString(query.Filters, cmd));
            }


            if (query?.Groups?.Count > 0)
            {
                sql.Append(" group by ");
                sql.Append(string.Join(",", query.Groups.Select(c => AddDelimiter(c.Name)).ToArray()));
            }
            if (query?.Sorts?.Count > 0)
            {
                sql.Append(" order by ");
                sql.Append(string.Join(",", query.Sorts.Select(c => AddDelimiter(c.Column.Name) + " " + (c.Direction == Sort.EDirection.Descending ? " desc" : "")).ToArray()));
            }

            return sql.ToString();
        }

        protected string BuildFiltersString(List<Filter> filters, DbCommand cmd)
        {
            if (filters == null || filters.Count == 0)
                return "";
            
            var sql = new StringBuilder();
            sql.Append(" where ");

            var index = 0;
            foreach (var filter in filters)
            {
                index++;
                
                if (filter.Value1 == null && filter.Column1 == null)
                {
                    throw new ConnectionException("The filter has no values or columns specified for the primary value.");
                }

                if (filter.Column1 != null)
                {
                    if (filter.Column1.IsInput)
                    {
                        // sql.Append(" " + GetSqlFieldValueQuote(filter.Column1.DataType, filter.Column1.Rank, filter.Column1.DefaultValue) + " ");
                        var param = cmd.CreateParameter();
                        param.ParameterName = $"{SqlParameterIdentifier}Filter{index}Column1Default";
                        // param.DbType = GetDbType(filter.Column1.DataType);
                        param.Direction = ParameterDirection.Input;
                        param.Value = filter.Column1.DefaultValue;
                        cmd.Parameters.Add(param);
                        sql.Append($" {param.ParameterName} ");
                    }
                    else
                    {
                        sql.Append(" " + AddDelimiter(filter.Column1.Name) + " ");    
                    }
                }
                else
                {
                    // sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, 0, filter.Value1) + " ");
                    var param = cmd.CreateParameter();
                    param.ParameterName = $"{SqlParameterIdentifier}Filter{index}Value1";
                    // param.DbType = GetDbType(filter.BestDataType());
                    param.Direction = ParameterDirection.Input;
                    param.Value = ConvertForWrite(filter.BestDataType(), 0, true, filter.Value1);
                    cmd.Parameters.Add(param);
                    sql.Append($" {param.ParameterName} ");
                }

                sql.Append(GetSqlCompare(filter.Operator));

                if (filter.Operator != Filter.ECompare.IsNull && filter.Operator != Filter.ECompare.IsNotNull)
                {
                    if (filter.Value2 == null && filter.Column2 == null)
                    {
                        throw new ConnectionException("The filter has no values or columns specified for the compare value.  Use the IsNull operation to compare to null rows.");
                    }

                    if (filter.Column2 != null)
                        if (filter.Column2.IsInput)
                        {
                            // sql.Append(" " + GetSqlFieldValueQuote(filter.Column2.DataType, filter.Column2.Rank, filter.Column2.DefaultValue) + " ");
                            var param = cmd.CreateParameter();
                            param.ParameterName = $"{SqlParameterIdentifier}Filter{index}Column2Default";
                            // param.DbType = GetDbType(filter.Column2.DataType);
                            param.Direction = ParameterDirection.Input;
                            param.Value = ConvertForWrite(filter.BestDataType(), 0, true, filter.Column2.DefaultValue);
                            cmd.Parameters.Add(param);
                            sql.Append($" {param.ParameterName} ");
                        }
                        else
                        {
                            sql.Append(" " + AddDelimiter(filter.Column2.Name) + " ");    
                        }
                    else
                    {
                        if (filter.Value2 != null && filter.Value2.GetType().IsArray)
                        {
                            var array = new List<string>();
                            foreach (var value in (Array) filter.Value2)
                                array.Add(value.ToString());
                                var index1 = index;
                                sql.Append(" (" + string.Join(",",
                                           array.Select((c, arrayIndex) =>
                                           {
                                               // return GetSqlFieldValueQuote(filter.CompareDataType, 0, c);
                                               var param = cmd.CreateParameter();
                                               param.Direction = ParameterDirection.Input;
                                               // param.DbType = GetDbType(filter.BestDataType());
                                               param.Value = ConvertForWrite(filter.BestDataType(), 0, true, c);
                                               param.ParameterName = $"{SqlParameterIdentifier}Filter{index1}ArrayValue{arrayIndex}";
                                               cmd.Parameters.Add(param);

                                               return $"{param.ParameterName}";
                                           })) +
                                       ") ");
                        }
                        else
                        {
                            // sql.Append(" " + GetSqlFieldValueQuote(filter.CompareDataType, 0, filter.Value2) + " ");
                            var param = cmd.CreateParameter();
                            param.ParameterName = $"{SqlParameterIdentifier}Filter{index}Value2";
                            // param.DbType  = GetDbType(filter.BestDataType());
                            param.Direction = ParameterDirection.Input;
                            param.Value = ConvertForWrite(filter.BestDataType(), 0, true, filter.Value2);
                            cmd.Parameters.Add(param);
                            sql.Append($" {param.ParameterName} ");
                        }
                    }
                }

                sql.Append(filter.AndOr.ToString());
            }

            sql.Remove(sql.Length - 3, 3); //remove last or/and

            return sql.ToString();
        }
        
        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                long identityValue = 0;

                using (var connection = await NewConnection())
                {
                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var query in queries)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            insert.Clear();
                            values.Clear();

                            insert.Append("INSERT INTO " + AddDelimiter(table.Name) + " (");
                            values.Append("VALUES (");

                            for (var i = 0; i < query.InsertColumns.Count; i++)
                            {
                                insert.Append(AddDelimiter(query.InsertColumns[i].Column.Name) + ",");
                                values.Append($"{SqlParameterIdentifier}col{i},");
                            }

                            var insertCommand = insert.Remove(insert.Length - 1, 1) + ") " +
                                                values.Remove(values.Length - 1, 1) + "); ";

                            try
                            {
                                using (var cmd = connection.CreateCommand())
                                {
                                    cmd.CommandText = insertCommand;
                                    cmd.Transaction = transaction;

                                    for (var i = 0; i < query.InsertColumns.Count; i++)
                                    {
                                        var param = cmd.CreateParameter();
                                        param.ParameterName = $"{SqlParameterIdentifier}col{i}";
                                        param.Value = ConvertForWrite(query.InsertColumns[i].Column, query.InsertColumns[i].Value);
                                        // param.DbType = GetDbType(query.InsertColumns[i].Column.DataType);
                                        cmd.Parameters.Add(param);
                                    }

                                    await cmd.ExecuteNonQueryAsync(cancellationToken);

                                }
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The insert query failed.  {ex.Message}", ex);
                            }
                        }
                        
                        var deltaColumn = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                        if (deltaColumn != null)
                        {
                            var autoIncrementSql = $" select max({AddDelimiter(deltaColumn.Name)}) from {AddDelimiter(table.Name)}";
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = autoIncrementSql;
                                cmd.Transaction = transaction;
                                var identity = await cmd.ExecuteScalarAsync(cancellationToken);
                                identityValue = Convert.ToInt64(identity);
                            }
                        }

                        transaction.Commit();
                    }

                    return identityValue; //sometimes reader returns -1, when we want this to be error condition.
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Insert into table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                using (var connection = await NewConnection())
                {

                    var sql = new StringBuilder();

                    using (var transaction = connection.BeginTransaction())
                    {
                        foreach (var query in queries)
                        {
                            sql.Clear();

                            sql.Append("update " + SqlTableName(table) + " set ");

                            var count = 0;
                            foreach (var column in query.UpdateColumns)
                            {
                                sql.Append(AddDelimiter(column.Column.Name) + $" = {SqlParameterIdentifier}col{count},");
                                count++;
                            }
                            sql.Remove(sql.Length - 1, 1); //remove last comma

                            //  Retrieving schema for columns from a single table
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.Transaction = transaction;

                                var parameters = new DbParameter[query.UpdateColumns.Count];
                                for (var i = 0; i < query.UpdateColumns.Count; i++)
                                {
                                    var param = cmd.CreateParameter();
                                    param.ParameterName = $"{SqlParameterIdentifier}col{i}";
                                    // param.DbType = GetDbType(query.UpdateColumns[i].Column.DataType);
                                    // param.Size = -1;

                                    var value = ConvertForWrite(query.UpdateColumns[i].Column, query.UpdateColumns[i].Value);
                                    param.Value = value;

                                    // replaced with ConvertParameterType above.
                                    //// GUID's get parameterized as binary.  So need to explicitly convert to string.
                                    //if (query.UpdateColumns[i].Column.DataType == ETypeCode.Guid)
                                    //{
                                    //    param.Value = query.UpdateColumns[i].Value == null ? (object)DBNull.Value : query.UpdateColumns[i].Value.ToString();
                                    //}
                                    //else
                                    //{
                                    //    param.Value = query.UpdateColumns[i].Value == null ? DBNull.Value
                                    //        : query.UpdateColumns[i].Value;
                                    //}

                                    cmd.Parameters.Add(param);
                                    parameters[i] = param;
                                }

                                sql.Append(" " + BuildFiltersString(query.Filters, cmd) );
                                cmd.CommandText = sql.ToString();

                                cancellationToken.ThrowIfCancellationRequested();

                                try
                                {
                                    await cmd.ExecuteNonQueryAsync(cancellationToken);
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

                            cancellationToken.ThrowIfCancellationRequested();

                            using (var cmd = connection.CreateCommand())
                            {
                                sql.Append(BuildFiltersString(query.Filters, cmd));

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

                    //  Retrieving schema for columns from a single table
                    using (var cmd = connection.CreateCommand())
                    {
                        var sql = BuildSelectQuery(table, query, cmd);
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
                            return Operations.Parse(table.Columns[query.Columns[0].Column].DataType, value);
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

                    var readerOpen = await reader.ReadAsync(cancellationToken); 

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        // use the type of the value (which is better for arrays) unless it's null, then use the GetFieldType.
                        var type = !readerOpen || reader[i] is null ? reader.GetFieldType(i) : reader[i].GetType();
                        var col = new TableColumn
                        {
                            Name = reader.GetName(i),
                            LogicalName = reader.GetName(i),
                            DataType = GetTypeCode(type, out var rank),
                            Rank = rank,
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
                cmd.CommandText = table.UseQuery ? table.QueryString : BuildSelectQuery(table, query, cmd);
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
