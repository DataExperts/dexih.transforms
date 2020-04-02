using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        public override bool CanGroup => true;
        public override bool CanDelete => true;
        public override bool CanUpdate => true;
        public override bool CanUseBinary => true;
        public override bool CanUseArray => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseJson => false;
        public override bool CanUseXml => false;
        public override bool CanUseSql => true;
        public override bool CanUseDbAutoIncrement => true;
        public override bool CanUseTransaction => true;
        public override bool DynamicTableCreation => false;


        //These properties can be overridden for different databases
        protected virtual string SqlDelimiterOpen { get; } = "\"";

        protected virtual string SqlDelimiterClose { get; } = "\"";
        public virtual string SqlValueOpen { get; } = "'";
        public virtual string SqlValueClose { get; } = "'";
        protected virtual string SqlFromAttribute(Table table) => "";

        public virtual bool AllowsTruncate { get; } = false;

        protected virtual char SqlParameterIdentifier => '@';

        protected virtual long MaxSqlSize { get; set; } = 4000000; // the largest size the sql command can be (default 4mb)


        protected string AddDelimiter(string name)
        {
            // if it ends with ), then it's a function like max(abc), so do not delimit
            if (name.EndsWith(")"))
            {
                return name;
            }
            
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

        private int _currentTransactionKey = 0;
        private readonly ConcurrentDictionary<int, (DbConnection connection, DbTransaction transaction)> _transactions = new ConcurrentDictionary<int, (DbConnection, DbTransaction)>();

         protected string AddEscape(string value) => value.Replace("'", "''");


        public abstract Task<DbConnection> NewConnection();

        protected abstract string GetSqlType(TableColumn column);
        // protected abstract string GetSqlFieldValueQuote(ETypeCode typeCode, int rank, object value);


        protected virtual DbCommand CreateCommand(DbConnection connection, string commandText, DbTransaction transaction = null)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = commandText;
            cmd.CommandTimeout = CommandTimeout;
            cmd.Transaction = transaction;

            return cmd;
        }

        //protected DbParameter CreateParameter(DbCommand cmd, string parameterName, object value)
        //{
        //    var param = cmd.CreateParameter();
        //    param.ParameterName = parameterName;
        //    param.Value = value;

        //    return param;
        //}

        public virtual DbParameter CreateParameter(DbCommand command, string name, ETypeCode type, int rank, ParameterDirection direction, object value)
        {
            var param = command.CreateParameter();
            param.ParameterName = name;
            param.Direction = direction;
            var converted = ConvertForWrite(param.ParameterName, type, rank, true, value);
            param.DbType = converted.typeCode.GetDbType();
            param.Value = converted.value;

            return param;
        }

        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
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

                    var columns = table.Columns.Where(c => c.DeltaType != EDeltaType.DbAutoIncrement).ToArray();
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

                            for (var i = 0; i < columns.Length; i++)
                            {
                                var param = CreateParameter(cmd, $"{SqlParameterIdentifier}col{i}", columns[i].DataType,
                                    columns[i].Rank, ParameterDirection.Input, null);
//                                var param = cmd.CreateParameter();
//                                param.ParameterName = $"{SqlParameterIdentifier}col{i}";
                                cmd.Parameters.Add(param);
                                parameters[i] = param;
                            }


                            while (await reader.ReadAsync(cancellationToken))
                            {
                                for (var i = 0; i < columns.Length; i++)
                                {
                                    // conversion done by reader now!!!
                                    //var column = columns[i];
                                    // var converted = ConvertForWrite(column.Name, column.DataType, column.Rank, true, reader[ordinals[i]]);
                                    // parameters[i].Value = converted.value;

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

        public override async Task<long> RowCount(Table table, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"select count(*) from {SqlTableName(table)} ";
                    var count = await cmd.ExecuteScalarAsync(cancellationToken);
                    return Convert.ToInt64(count);
                }

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The count failed to for table {table.Name} on {Name}", ex);
            }
        }

        public virtual async Task<bool> DropTable(Table table)
        {
            try
            {
                using (var connection = await NewConnection())
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"drop table {SqlTableName(table)}";
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
        public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
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
                    createSql.Append($"create table {SqlTableName(table)}");

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

                        if (col.IsAutoIncrement())
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


        public string GetSqlCompare(ECompare compare)
        {
            switch (compare)
            {
                case ECompare.IsEqual: return "=";
                case ECompare.GreaterThan: return ">";
                case ECompare.GreaterThanEqual: return ">=";
                case ECompare.LessThan: return "<";
                case ECompare.LessThanEqual: return "<=";
                case ECompare.NotEqual: return "!=";
                case ECompare.IsIn: return "IN";
                case ECompare.IsNull: return "is null";
                case ECompare.IsNotNull: return "is not null";
                case ECompare.Like: return "like";
                default:
                    return "";
            }
        }

        protected string AggregateFunction(SelectColumn column)
        {
            var selectColumn = AddDelimiter(column.Column.Name);

            if (column.Aggregate == EAggregate.None)
            {
                return selectColumn;
            }
            
            var agg = column.Aggregate switch
            {
                EAggregate.Sum => "sum",
                EAggregate.Average => "sum",
                EAggregate.Min => "min",
                EAggregate.Max => "max",
                EAggregate.Count => "count",
                _ => throw new NotSupportedException()
            };

            return $"{agg}({selectColumn})";
        }

        public override async Task<int> StartTransaction()
        {
            var key = Interlocked.Increment(ref _currentTransactionKey);
            var connection = await NewConnection();
            var transaction = connection.BeginTransaction();
            if(!_transactions.TryAdd(key, (connection, transaction) ))
            {
                throw new ConnectionException("Failed to start the transaction.");
            }
            return key;
        }

        public override void CommitTransaction(int transactionReference)
        {
            if (!_transactions.TryRemove(transactionReference, out var transaction))
            {
                throw new ConnectionException("Failed to commit the transaction.");
            }
            transaction.transaction.Commit();
            transaction.transaction.Dispose();
            transaction.connection.Close();
            transaction.connection.Dispose();
        }

        public override void RollbackTransaction(int transactionReference)
        {
            if (!_transactions.TryRemove(transactionReference, out var transaction))
            {
                throw new ConnectionException("Failed to commit the transaction.");
            }
            transaction.transaction.Rollback(); 
            transaction.transaction.Dispose();
            transaction.connection.Close();
            transaction.connection.Dispose();
        }

        public virtual string GetJoinType(EJoinType joinType)
        {
            switch (joinType)
            {
                case EJoinType.Inner:
                    return "inner";
                case EJoinType.Left:
                    return "left";
                case EJoinType.Right:
                    return "right";
                case EJoinType.Full:
                    return "full";
                default:
                    throw new ArgumentOutOfRangeException(nameof(joinType), joinType, null);
            }
        }
        
        public override bool IsFilterSupported(Filter filter)
        {
            //TODO add support for array columns
            if ((filter.Column1?.IsArray() ?? false) || (filter.Column2?.IsArray() ?? false))
            {
                return false;
            }

            return true;
        }

        private string BuildSelectQuery(Table table, SelectQuery query, DbCommand cmd)
        {
            var sql = new StringBuilder();

            //if the query doesn't have any columns, then use all columns from the table.
            Dictionary<string, string> columns;
            if (query?.Columns?.Count > 0)
            {
                columns = query.Columns.ToDictionary(c => c.GetOutputName(), AggregateFunction);
            }
            else
            {
                columns = table.Columns.Where(c => c.DeltaType != EDeltaType.IgnoreField)
                    .ToDictionary(c => c.Name, c => c.Name);
            }

            sql.Append("select ");
            sql.Append(string.Join(",", columns.Select(c => AddDelimiter(c.Value) + " " + AddDelimiter(c.Key))) + " ");
            sql.Append("from " + SqlTableName(table) + " ");
            sql.Append(" " + SqlFromAttribute(table) + " ");
            
            var filters = new Filters();
            if (query?.Filters != null && query.Filters.Any() )
            {
                filters.AddRange(query.Filters);
            }

            // set default value on any input columns.
            var inputColumns = table.Columns.Where(c => c.IsInput && !c.DefaultValue.ObjectIsNullOrBlank()).ToArray();
            foreach (var inputColumn in inputColumns)
            {
                var filter = new Filter(inputColumn, ECompare.IsEqual, inputColumn.DefaultValue);
                filters.Add(filter);
            }

            sql.Append(BuildFiltersString(filters, cmd));

            if (query?.Groups?.Count > 0)
            {
                sql.Append(" group by ");
                sql.Append(string.Join(",", query.Groups.Select(c => AddDelimiter(c.Name)).ToArray()));
            }

            if (query?.GroupFilters?.Count > 0)
            {
                var groupFilters = query.GroupFilters.Select(c => new Filter()
                {
                    Column1 = c.Column1 == null ? null : new TableColumn(columns[c.Column1.Name], c.Column1.DataType, c.Column1.DeltaType, c.Column1.Rank),
                    Column2 = c.Column2 == null ? null :new TableColumn(columns[c.Column2.Name], c.Column2.DataType, c.Column2.DeltaType, c.Column2.Rank),
                    Value1 =  c.Value1,
                    Value2 = c.Value2,
                    Operator = c.Operator,
                    CompareDataType = c.CompareDataType
                }).ToList();

                sql.Append(BuildFiltersString(groupFilters, cmd, "having"));
            }

            if (query?.Sorts?.Count > 0)
            {
                sql.Append(" order by ");
                sql.Append(string.Join(",", query.Sorts.Select(c => AddDelimiter(c.Column.Name) + " " + (c.Direction == ESortDirection.Descending ? " desc" : "")).ToArray()));
            }

            return sql.ToString();
        }
        
        protected string BuildFiltersString(List<Filter> filters, DbCommand cmd, string prefix = "where")
        {
            if (filters == null || filters.Count == 0)
                return "";
            
            var sql = new StringBuilder();
            sql.Append(" " + prefix + " ");

            var index = 0;
            foreach (var filter in filters)
            {
                if (!IsFilterSupported(filter))
                {
                    continue;
                }
                
                index++;
                
                if (filter.Value1 == null && filter.Column1 == null)
                {
                    throw new ConnectionException("The filter has no values or columns specified for the primary value.");
                }

                if (filter.Column1 != null)
                {
                    if (filter.Column1.IsInput && filter.Value2 == null)
                    {
                        var parameterName = $"{SqlParameterIdentifier}{prefix}{index}Column1Default";
                        if (cmd != null)
                        {
                            var param = CreateParameter(
                                cmd, parameterName, 
                                filter.Column1.DataType, 
                                filter.Column1.Rank,
                                ParameterDirection.Input,
                                filter.Column1.DefaultValue);

                            //var param = cmd.CreateParameter();
                            //param.ParameterName = parameterName;
                            //param.DbType = GetDbType(filter.Column1.DataType);
                            //param.Direction = ParameterDirection.Input;
                            //param.Value = filter.Column1.DefaultValue;
                            cmd.Parameters.Add(param);
                        }

                        sql.Append($" {parameterName} ");
                    }
                    else
                    {
                        sql.Append(" " + AddDelimiter(filter.Column1.Name) + " ");    
                    }
                }
                else
                {
                    var parameterName = $"{SqlParameterIdentifier}{prefix}{index}Value1";
                    if (cmd != null)
                    {
                        var param = CreateParameter(cmd, parameterName, filter.BestDataType(), 0, ParameterDirection.Input,
                            filter.Value1);

                        //var param = cmd.CreateParameter();
                        //param.ParameterName = parameterName;
                        //param.DbType = GetDbType(filter.BestDataType());
                        //param.Direction = ParameterDirection.Input;
                        //param.Value = ConvertForWrite(param.ParameterName, filter.BestDataType(), 0, true,
                        //    filter.Value1);
                        cmd.Parameters.Add(param);
                    }

                    sql.Append($" {parameterName} ");
                }

                sql.Append(GetSqlCompare(filter.Operator) + " ");

                if (filter.Operator != ECompare.IsNull && filter.Operator != ECompare.IsNotNull)
                {
                    if (filter.Column2 != null)
                        if (filter.Column2.IsInput)
                        {
                            var parameterName = $"{SqlParameterIdentifier}{prefix}{index}Column2Default";
                            if (cmd != null)
                            {
                                var param = CreateParameter(cmd, parameterName, filter.Column2.DataType, filter.Column2.Rank, ParameterDirection.Input,
                                    filter.Column2.DefaultValue);

                                //var param = cmd.CreateParameter();
                                //param.ParameterName = parameterName;
                                //param.DbType = GetDbType(filter.Column2.DataType);
                                //param.Direction = ParameterDirection.Input;
                                //param.Value = ConvertForWrite(param.ParameterName, filter.BestDataType(), 0, true,
                                //    filter.Column2.DefaultValue);
                                cmd.Parameters.Add(param);
                                sql.Append($" {parameterName} ");
                            }
                        }
                        else
                        {
                            sql.Append(" " + AddDelimiter(filter.Column2.Name) + " ");    
                        }
                    else
                    {
                        if (filter.Value2 != null && filter.Value2.GetType().IsArray)
                        {
                            var array = (from object value in (Array) filter.Value2 select value.ToString()).ToList();

                            var index1 = index;
                            sql.Append(" (" + string.Join(",", array.Select((c, arrayIndex) =>
                                       {
                                           var parameterName = $"{SqlParameterIdentifier}{prefix}{index1}ArrayValue{arrayIndex}";
                                           if (cmd != null)
                                           {
                                               var param = CreateParameter(cmd, parameterName, filter.BestDataType(),0, ParameterDirection.Input, c);

                                               //var param = cmd.CreateParameter();
                                               //param.Direction = ParameterDirection.Input;
                                               //param.DbType = GetDbType(filter.BestDataType());
                                               //param.Value = ConvertForWrite(param.ParameterName,
                                               //    filter.BestDataType(), 0, true, c);
                                               //param.ParameterName = parameterName;
                                               cmd.Parameters.Add(param);
                                           }

                                           return $"{parameterName}";
                                       })) +
                                   ") ");
                        }
                        else
                        {
                            var parameterName = $"{SqlParameterIdentifier}{prefix}{index}Value2";
                            if (cmd != null)
                            {
                                var param = CreateParameter(cmd, parameterName, filter.BestDataType(), 0, ParameterDirection.Input,
                                    filter.Value2);
                                //var param = cmd.CreateParameter();
                                //param.ParameterName = parameterName;
                                //param.Direction = ParameterDirection.Input;
                                //param.DbType = GetDbType(filter.BestDataType());
                                //param.Value = ConvertForWrite(param.ParameterName, filter.BestDataType(), 0, true,
                                //    filter.Value2);
                                cmd.Parameters.Add(param);
                            }

                            sql.Append($" {parameterName} ");
                        }
                    }
                }

                switch (filter.AndOr)
                {
                    case EAndOr.And:
                        sql.Append("and");
                        break;
                    case EAndOr.Or:
                        sql.Append("or");
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            sql.Remove(sql.Length - 3, 3); //remove last or/and

            return sql.ToString();
        }

 
        
        /// <summary>
        /// Either gets an active transaction, or creates a new transaction
        /// </summary>
        /// <param name="transactionReference"></param>
        /// <returns></returns>
        /// <exception cref="ConnectionException"></exception>
        protected async Task<(DbConnection connection, DbTransaction transaction)> GetTransaction(int transactionReference)
        {
            if (transactionReference > 0)
            {
                if (!_transactions.TryGetValue(transactionReference, out var connectionTransaction))
                {
                    throw new ConnectionException("Failed to get the transaction.");
                }

                return connectionTransaction;
            }
            else
            {
                var connection = await NewConnection();
                var transaction = connection.BeginTransaction();
                return (connection, transaction);
            }
        }

        /// <summary>
        /// Ends the transaction if it is not active
        /// </summary>
        /// <param name="transactionReference"></param>
        /// <param name="transaction"></param>
        protected void EndTransaction(int transactionReference, (DbConnection connection, DbTransaction transaction) transaction)
        {
            if (transactionReference <= 0)
            {
                transaction.transaction?.Commit();
                transaction.transaction?.Dispose();
                transaction.connection?.Close();
                transaction.connection?.Dispose();
            }
        }
        
        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                long identityValue = 0;

                var transaction = await GetTransaction(transactionReference);

                try
                {

                    var insert = new StringBuilder();
                    var values = new StringBuilder();

                    foreach (var query in queries)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        insert.Clear();
                        values.Clear();

                        insert.Append("INSERT INTO " + SqlTableName(table) + " (");
                        values.Append("VALUES (");

                        for (var i = 0; i < query.InsertColumns.Count; i++)
                        {
                            if (query.InsertColumns[i].Column.DeltaType == EDeltaType.DbAutoIncrement)
                                continue;

                            if (query.InsertColumns[i].Column.DeltaType == EDeltaType.AutoIncrement)
                                identityValue = Convert.ToInt64(query.InsertColumns[i].Value);

                            insert.Append(AddDelimiter(query.InsertColumns[i].Column.Name) + ",");
                            values.Append($"{SqlParameterIdentifier}col{i},");
                        }

                        var insertCommand = insert.Remove(insert.Length - 1, 1) + ") " +
                                            values.Remove(values.Length - 1, 1) + "); ";

                        try
                        {
                            using (var cmd = transaction.connection.CreateCommand())
                            {
                                cmd.CommandText = insertCommand;
                                cmd.Transaction = transaction.transaction;

                                for (var i = 0; i < query.InsertColumns.Count; i++)
                                {
                                    var param = CreateParameter(cmd, $"{SqlParameterIdentifier}col{i}",
                                        query.InsertColumns[i].Column.DataType, query.InsertColumns[i].Column.Rank, ParameterDirection.Input,
                                         query.InsertColumns[i].Value);
                                    
                                    //    query.InsertColumns[i].Value) )
                                    //var param = cmd.CreateParameter();
                                    //param.ParameterName = $"{SqlParameterIdentifier}col{i}";
                                    //param.Value = ConvertForWrite(query.InsertColumns[i].Column,
                                    //    query.InsertColumns[i].Value);
                                    //// param.DbType = GetDbType(query.InsertColumns[i].Column.DataType);
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

                    var deltaColumn = table.GetColumn(EDeltaType.DbAutoIncrement);

                    if (deltaColumn != null)
                    {
                        var autoIncrementSql =
                            $" select max({AddDelimiter(deltaColumn.Name)}) from {SqlTableName(table)}";
                        using (var cmd = transaction.connection.CreateCommand())
                        {
                            cmd.CommandText = autoIncrementSql;
                            cmd.Transaction = transaction.transaction;
                            var identity = await cmd.ExecuteScalarAsync(cancellationToken);
                            identityValue = Convert.ToInt64(identity);
                        }
                    }
                }
                finally
                {
                    EndTransaction(transactionReference, transaction);
                }

                return identityValue; //sometimes reader returns -1, when we want this to be error condition.

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert into table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override async Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var transaction = await GetTransaction(transactionReference);
                try
                {

                    var sql = new StringBuilder();
                    foreach (var query in queries)
                    {
                        sql.Clear();

                        sql.Append("update " + SqlTableName(table) + " set ");

                        var count = 0;
                        foreach (var column in query.UpdateColumns)
                        {
                            sql.Append(AddDelimiter(column.Column.Name) +
                                       $" = {SqlParameterIdentifier}col{count},");
                            count++;
                        }

                        sql.Remove(sql.Length - 1, 1); //remove last comma

                        //  Retrieving schema for columns from a single table
                        using (var cmd = transaction.connection.CreateCommand())
                        {
                            cmd.Transaction = transaction.transaction;

                            var parameters = new DbParameter[query.UpdateColumns.Count];
                            for (var i = 0; i < query.UpdateColumns.Count; i++)
                            {
                                var param = CreateParameter(cmd, $"{SqlParameterIdentifier}col{i}",
                                    query.UpdateColumns[i].Column.DataType,  
                                    query.UpdateColumns[i].Column.Rank, 
                                    ParameterDirection.Input, 
                                    query.UpdateColumns[i].Value);

                                //var param = cmd.CreateParameter();
                                //param.ParameterName = $"{SqlParameterIdentifier}col{i}";
                                //// param.DbType = GetDbType(query.UpdateColumns[i].Column.DataType);
                                //// param.Size = -1;

                                //var value = ConvertForWrite(query.UpdateColumns[i].Column,
                                //    query.UpdateColumns[i].Value);
                                //param.Value = value;

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

                            sql.Append(BuildFiltersString(query.Filters, cmd));

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
                }
                finally
                {
                    EndTransaction(transactionReference, transaction);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Update table {table.Name} failed.  {ex.Message}", ex);
            }
        }

        public override async Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var transaction = await GetTransaction(transactionReference);
                try
                {
                    var sql = new StringBuilder();

                    foreach (var query in queries)
                    {
                        sql.Clear();
                        sql.Append("delete from " + SqlTableName(table) + " ");

                        cancellationToken.ThrowIfCancellationRequested();

                        using (var cmd = transaction.connection.CreateCommand())
                        {
                            sql.Append(BuildFiltersString(query.Filters, cmd));

                            cmd.Transaction = transaction.transaction;
                            cmd.CommandText = sql.ToString();

                            try
                            {
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException($"The delete query failed. {ex.Message}", ex);
                            }
                        }
                    }
                }
                finally
                {
                    EndTransaction(transactionReference, transaction);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Delete rows from table {table.Name} failed.  {ex.Message}", ex);
            }

        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
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

        public override async Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
                var transaction = await GetTransaction(transactionReference);
                
                try
                {
                    var cmd = transaction.connection.CreateCommand();
                    cmd.Transaction = transaction.transaction;
                    
                    // if there is no transaction, then use truncate
                    if (transactionReference <= 0 && AllowsTruncate)
                    {
                        cmd.CommandText = "truncate table " + SqlTableName(table);

                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                        }
                        catch (Exception)
                        {
                            if(transaction.transaction.Connection == null)
                            {
                                transaction = await GetTransaction(transactionReference);
                            }
                            cmd = transaction.connection.CreateCommand();
                            cmd.Transaction = transaction.transaction;
                            cmd.CommandText = "delete from " + SqlTableName(table);

                            try
                            {
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                            catch (Exception ex2)
                            {
                                throw new ConnectionException($"Truncate and delete query failed. {ex2.Message}", ex2);
                            }
                        }
                    }
                    else
                    {
                        cmd.CommandText = "delete from " + SqlTableName(table);
                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                        }
                        catch (Exception ex2)
                        {
                            throw new ConnectionException($"Delete query failed. {ex2.Message}", ex2);
                        }
                    }
                }
                finally
                {
                    EndTransaction(transactionReference, transaction);
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Truncate table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task<Table> InitializeTable(Table table, int position, CancellationToken cancellationToken)
        {
            return Task.FromResult(table);
        }

        public async Task<Table> GetQueryTable(Table table, CancellationToken cancellationToken = default)
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
                        var type = !readerOpen || reader.IsDBNull(i) ? reader.GetFieldType(i) : reader[i].GetType();
                        var col = new TableColumn
                        {
                            Name = reader.GetName(i),
                            LogicalName = reader.GetName(i),
                            DataType = GetTypeCode(type, out var rank),
                            Rank = rank,
                            DeltaType = EDeltaType.TrackingField
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

        public override async Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            try
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = table.TableType == Table.ETableType.Query ? table.QueryString : BuildSelectQuery(table, query, cmd);
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

        public override string GetDatabaseQuery(Table table, SelectQuery query)
        {
            return table.TableType == Table.ETableType.Query ? table.QueryString : BuildSelectQuery(table, query, null);
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderSql(this, table);
            return reader;
        }

    }
}
