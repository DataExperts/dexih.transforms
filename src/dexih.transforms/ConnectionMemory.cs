using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using System.Diagnostics;
using dexih.transforms.Exceptions;
using dexih.functions.Query;

namespace dexih.transforms
{
    public class ConnectionMemory : Connection
    {
        private readonly Dictionary<string, Table> _tables = new Dictionary<string, Table>();

        public override bool AllowNtAuth => false;

        public override bool AllowUserPass => false;

        public override bool CanBulkLoad => false;

        public override ECategory DatabaseCategory => ECategory.NoSqlDatabase;

        public override string DatabaseTypeName => "In Memory";

        public override string DefaultDatabaseHelp => "";

        public override string ServerHelp => "";

        public override bool CanSort => false;

        public override bool CanFilter => false;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanAggregate => false;
        public override bool CanUseBinary => true;
        public override bool CanUseSql => false;
        public override bool DynamicTableCreation => false;

        public override Task<Table> InitializeTable(Table table, int position) => Task.FromResult<Table>(null);

        public override Task CreateDatabase(string databaseName, CancellationToken cancelToken) => Task.FromResult(true);

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            if (_tables.ContainsKey(table.Name))
            {
                if (dropTable)
                    _tables[table.Name] = table;
                else
                    throw new ConnectionException($"The table {table.Name} already exists on {Name}.");
            }
            else
            {
                _tables.Add(table.Name, table);
            }

            return Task.CompletedTask;
        }

        public override Task<long> ExecuteDelete(Table table, List<DeleteQuery> deleteQueries, CancellationToken cancelToken)
        {
            var deleteTable = _tables[table.Name];

            var timer = Stopwatch.StartNew();

            foreach (var query in deleteQueries)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The execute delete for table {table.Name} was cancelled.", cancelToken);
                }

                var lookupResult = deleteTable.LookupMultipleRows(query.Filters);

                if (lookupResult != null)
                {
                    foreach (var row in lookupResult)
                    {
                        deleteTable.Data.Remove(row);
                    }
                }
            }
            timer.Stop();

            return Task.FromResult(timer.ElapsedTicks);
        }

        public override async Task<long> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            var insertTable = _tables[table.Name];

            var timer = Stopwatch.StartNew();
            while(await sourceData.ReadAsync(cancelToken))
            {
                if (cancelToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The bulk insert for table {table.Name} was cancelled.", cancelToken);
                }

                var row = new object[sourceData.FieldCount];
                sourceData.GetValues(row);
                insertTable.Data.Add(row);
            }
            timer.Stop();
            return timer.ElapsedTicks;
        }


        public override Task<Tuple<long, long>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            var timer = Stopwatch.StartNew();
            var insertTable = _tables[table.Name];

            foreach (var query in queries)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The insert rows for table {table.Name} was cancelled.", cancelToken);
                }

                var row = new object[table.Columns.Count];
                foreach(var item in query.InsertColumns)
                {
                    var ordinal = table.Columns.GetOrdinal(item.Column.Name);
                    row[ordinal] = item.Value;
                }

                insertTable.Data.Add(row);
            }

            timer.Stop();
            return Task.FromResult(Tuple.Create<long, long>(0, timer.ElapsedTicks));
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            var reader = new ReaderMemory(_tables[table.Name], null);
            return Task.FromResult<DbDataReader>(reader);
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            //TODO Implement ExecuteScalar in ConnectionMemory.

            throw new NotImplementedException();
        }

        public override Task<long> ExecuteUpdate(Table table, List<UpdateQuery> updateQueries, CancellationToken cancelToken)
        {
            var updateTable = _tables[table.Name];

            var timer = Stopwatch.StartNew();

            foreach (var query in updateQueries)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The update rows for table {table.Name} was cancelled.", cancelToken);
                }

                var lookupResult = updateTable.LookupMultipleRows(query.Filters);
                if (lookupResult != null)
                {
                    foreach (var row in lookupResult)
                    {
                        foreach (var updateColumn in query.UpdateColumns)
                        {
                            var ordinal = updateTable.GetOrdinal(updateColumn.Column.TableColumnName());
                            row[ordinal] = updateColumn.Value;
                        }
                    }
                }
            }

            timer.Stop();
            return Task.FromResult<long>(timer.ElapsedTicks);
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancelToken)
        {
            return Task.FromResult(new List<string>() { "" });
        }

        public override Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            if (_tables.Keys.Contains(originalTable.Name))
            {
                return Task.FromResult(_tables[originalTable.Name]);
            }
            else
            {
                return Task.FromResult<Table>(null);

            }
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancelToken)
        {
            return Task.FromResult(_tables.Values.ToList());
        }

        public override Task TruncateTable(Table table, CancellationToken cancelToken)
        {
            _tables[table.Name].Data.Clear();
            return Task.CompletedTask;
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderMemory(table);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancelToken)
        {
            return Task.FromResult(_tables.ContainsKey(table.Name));
        }
    }
}
