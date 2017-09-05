using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using System.Diagnostics;

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

        public override Task<ReturnValue<Table>> InitializeTable(Table table, int position) => Task.Run(() => new ReturnValue<Table>(true, table));

        public override Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken) => Task.Run(() => new ReturnValue(true));

        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            return await Task.Run(() =>
            {
                if (_tables.ContainsKey(table.Name))
                {
                    if (dropTable)
                        _tables[table.Name] = table;
                    else
                        return new ReturnValue(false, "The table " + table.Name + " already exists.", null);
                }
                else
                {
                    _tables.Add(table.Name, table);
                }

                return new ReturnValue(true);
            });
        }

        public override async Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> deleteQueries, CancellationToken cancelToken)
        {
            return await Task.Run(() =>
            {
                var deleteTable = _tables[table.Name];

                var timer = Stopwatch.StartNew();

                foreach (var query in deleteQueries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Insert rows cancelled.", null);

                    var lookupResult = deleteTable.LookupMultipleRows(query.Filters);
                    if (lookupResult.Success == false)
                        return new ReturnValue<long>(lookupResult);

                    foreach (var row in lookupResult.Value)
                    {
                        deleteTable.Data.Remove(row);
                    }
                }
                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }, cancelToken);
        }

        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            var insertTable = _tables[table.Name];

            var timer = Stopwatch.StartNew();
            while(await sourceData.ReadAsync(cancelToken))
            {
                if (cancelToken.IsCancellationRequested)
                    return new ReturnValue<long>(false, "Insert rows cancelled.", null);

                var row = new object[sourceData.FieldCount];
                sourceData.GetValues(row);
                insertTable.Data.Add(row);
            }
            timer.Stop();
            return new ReturnValue<long>(true, timer.ElapsedTicks);
        }


        public override async Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            return await Task.Run(() => {
                var timer = Stopwatch.StartNew();
                var insertTable = _tables[table.Name];

                foreach (var query in queries)
                {
                    var row = new object[table.Columns.Count];
                    foreach(var item in query.InsertColumns)
                    {
                        var ordinal = table.Columns.GetOrdinal(item.Column.Name);
                        row[ordinal] = item.Value;
                    }

                    insertTable.Data.Add(row);
                }

                timer.Stop();
                return new ReturnValue<Tuple<long, long>>(true, Tuple.Create<long, long>(0, timer.ElapsedTicks));
            }, cancelToken);
        }

        public override async Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
           return await Task.Run(() =>
          {
              var reader = new ReaderMemory(_tables[table.Name], null);
              return new ReturnValue<DbDataReader>(true, reader);
          }, cancelToken);
        }

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> updateQueries, CancellationToken cancelToken)
        {
            return await Task.Run(() =>
            {
                var updateTable = _tables[table.Name];

                var timer = Stopwatch.StartNew();

                foreach (var query in updateQueries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Update cancelled", null);

                    var lookupResult = updateTable.LookupMultipleRows(query.Filters);
                    if (lookupResult.Success == false)
                        return new ReturnValue<long>(lookupResult);

                    foreach (var row in lookupResult.Value)
                    {
                        foreach (var updateColumn in query.UpdateColumns)
                        {
                            var ordinal = updateTable.GetOrdinal(updateColumn.Column.SchemaColumnName());
                            row[ordinal] = updateColumn.Value;
                        }
                    }
                }

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }, cancelToken);
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue<List<string>>(true, new List<string>() { "" } ), cancelToken);
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            return await Task.Run( () => new ReturnValue<Table>(true, _tables[originalTable.Name]), cancelToken);
        }

        public override async Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue<List<Table>>(true, _tables.Values.ToList()), cancelToken);
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
           return await Task.Run(() =>
           {
               _tables[table.Name].Data.Clear();
               return new ReturnValue(true);
           }, cancelToken);

        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderMemory(table);
            return reader;
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue<bool>(true, _tables.ContainsKey(table.Name)), cancelToken);
        }
    }
}
