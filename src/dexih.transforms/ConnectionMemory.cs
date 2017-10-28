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
using Dexih.Utils.DataType;

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

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken) => Task.FromResult(true);

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
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

        public override Task ExecuteDelete(Table table, List<DeleteQuery> deleteQueries, CancellationToken cancellationToken)
        {
            var deleteTable = _tables[table.Name];

            foreach (var query in deleteQueries)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The execute delete for table {table.Name} was cancelled.", cancellationToken);
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

			return Task.CompletedTask;
        }

        public override async Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken)
        {
            var insertTable = _tables[table.Name];

            while(await sourceData.ReadAsync(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The bulk insert for table {table.Name} was cancelled.", cancellationToken);
                }

                var row = new object[sourceData.FieldCount];
                sourceData.GetValues(row);
                insertTable.Data.Add(row);
            }
        }


        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
            var insertTable = _tables[table.Name];

            long maxIncrement = 0;
            var autoIncrementOrdinal = -1;
            var autoIncrement = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
            if(autoIncrement != null)
            {
                autoIncrementOrdinal = table.GetOrdinal(autoIncrement.Name);
                foreach(var row in insertTable.Data)
                {
                    var value = (long)DataType.TryParse(DataType.ETypeCode.Int64, row[autoIncrementOrdinal]);
                    if(value > maxIncrement)
                    {
                        maxIncrement = value;
                    }
                }
            }


            foreach (var query in queries)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The insert rows for table {table.Name} was cancelled.", cancellationToken);
                }

                var row = new object[table.Columns.Count];
                foreach(var item in query.InsertColumns)
                {
                    var ordinal = table.Columns.GetOrdinal(item.Column.Name);
                    row[ordinal] = item.Value;
                }

                if(autoIncrement != null)
                {
                    row[autoIncrementOrdinal] = ++maxIncrement;
                }

                insertTable.Data.Add(row);
            }

            return Task.FromResult<long>(maxIncrement);
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
            var reader = new ReaderMemory(_tables[table.Name], null);
            return Task.FromResult<DbDataReader>(reader);
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken)
        {
            //TODO Implement ExecuteScalar in ConnectionMemory.

            throw new NotImplementedException();
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> updateQueries, CancellationToken cancellationToken)
        {
            var updateTable = _tables[table.Name];

            foreach (var query in updateQueries)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException($"The update rows for table {table.Name} was cancelled.", cancellationToken);
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

			return Task.CompletedTask;
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
        {
            return Task.FromResult(new List<string>() { "" });
        }

        public override Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken)
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

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken)
        {
            return Task.FromResult(_tables.Values.ToList());
        }

        public override Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            _tables[table.Name].Data.Clear();
            return Task.CompletedTask;
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderMemory(table);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken)
        {
            return Task.FromResult(_tables.ContainsKey(table.Name));
        }
    }
}
