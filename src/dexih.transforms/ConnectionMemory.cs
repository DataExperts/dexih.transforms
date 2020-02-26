using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    public class ConnectionMemory : Connection
    {
        private readonly Dictionary<string, Table> _tables = new Dictionary<string, Table>();

        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanGroup => false;
        public override bool CanUseBinary => true;
        public override bool CanUseArray => true;
        public override bool CanUseJson => true;
        public override bool CanUseXml => true;
        public override bool CanUseCharArray => true;
        public override bool CanUseSql => false;
        public override bool CanUseDbAutoIncrement => false;
        public override bool DynamicTableCreation => false;

        public override Task<Table> InitializeTable(Table table, int position) => Task.FromResult(table);

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default) => Task.FromResult(true);

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
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

        public override Task ExecuteDelete(Table table, List<DeleteQuery> deleteQueries, int transactionReference, CancellationToken cancellationToken = default)
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

        public override async Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken = default)
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
                insertTable.AddRow(row);
            }
        }


        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            var insertTable = _tables[table.Name];

            long maxIncrement = 0;
            var autoIncrementOrdinal = -1;
            var autoIncrement = table.GetColumn(EDeltaType.DbAutoIncrement);
            if(autoIncrement != null)
            {
                autoIncrementOrdinal = table.GetOrdinal(autoIncrement.Name);
                foreach(var row in insertTable.Data)
                {
                    var value = Operations.Parse<long>(row[autoIncrementOrdinal]);
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

                insertTable.AddRow(row);
            }

            return Task.FromResult(maxIncrement);
        }

        public TableCache GetTableData(Table table)
        {
            var returnTable = _tables[table.Name];
            return returnTable?.Data;
        }

        public Table GetTable(string tableName)
        {
            if (_tables.TryGetValue(tableName, out var getTable))
            {
                return getTable;
            }
            
            throw new ConnectionException($"The table {tableName} does not exist in the memory connection.");
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            var reader = new ReaderMemory(_tables[table.Name], null);
            return Task.FromResult<DbDataReader>(reader);
        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
            var t = GetTable(table.Name);
            using var reader = new TransformQuery(new ReaderMemory(t), query);
            await reader.Open(cancellationToken);
            var hasRow = await reader.ReadAsync(cancellationToken);

            object value;
            if (!hasRow)
            {
                value = null;
            }
            else if (query.Columns.Count == 0)
            {
                value = reader[0];
            }
            else
            {
                value = reader[query.Columns[0].Column.Name];
            }
            reader.Close();

            return value;
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> updateQueries, int transactionReference, CancellationToken cancellationToken = default)
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

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new List<string>() { "" });
        }

        public override Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken = default)
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

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_tables.Values.ToList());
        }

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            _tables[table.Name].Data.Clear();
            return Task.CompletedTask;
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            // var reader = new ReaderMemory(table);
            var reader = new ReaderMemory(_tables[table.Name], null);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_tables.ContainsKey(table.Name));
        }
    }
}
