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
        private readonly Dictionary<string, Table> Tables = new Dictionary<string, Table>();

        public override bool AllowNtAuth => false;

        public override bool AllowUserPass => false;

        public override bool CanBulkLoad => false;

        public override ECategory DatabaseCategory => ECategory.NoSqlDatabase;

        public override string DatabaseTypeName => "In Memory";

        public override string DefaultDatabaseHelp => "";

        public override string ServerHelp => "";

        public override bool CanSort => false;

        public override bool CanFilter => false;
        public override bool CanAggregate => false;

        public override Task<ReturnValue> AddMandatoryColumns(Table table, int position) => Task.Run(() => new ReturnValue(true));

        public override Task<ReturnValue> CreateDatabase(string DatabaseName) => Task.Run(() => new ReturnValue(true));

        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable = false)
        {
            return await Task.Run(() =>
            {
                if (Tables.ContainsKey(table.TableName))
                {
                    if (dropTable)
                        Tables[table.TableName] = table;
                    else
                        return new ReturnValue(false, "The table " + table.TableName + " already exists.", null);
                }
                else
                {
                    Tables.Add(table.TableName, table);
                }

                return new ReturnValue(true);
            });
        }

        public override async Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> deleteQueries, CancellationToken cancelToken)
        {
            return await Task.Run(() =>
            {
                var deleteTable = Tables[table.TableName];

                int count = 0;
                var timer = Stopwatch.StartNew();

                foreach (DeleteQuery query in deleteQueries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Insert rows cancelled.", null);

                    var lookupResult = deleteTable.LookupMultipleRows(query.Filters);
                    if (lookupResult.Success == false)
                        return new ReturnValue<long>(lookupResult);

                    foreach (object[] row in lookupResult.Value)
                    {
                        count++;
                        deleteTable.Data.Remove(row);
                    }
                }
                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            });
        }

        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            Table insertTable = Tables[table.TableName];

            var timer = Stopwatch.StartNew();
            while(await sourceData.ReadAsync(cancelToken))
            {
                if (cancelToken.IsCancellationRequested)
                    return new ReturnValue<long>(false, "Insert rows cancelled.", null);

                object[] row = new object[sourceData.FieldCount];
                sourceData.GetValues(row);
                insertTable.Data.Add(row);
            }
            timer.Stop();
            return new ReturnValue<long>(true, timer.ElapsedTicks);
        }


        public async override Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            return await Task.Run(() => {
                var timer = Stopwatch.StartNew();
                Table insertTable = Tables[table.TableName];

                foreach (var query in queries)
                {
                    object[] row = new object[table.Columns.Count];
                    foreach(var item in query.InsertColumns)
                    {
                        var ordinal = table.Columns.GetOrdinal(item.Column.ColumnName);
                        row[ordinal] = item.Value;
                    }

                    insertTable.Data.Add(row);
                }

                timer.Stop();
                return new ReturnValue<Tuple<long, long>>(true, Tuple.Create<long, long>(0, timer.ElapsedTicks));
            });
        }

        public override async Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query = null)
        {
           return await Task.Run(() =>
          {
              var reader = new ReaderMemory(Tables[table.TableName], null);
              return new ReturnValue<DbDataReader>(true, reader);
          });
        }

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> updateQueries, CancellationToken cancelToken)
        {
            return await Task.Run((Func<ReturnValue<long>>)(() =>
            {
                var updateTable = Tables[table.TableName];

                int count = 0;
                var timer = Stopwatch.StartNew();

                foreach (UpdateQuery query in updateQueries)
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Update cancelled", null);

                    var lookupResult = updateTable.LookupMultipleRows(query.Filters);
                    if (lookupResult.Success == false)
                        return new ReturnValue<long>(lookupResult);

                    foreach (object[] row in lookupResult.Value)
                    {
                        count++;
                        foreach (var updateColumn in query.UpdateColumns)
                        {
                            int ordinal = updateTable.GetOrdinal((string)updateColumn.Column.SchemaColumnName());
                            row[ordinal] = updateColumn.Value;
                        }
                    }
                }

                timer.Stop();
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }));
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            return await Task.Run(() => new ReturnValue<List<string>>(true, new List<string>() { "" } ));
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable)
        {
            return await Task.Run( () => new ReturnValue<Table>(true, Tables[originalTable.TableName]));
        }

        public override async Task<ReturnValue<List<Table>>> GetTableList()
        {
            return await Task.Run(() =>
            {
				return new ReturnValue<List<Table>>(true, Tables.Values.ToList());
            });
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
           return await Task.Run(() =>
           {
               Tables[table.TableName].Data.Clear();
               return new ReturnValue(true);
           });

        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null)
        {
            var reader = new ReaderMemory(table);
            return reader;
        }

        public async override Task<ReturnValue<bool>> TableExists(Table table)
        {
            return await Task.Run(() =>
            {
                return new ReturnValue<bool>(true, Tables.ContainsKey(table.TableName));
            });
        }
    }
}
