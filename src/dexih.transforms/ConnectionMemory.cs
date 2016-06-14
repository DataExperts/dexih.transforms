using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public class ConnectionMemory : Connection
    {
        Dictionary<string, Table> Tables = new Dictionary<string, Table>();

        public override bool AllowNtAuth => false;

        public override bool AllowUserPass => false;

        public override bool CanBulkLoad => false;

        public override ECategory DatabaseCategory => ECategory.NoSqlDatabase;

        public override string DatabaseTypeName => "In Memory";

        public override string DefaultDatabaseHelp => "";

        public override string ServerHelp => "";

        public override Task<ReturnValue> AddMandatoryColumns(Table table, int position) => Task.Run(() => new ReturnValue(true));

        public override Task<ReturnValue> CreateDatabase(string DatabaseName) => Task.Run(() => new ReturnValue(true));

        public override async Task<ReturnValue> CreateManagedTable(Table table, bool dropTable = false)
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

        public override async Task<ReturnValue<int>> ExecuteDelete(Table table, List<DeleteQuery> deleteQueries)
        {
            return await Task.Run(() =>
            {
                var deleteTable = Tables[table.TableName];

                int count = 0;
                foreach (DeleteQuery query in deleteQueries)
                {
                    var lookupResult = deleteTable.LookupMultipleRows(query.Filters);
                    if (lookupResult.Success == false)
                        return new ReturnValue<int>(lookupResult);

                    foreach (object[] row in lookupResult.Value)
                    {
                        count++;
                        deleteTable.Data.Remove(row);
                    }
                }
                return new ReturnValue<int>(true, count);
            });
        }

        public override async Task<ReturnValue<int>> ExecuteInsertBulk(Table table, DbDataReader sourceData)
        {
            return await Task.Run(() =>
            {
                Table insertTable = Tables[table.TableName];

                int count = 0;
                while(sourceData.Read())
                {
                    object[] row = new object[sourceData.FieldCount];
                    sourceData.GetValues(row);
                    insertTable.Data.Add(row);
                    count++;
                }
                return new ReturnValue<int>(true, count);
            });
        }


        public override async Task<ReturnValue<int>> ExecuteInsert(Table table, List<InsertQuery> queries)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<Transform>> ExecuteReader(Table table, SelectQuery query = null)
        {
           return await Task.Run(() =>
          {
              var reader = new ReaderMemory(Tables[table.TableName], null);
              return new ReturnValue<Transform>(true, reader);
          });
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<int>> ExecuteUpdate(Table table, List<UpdateQuery> updateQueries)
        {
            return await Task.Run(() =>
            {
                var updateTable = Tables[table.TableName];

                int count = 0;
                foreach (UpdateQuery query in updateQueries)
                {
                    var lookupResult = updateTable.LookupMultipleRows(query.Filters);
                    if (lookupResult.Success == false)
                        return new ReturnValue<int>(lookupResult);

                    foreach (object[] row in lookupResult.Value)
                    {
                        count++;
                        foreach (var updateColumn in query.UpdateColumns)
                        {
                            int ordinal = updateTable.GetOrdinal(updateColumn.Column);
                            row[ordinal] = updateColumn.Value;
                        }
                    }
                }
                return new ReturnValue<int>(true, count);
            });
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            return await Task.Run(() => new ReturnValue<List<string>>(true, new List<string>() { "" } ));
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties)
        {
            return await Task.Run( () => new ReturnValue<Table>(true, Tables[tableName]));
        }

        public override async Task<ReturnValue<List<string>>> GetTableList()
        {
            return await Task.Run(() =>
            {
                return new ReturnValue<List<string>>(true, Tables.Keys.ToList());
            });
        }

        public override async Task<ReturnValue> TruncateTable(Table table)
        {
           return await Task.Run(() =>
           {
               Tables[table.TableName].Data.Clear();
               return new ReturnValue(true);
           });

        }
    }
}
