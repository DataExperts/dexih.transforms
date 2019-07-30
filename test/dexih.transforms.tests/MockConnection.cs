using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.tests
{
    public class MockConnection: Connection
    {
        public override bool CanBulkLoad { get; } = true;
        public override bool CanSort { get; } = true;
        public override bool CanFilter { get; } = true;
        public override bool CanUpdate { get; } = true;
        public override bool CanDelete { get; } = true;
        public override bool CanAggregate { get; } = true;
        public override bool CanUseBinary { get; } = true;
        public override bool CanUseArray { get; } = true;
        public override bool CanUseJson { get; } = true;
        public override bool CanUseCharArray { get; } = true;
        public override bool CanUseXml { get; } = true;
        public override bool CanUseDbAutoIncrement { get; } = true;
        public override bool CanUseSql { get; } = true;
        public override bool DynamicTableCreation { get; } = true;

        
        // properties that can be used to validate connection is being called correctly;
        public SelectQuery SelectQuery { get; set; }
        public Table Table { get; set; }
        
        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
            Table = table;
            SelectQuery = query;
            return Task.FromResult((object)1);
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            Table = table;
            var reader = new MockReader(table);
            return reader;

        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query,
            CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<Table> GetSourceTableInfo(Table table, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            throw new System.NotImplementedException();
        }
    }
}