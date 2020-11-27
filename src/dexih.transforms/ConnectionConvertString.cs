using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;

namespace dexih.transforms
{
    /// <summary>
    /// This is a dummy connection that is used to convert arrays and other values to strings.
    /// </summary>
    public class ConnectionConvertString : Connection
    {

        public override bool CanBulkLoad => throw new NotImplementedException();

        public override bool CanSort => throw new NotImplementedException();

        public override bool CanFilter => throw new NotImplementedException();

        public override bool CanJoin => false;
        public override bool CanUpdate => throw new NotImplementedException();

        public override bool CanDelete => throw new NotImplementedException();

        public override bool CanGroup => throw new NotImplementedException();

        public override bool CanUseDateTimeOffset => false;

        public override bool CanUseBinary => false;

        public override bool CanUseArray => false;

        public override bool CanUseJson => false;

        public override bool CanUseCharArray => false;

        public override bool CanUseXml => false;

        public override bool CanUseSql => throw new NotImplementedException();

        public override bool CanUseDbAutoIncrement => false;
        
        public override bool DynamicTableCreation => throw new NotImplementedException();

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Table> GetSourceTableInfo(Table table, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            throw new NotImplementedException();
        }

        public override Task<Table> InitializeTable(Table table, int position, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}
