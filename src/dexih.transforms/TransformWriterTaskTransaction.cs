using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    public class TransformWriterTaskTransaction: TransformWriterTask
    {
        private int _transactionReference = -1;
        
        public override async Task<int> StartTransaction(int transactionReference = -1)
        {
            if (transactionReference > -1)
            {
                _transactionReference = transactionReference;
                return transactionReference;
            }
            
            _transactionReference = await TargetConnection.StartTransaction();
            return _transactionReference;
        }

        public override void CommitTransaction()
        {
            if (_transactionReference == -1)
            {
                throw new TransformWriterException("There is no current transaction to be committed.");
            }

            TargetConnection.CommitTransaction(_transactionReference);
            _transactionReference = -1;
        }

        public override void RollbackTransaction()
        {
            if (_transactionReference == -1)
            {
                throw new TransformWriterException("There is no current transaction to be committed.");
            }

            TargetConnection.RollbackTransaction(_transactionReference);
            _transactionReference = -1;
        }

        public override async Task<long> AddRecord(char operation, object[] row, CancellationToken cancellationToken = default)
        {
            var surrogateKey = 0L;
            switch (operation)
            {
                case 'C':
                    var queryColumns = new List<QueryColumn>();
                    for (var i = 0; i < TargetTable.Columns.Count; i++)
                    {
                        var col = TargetTable.Columns[i];
                        if (col.DeltaType != TableColumn.EDeltaType.DbAutoIncrement)
                        {
                            queryColumns.Add(new QueryColumn(col, row[i]));
                        }
                    }

                    var insertQuery = new InsertQuery(queryColumns);
                    surrogateKey = await TargetConnection.ExecuteInsert(TargetTable,
                        new List<InsertQuery>() {insertQuery}, _transactionReference, cancellationToken);
                    break;
                case 'R':
                    var rejectQueryColumns = new List<QueryColumn>();
                    for (var i = 0; i < TargetTable.Columns.Count; i++)
                    {
                        var col = TargetTable.Columns[i];
                        if (col.DeltaType != TableColumn.EDeltaType.DbAutoIncrement)
                        {
                            rejectQueryColumns.Add(new QueryColumn(col, row[i]));
                        }
                    }

                    var rejectQuery = new InsertQuery(rejectQueryColumns);
                    surrogateKey = await RejectConnection.ExecuteInsert(RejectTable,
                        new List<InsertQuery>() {rejectQuery}, cancellationToken);
                    break;
                    
                case 'U':
                    var updateQuery = new UpdateQuery(
                        TargetTable.Columns.Where(c => !c.IsAutoIncrement())
                            .Select((c, index) => new QueryColumn(c, row[index])).ToList(),
                        TargetTable.Columns.Where(c => c.IsAutoIncrement())
                            .Select((c, index) => new Filter(c, ECompare.IsEqual, row[index])).ToList()
                    );
                    await TargetConnection.ExecuteUpdate(TargetTable, new List<UpdateQuery>() {updateQuery}, _transactionReference, 
                        cancellationToken = default);
                    break;

                case 'D':
                    var deleteQuery = new DeleteQuery(
                        TargetTable.Name,
                        TargetTable.Columns.Where(c => c.IsAutoIncrement())
                            .Select((c, index) => new Filter(c, ECompare.IsEqual, row[index])).ToList()
                    );
                    await TargetConnection.ExecuteDelete(TargetTable, new List<DeleteQuery>() {deleteQuery}, _transactionReference,
                        cancellationToken = default);
                    break;
                case 'T':
                    if (!TargetConnection.DynamicTableCreation && !TruncateComplete)
                    {
                        await TargetConnection.TruncateTable(TargetTable, _transactionReference, cancellationToken);
                        TruncateComplete = true;
                    }

                    break;
            }

            return surrogateKey;
        }

        public override Task FinalizeWrites(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}