using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms
{
    public class TransformWriterTaskTransaction: TransformWriterTask
    {
        public override async Task<long> AddRecord(char operation, object[] row, CancellationToken cancellationToken)
        {
            var surrogateKey = 0L;
            switch (operation)
            {
                case 'C':
                case 'R':
                    var queryColumns = new List<QueryColumn>();
                    for (var i = 0; i < TargetTable.Columns.Count; i++)
                    {
                        var col = TargetTable.Columns[i];
                        if (!col.IsGeneratedColumn())
                        {
                            queryColumns.Add(new QueryColumn(col, row[i]));
                        }
                    }

                    var insertQuery = new InsertQuery(TargetTable.Name, queryColumns);
                    surrogateKey = await TargetConnection.ExecuteInsert(TargetTable,
                        new List<InsertQuery>() {insertQuery}, cancellationToken);
                    break;
                case 'U':
                    var updateQuery = new UpdateQuery(
                        TargetTable.Name,
                        TargetTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.AutoIncrement)
                            .Select((c, index) => new QueryColumn(c, row[index])).ToList(),
                        TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                            .Select((c, index) => new Filter(c, Filter.ECompare.IsEqual, row[index])).ToList()
                    );
                    await TargetConnection.ExecuteUpdate(TargetTable, new List<UpdateQuery>() {updateQuery},
                        cancellationToken);
                    break;

                case 'D':
                    var deleteQuery = new DeleteQuery(
                        TargetTable.Name,
                        TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.AutoIncrement)
                            .Select((c, index) => new Filter(c, Filter.ECompare.IsEqual, row[index])).ToList()
                    );
                    await TargetConnection.ExecuteDelete(TargetTable, new List<DeleteQuery>() {deleteQuery},
                        cancellationToken);
                    break;
            }

            return surrogateKey;
        }

        public override Task FinalizeRecords(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}