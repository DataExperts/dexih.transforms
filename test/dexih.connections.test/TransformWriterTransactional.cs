using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.File;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.connections.test
{
    public class TransformWriterTransactional
    {
        private async Task<Transform> GetReader()
        {
            var stream = System.IO.File.OpenRead("Data/transactions.json");
            var table = new WebService();

            var handler = new FileHandlerJson(table, null);
            var columns = (await handler.GetSourceColumns(stream)).ToArray();
            table.Columns = new TableColumns(columns);
            
            stream = System.IO.File.OpenRead("Data/transactions.json");
            handler = new FileHandlerJson(table, null);

            await handler.SetStream(stream, null);

            var transform = new ReaderFileHandler(handler, table);
            await transform.Open();
            return transform;
        }
        
        public async Task Unit(Connection connection, string databaseName)
        {
            await connection.CreateDatabase(databaseName, CancellationToken.None);
            
            var transactionTable = new Table("transaction");
            transactionTable.Columns.Add(new TableColumn("id", DataType.ETypeCode.String, TableColumn.EDeltaType.TrackingField));
            transactionTable.Columns.Add(new TableColumn("desc", DataType.ETypeCode.String, TableColumn.EDeltaType.TrackingField));
            transactionTable.Columns.Add(new TableColumn("transaction_key", DataType.ETypeCode.Int64, TableColumn.EDeltaType.AutoIncrement));

            var componentTable = new Table("component");
            componentTable.Columns.Add(new TableColumn("item_id", DataType.ETypeCode.String, TableColumn.EDeltaType.TrackingField));
            componentTable.Columns.Add(new TableColumn("desc", DataType.ETypeCode.String, TableColumn.EDeltaType.TrackingField));
            componentTable.Columns.Add(new TableColumn("component_key", DataType.ETypeCode.Int64, TableColumn.EDeltaType.AutoIncrement));
            componentTable.Columns.Add(new TableColumn("transaction_key", DataType.ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));

            var reader = await GetReader();
            var writer = new TransformWriter();
            
            var writerResult = new TransformWriterResult();
            await connection.InitializeAudit(writerResult, 0, 1, "DataLink", 1, 2, "Test", 1, "Source", 2, "Target", TransformWriterResult.ETriggerMethod.Manual, "Test", CancellationToken.None);

            // add two target tables
            var targets = new TransformWriterTargets();
            targets.Add(new TransformWriterTarget(TransformWriterTarget.ETransformWriterMethod.Transaction,
                writerResult, connection, transactionTable, null, null)
            {
                ColumnPath =  null                    
            });
            
            targets.Add(new TransformWriterTarget(TransformWriterTarget.ETransformWriterMethod.Transaction, new TransformWriterResult(), connection, componentTable, null, null)
            {
                ColumnPath =  new [] { "items" },
            });
            
            var writeRecords = await writer.WriteRecordsAsync(reader, targets, CancellationToken.None);
            Assert.True(writeRecords, $"WriteAllRecords failed with message {writerResult.Message}.  Details:{writerResult.ExceptionDetails}");
            
            
        }
    }
}