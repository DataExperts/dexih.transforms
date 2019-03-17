using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.File;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.connections.test
{
    public class TransformWriterTarget
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
            // await transform.Open();
            return transform;
        }
        
        public async Task ParentChild_Write(Connection connection, string databaseName, bool useDbAutoIncrement, bool useTransaction)
        {
            var autoIncrement = useDbAutoIncrement
                ? TableColumn.EDeltaType.DbAutoIncrement
                : TableColumn.EDeltaType.AutoIncrement;

            var transactionType = useTransaction
                ? transforms.TransformWriterTarget.ETransformWriterMethod.Transaction
                : transforms.TransformWriterTarget.ETransformWriterMethod.Bulk;
                
            
            await connection.CreateDatabase(databaseName, CancellationToken.None);
            
            var transactionTable = new Table("transaction");
            transactionTable.Columns.Add(new TableColumn("id", DataType.ETypeCode.Int32, TableColumn.EDeltaType.TrackingField));
            transactionTable.Columns.Add(new TableColumn("desc", DataType.ETypeCode.String, TableColumn.EDeltaType.TrackingField));
            transactionTable.Columns.Add(new TableColumn("transaction_key", DataType.ETypeCode.Int64, autoIncrement));

            var componentTable = new Table("component");
            componentTable.Columns.Add(new TableColumn("item_id", DataType.ETypeCode.Int32, TableColumn.EDeltaType.TrackingField));
            componentTable.Columns.Add(new TableColumn("desc", DataType.ETypeCode.String, TableColumn.EDeltaType.TrackingField));
            componentTable.Columns.Add(new TableColumn("component_key", DataType.ETypeCode.Int64, autoIncrement));
            componentTable.Columns.Add(new TableColumn("transaction_key", DataType.ETypeCode.Int64, TableColumn.EDeltaType.TrackingField));

            var reader = await GetReader();
           
            // var writerResult = await connection.InitializeAudit(CancellationToken.None);

            // add two target tables
            var targets = new transforms.TransformWriterTarget(connection, transactionTable, transactionType);
            targets.Add(new transforms.TransformWriterTarget(connection, componentTable,transactionType, new TransformWriterResult()), new[] {"items"});
            
            await targets.WriteRecordsAsync(reader, CancellationToken.None);

            var transactionReader = connection.GetTransformReader(transactionTable);
            transactionReader = new TransformSort(transactionReader, "id");
            
            await transactionReader.Open();

            await transactionReader.ReadAsync();
            Assert.Equal(1, transactionReader["id"]);
            Assert.Equal("product 1", transactionReader["desc"]);
            Assert.Equal(1L, transactionReader["transaction_key"]);

            await transactionReader.ReadAsync();
            Assert.Equal(2, transactionReader["id"]);
            Assert.Equal("product 2", transactionReader["desc"]);
            Assert.Equal(2L, transactionReader["transaction_key"]);
            
            Assert.False(await transactionReader.ReadAsync());

            var componentReader = connection.GetTransformReader(componentTable);
            var sortQuery = new SelectQuery()
            {
                Sorts = new List<Sort>() {new Sort(componentTable["component_key"])}
            };
            await componentReader.Open(0, sortQuery, CancellationToken.None);

            var count = 0L;
            for (var a = 1L; a <= 2; a++)
            {
                for (var i = 1; i <= 3; i++)
                {
                    count++;
                    await componentReader.ReadAsync();
                    var componentId = (10 * a) + i;
                    Assert.Equal((int)componentId, componentReader["item_id"]);
                    Assert.Equal($"component {componentId}", componentReader["desc"]);
                    Assert.Equal(a, componentReader["transaction_key"]);
                    Assert.Equal(count, componentReader["component_key"]);
                }
            }
        }
    }
}