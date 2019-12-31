using System;
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
using Xunit.Abstractions;

namespace dexih.connections.test
{
    public class TransformWriterTargetTests
    {
        private readonly ITestOutputHelper _output;

        public TransformWriterTargetTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        private async Task<Transform> GetReader()
        {
            var stream = System.IO.File.OpenRead("Data/transactions.json");
            var table = new WebService() { Name = "transactions", MaxImportLevels = 5};

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
        
        public async Task ParentChild_Write(Connection connection, string databaseName, bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            _output.WriteLine("Using database: " + databaseName);
            
            var autoIncrement = useDbAutoIncrement
                ? EDeltaType.DbAutoIncrement
                : EDeltaType.AutoIncrement;

            var transactionType = useTransaction
                ? TransformWriterTarget.ETransformWriterMethod.Transaction
                : TransformWriterTarget.ETransformWriterMethod.Bulk;

            await connection.CreateDatabase(databaseName, CancellationToken.None);
            
            var transactionTable = new Table("transaction");
            transactionTable.Columns.Add(new TableColumn("transactionId", ETypeCode.Int32, EDeltaType.NaturalKey));
            transactionTable.Columns.Add(new TableColumn("desc", ETypeCode.String, EDeltaType.TrackingField));
            transactionTable.Columns.Add(new TableColumn("transactionKey", ETypeCode.Int64, autoIncrement));

            var componentTable = new Table("component");
            componentTable.Columns.Add(new TableColumn("itemId", ETypeCode.Int32, EDeltaType.NaturalKey));
            componentTable.Columns.Add(new TableColumn("desc", ETypeCode.String, EDeltaType.TrackingField));
            componentTable.Columns.Add(new TableColumn("componentKey", ETypeCode.Int64, autoIncrement));
            componentTable.Columns.Add(new TableColumn("transactionKey", ETypeCode.Int64, EDeltaType.TrackingField));

            var reader = await GetReader();

            // add two target tables
            var transactionOptions = new TransformWriterOptions() {TargetAction = TransformWriterOptions.ETargetAction.DropCreate};
            var targets = new TransformWriterTarget(connection, transactionTable, null, transactionOptions);
            targets.Add(new TransformWriterTarget(connection, componentTable, new TransformWriterResult(), transactionOptions), new[] {"items"});
            
            await targets.WriteRecordsAsync(reader, updateStrategy, transactionType, CancellationToken.None);

            var transactionReader = connection.GetTransformReader(transactionTable);
            transactionReader = new TransformSort(transactionReader, "transactionId");
            await transactionReader.Open();

            await transactionReader.ReadAsync();
            var transactionKey = Convert.ToInt64(transactionReader["transactionKey"]);
            Assert.Equal(1, transactionReader["transactionId"]);
            Assert.Equal("product 1", transactionReader["desc"]);
            Assert.Equal(transactionKey++, transactionReader["transactionKey"]);

            await transactionReader.ReadAsync();
            Assert.Equal(2, transactionReader["transactionId"]);
            Assert.Equal("product 2", transactionReader["desc"]);
            Assert.Equal(transactionKey, transactionReader["transactionKey"]);
            
            Assert.False(await transactionReader.ReadAsync());

            var componentReader = connection.GetTransformReader(componentTable);
            var sortQuery = new SelectQuery()
            {
                Sorts = new Sorts() {new Sort(componentTable["componentKey"])}
            };
            await componentReader.Open(0, sortQuery, CancellationToken.None);

            var componentKey = -1L;
            transactionKey = -1L;
            for (var a = 1; a <= 2; a++)
            {
                if (transactionKey > -1) transactionKey++;

                for (var i = 1; i <= 3; i++)
                {
                    await componentReader.ReadAsync();
                    var componentId = (10 * a) + i;
                    Assert.Equal(componentId, componentReader["itemId"]);
                    Assert.Equal($"component {componentId}", componentReader["desc"]);

                    if (transactionKey == -1)
                    {
                        transactionKey = (long)componentReader["transactionKey"];
                    }

                    Assert.Equal(transactionKey, componentReader["transactionKey"]);
                    if (componentKey == -1)
                    {
                        componentKey = (long) componentReader["componentKey"];
                    }

                    Assert.Equal(componentKey++, componentReader["componentKey"]);
                }
            }
        }
        


        public async Task ParentChild_Write_Large(Connection connection, int rows, string databaseName, bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            _output.WriteLine("Using database: " + databaseName);
            
            await connection.CreateDatabase(databaseName, CancellationToken.None);
            
            var parentTable = DataSets.CreateParentTable();
            parentTable.AddAuditColumns("parent_key");

            var childTable = DataSets.CreateChildTable();
            childTable.AddAuditColumns("child_key");
            childTable.AddColumn("parent_key", ETypeCode.Int64);

            var grandChildTable = DataSets.CreateGrandChildTable();
            grandChildTable.AddAuditColumns("grandChild_key");
            grandChildTable.AddColumn("child_key", ETypeCode.Int64);

            if (useDbAutoIncrement)
            {
                parentTable["parent_key"].DeltaType = EDeltaType.DbAutoIncrement;
                childTable["child_key"].DeltaType = EDeltaType.DbAutoIncrement;
                grandChildTable["grandChild_key"].DeltaType = EDeltaType.DbAutoIncrement;
            }

            var parentTarget = new TransformWriterTarget(connection, parentTable);
            var childTarget = new TransformWriterTarget(connection, childTable);
            parentTarget.Add(childTarget, new[] {"children"});
            var grandChildTarget = new TransformWriterTarget(connection, grandChildTable);
            parentTarget.Add(grandChildTarget, new[] {"children", "grandChildren"});

            // creates a three level hierarchy parent/child/grandchild.
            var reader = DataSets.CreateParentChildReader(rows);

            await parentTarget.WriteRecordsAsync(reader, updateStrategy);
            
            Assert.Equal(rows, await connection.RowCount("parent"));
            Assert.Equal(rows, await connection.RowCount("child"));
            Assert.Equal(rows, await connection.RowCount("grandChild"));
        }
    }
}