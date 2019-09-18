using dexih.functions.Query;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.connections.test
{
    public class TransformTests
    {

        public async Task Transform(Connection connection, string databaseName)
        {
            var table = DataSets.CreateTable(connection.CanUseDbAutoIncrement);
            await connection.CreateDatabase(databaseName, CancellationToken.None);
            
            var writerResult = new TransformWriterResult(connection)
            {
                HubKey = 0, AuditConnectionKey = 1, AuditType = "Datalink"
            };
            
            //create a new table and write some data to it.  
            Transform reader = DataSets.CreateTestData();
            await connection.CreateTable(table, true, CancellationToken.None);
            
            var writer = new transforms.TransformWriterTarget(connection, table, writerResult);
            await writer.WriteRecordsAsync(reader, CancellationToken.None);
            
            //check database can sort 
            if (connection.CanSort)
            {
                //use the new table test the data base is sorting
                reader = connection.GetTransformReader(table);

                var query = new SelectQuery()
                {
                    Sorts = new Sorts() { new Sort("IntColumn", Sort.EDirection.Descending) }
                };
                await reader.Open(0, query, CancellationToken.None);


                var sortValue = 10;
                while (await reader.ReadAsync())
                {
                    Assert.Equal(sortValue, Convert.ToInt32(reader["IntColumn"]));
                    sortValue--;
                }
                Assert.Equal(0, sortValue);
            }

            //check database can filter
            if (connection.CanFilter)
            {
                //use the new table to test database is filtering
                reader = connection.GetTransformReader(table);

                var query = new SelectQuery()
                {
                    Filters = new Filters("IntColumn", ECompare.LessThanEqual, 5)
                };
                await reader.Open(0, query, CancellationToken.None);


                var count = 0;
                while (await reader.ReadAsync())
                {
                    Assert.True(Convert.ToInt32(reader["IntColumn"]) <= 5);
                    count++;
                }
                Assert.Equal(5, count);
            }

            var deltaTable = DataSets.CreateTable(connection.CanUseDbAutoIncrement);
            deltaTable.AddAuditColumns();
            deltaTable.Name = "DeltaTable";
            await connection.CreateTable(deltaTable, true, CancellationToken.None);

            var targetReader = connection.GetTransformReader(deltaTable);
            reader = connection.GetTransformReader(table);
            
//            var transformDelta = new TransformDelta(reader, targetReader, TransformDelta.EUpdateStrategy.AppendUpdate, 1, false);
//            await transformDelta.Open(0, null, CancellationToken.None);
//
//            writerResult = new TransformWriterResult(0, 1, "Datalink", 1, 2, "Test", 1, "Source", 2, "Target", null, null);
//            await connection.InitializeAudit(writerResult, CancellationToken.None);
//
//            var writeAllResult = await writer.WriteRecordsAsync( writerResult, transformDelta, TransformWriterTarget.ETransformWriterMethod.Bulk, deltaTable, connection, null, CancellationToken.None);
//            Assert.True(writeAllResult, writerResult.Message);
//            Assert.Equal(10L, writerResult.RowsCreated);

            writerResult = new TransformWriterResult(connection) {HubKey = 0, AuditType = "Datalink", AuditConnectionKey = 1};
            
            var options = new TransformWriterOptions();
            var target = new transforms.TransformWriterTarget(connection, deltaTable, writerResult);
            await target.WriteRecordsAsync(reader, TransformDelta.EUpdateStrategy.Reload, CancellationToken.None);

            Assert.Equal(10L, writerResult.RowsCreated);

            //check the audit table loaded correctly.
            var auditTable = await connection.GetTransformWriterResults(0, 1, null, "Datalink", writerResult.AuditKey, null, true, false, false, null, 1, null, false, CancellationToken.None);
            Assert.Equal(10L, auditTable[0].RowsCreated);
        }

    }
}
