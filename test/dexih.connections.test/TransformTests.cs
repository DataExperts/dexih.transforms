using dexih.connections;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.test
{
    public class TransformTests
    {

        public async Task Transform(Connection connection, string databaseName)
        {
            Table table = DataSets.CreateTable();

            bool returnValue;
            returnValue = await connection.CreateDatabase(databaseName, CancellationToken.None);
            Assert.True(returnValue, "New Database");

            //create a new table and write some data to it.  
            Transform reader = DataSets.CreateTestData();
            returnValue = await connection.CreateTable(table, true, CancellationToken.None);
            Assert.True(returnValue, "CreateManagedTables");
            TransformWriter writer = new TransformWriter();

            TransformWriterResult writerResult = await connection.InitializeAudit(0, "DataLink", 1, 2, "Test", 1, "Source", 2, "Target", TransformWriterResult.ETriggerMethod.Manual, "Test", CancellationToken.None);

            returnValue = await writer.WriteAllRecords(writerResult, reader, table, connection, null, null, null, null, CancellationToken.None);
            Assert.True(returnValue, "Write data");

            //check database can sort 
            if (connection.CanSort)
            {
                //use the new table test the data base is sorting
                reader = connection.GetTransformReader(table);

                SelectQuery query = new SelectQuery()
                {
                    Sorts = new List<Sort>() { new Sort("IntColumn", Sort.EDirection.Descending) }
                };
                await reader.Open(0, query, CancellationToken.None);


                int sortValue = 10;
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

                SelectQuery query = new SelectQuery()
                {
                    Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.LessThanEqual, 5) }
                };
                await reader.Open(0, query, CancellationToken.None);


                int count = 0;
                while (await reader.ReadAsync())
                {
                    Assert.True(Convert.ToInt32(reader["IntColumn"]) <= 5);
                    count++;
                }
                Assert.Equal(5, count);
            }

            Table deltaTable = DataSets.CreateTable();
            deltaTable.AddAuditColumns();
            deltaTable.Name = "DeltaTable";
            returnValue = await connection.CreateTable(deltaTable, true, CancellationToken.None);

            Transform targetReader = connection.GetTransformReader(deltaTable);
            reader = connection.GetTransformReader(table);
            TransformDelta transformDelta = new TransformDelta(reader, targetReader, TransformDelta.EUpdateStrategy.AppendUpdate, 1, false);

            writerResult = await connection.InitializeAudit(0, "DataLink", 1, 2, "Test", 1, "Source", 2, "Target", TransformWriterResult.ETriggerMethod.Manual, "Test", CancellationToken.None);

            returnValue = await writer.WriteAllRecords(writerResult, transformDelta, deltaTable, connection, CancellationToken.None);
            Assert.True(returnValue);
            Assert.Equal(10, writerResult.RowsCreated);

            //check the audit table loaded correctly.
            var auditTable = await connection.GetTransformWriterResults(0, null, writerResult.AuditKey, null, true, false, false, null, 1, null, false, CancellationToken.None);
            Assert.Equal((long)10, auditTable[0].RowsCreated);

        }

    }
}
