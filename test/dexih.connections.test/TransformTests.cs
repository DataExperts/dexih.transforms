using dexih.connections;
using dexih.functions;
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
                Table table = Helpers.CreateTable();

                ReturnValue returnValue;
                returnValue = await connection.CreateDatabase(databaseName);
                Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

                //create a new table and write some data to it.  
                Transform reader = Helpers.CreateTestData();
                returnValue = await connection.CreateTable(table, true);
                Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);
                TransformWriter writer = new TransformWriter();
                TransformWriterResult writerResult = new TransformWriterResult();
                returnValue = await writer.WriteAllRecords(writerResult, reader, table, connection, null, null, CancellationToken.None);
                Assert.True(returnValue.Success, "Write data:" + returnValue.Message);

                //check database can sort 
                if (connection.CanSort)
                {
                    //use the new table test the data base is sorting
                    reader = connection.GetTransformReader(table);

                    SelectQuery query = new SelectQuery()
                    {
                        Sorts = new List<Sort>() { new Sort("IntColumn", Sort.EDirection.Descending) }
                    };
                    await reader.Open(query);


                    int sortValue = 10;
                    while (await reader.ReadAsync())
                    {
                        Assert.Equal(sortValue, reader["IntColumn"]);
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
                    await reader.Open(query);


                    int count = 0;
                    while (await reader.ReadAsync())
                    {
                        Assert.True((int)reader["IntColumn"] <= 5);
                        count++;
                    }
                    Assert.Equal(5, count);
                }

                Table deltaTable = Helpers.CreateTable();
                deltaTable.AddAuditColumns();
                deltaTable.TableName = "DeltaTable";
                returnValue = await connection.CreateTable(deltaTable, true);

                Transform targetReader = connection.GetTransformReader(deltaTable);
                reader = connection.GetTransformReader(table);
                TransformDelta transformDelta = new TransformDelta(reader, targetReader, TransformDelta.EUpdateStrategy.AppendUpdate, 1, 1);

                writerResult = new TransformWriterResult();
                returnValue = await writer.WriteAllRecords(writerResult, transformDelta, deltaTable, connection, null, null, CancellationToken.None);
                Assert.True(returnValue.Success, returnValue.Message);
                Assert.Equal(10, writerResult.RowsCreated);
        }

    }
}
