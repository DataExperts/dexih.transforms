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
  


 

        
        public void Transform(Connection connection, string databaseName)
        {
            Table table = Helpers.CreateTable();

            ReturnValue returnValue;
            returnValue = connection.CreateDatabase(databaseName).Result;
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            //create a new table and write some data to it.  
            Transform reader = Helpers.CreateTestData();
            returnValue = connection.CreateTable(table, true).Result;
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);
            TransformWriter writer = new TransformWriter();
            TransformWriterResult writerResult = new TransformWriterResult();
            returnValue = writer.WriteAllRecords(writerResult, reader, table, connection, null, null, CancellationToken.None).Result;
            Assert.True(returnValue.Success, "Write data:" + returnValue.Message);

            //check database can sort 
            if(connection.CanSort)
            {
                //use the new table test the data base is sorting
                reader = connection.GetTransformReader(table);

                SelectQuery query = new SelectQuery()
                {
                    Sorts = new List<Sort>() { new Sort("IntColumn", Sort.EDirection.Descending) }
                };
                reader.Open(query).Wait();

                int sortValue = 10;
                while(reader.Read())
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
                    Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.LessThanEqual, 5)  }
                };
                reader.Open(query).Wait();

                int count = 0;
                while (reader.Read())
                {
                    Assert.True((int)reader["IntColumn"] <= 5);
                    count++;
                }
                Assert.Equal(5, count);
            }

            Table deltaTable = Helpers.CreateTable();
            deltaTable.AddAuditColumns();
            deltaTable.TableName = "DeltaTable";
            returnValue = connection.CreateTable(deltaTable, true).Result;

            Transform targetReader = connection.GetTransformReader(deltaTable);
            reader = connection.GetTransformReader(table);
            TransformDelta transformDelta = new TransformDelta(reader, targetReader, TransformDelta.EUpdateStrategy.AppendUpdate, 1, 1);

            writerResult = new TransformWriterResult();
            returnValue = writer.WriteAllRecords(writerResult, transformDelta, deltaTable, connection, null, null, CancellationToken.None).Result;
            Assert.True(returnValue.Success, returnValue.Message);
            Assert.Equal(10, writerResult.RowsCreated);

        }

    }
}
