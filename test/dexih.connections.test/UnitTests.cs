using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.flatfile;
using Xunit;

namespace dexih.connections.test
{
    public class UnitTests
    {

        public async Task Unit(Connection connection, string databaseName)
        {
            ReturnValue returnValue;

            returnValue = await connection.CreateDatabase(databaseName, CancellationToken.None);
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            var newTable = DataSets.CreateTable();

            var initTableResult = await connection.InitializeTable(newTable, 1000);
            Assert.True(initTableResult.Success, initTableResult.Message);
            var table = initTableResult.Value;

            //create the table
            returnValue = await connection.CreateTable(table, true, CancellationToken.None);
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);

            //insert a single row
            InsertQuery insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                    new QueryColumn(new TableColumn("IntColumn", DataType.ETypeCode.Int32), 1),
                    new QueryColumn(new TableColumn("StringColumn", DataType.ETypeCode.String), "value1" ),
                new QueryColumn(new TableColumn("DateColumn", DataType.ETypeCode.DateTime), new DateTime(2001, 01, 21) ),
                    new QueryColumn(new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal), 1.1 ),
                    new QueryColumn(new TableColumn("GuidColumn", DataType.ETypeCode.Guid), Guid.NewGuid() )
            });

            returnValue = await connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None);
            Assert.True(returnValue.Success, "InsertQuery - Message:" + returnValue.Message);

            //insert a second row
            insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                    new QueryColumn(new TableColumn("IntColumn", DataType.ETypeCode.Int32), 2 ),
                    new QueryColumn(new TableColumn("StringColumn", DataType.ETypeCode.String), "value2" ),
                    new QueryColumn(new TableColumn("DateColumn", DataType.ETypeCode.DateTime), new DateTime(2001, 01, 21) ),
                    new QueryColumn(new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal), 1.2 ),
                    new QueryColumn(new TableColumn("GuidColumn", DataType.ETypeCode.Guid), Guid.NewGuid() )
            });

            returnValue = await connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None);

            Assert.True(returnValue.Success, "InsertQuery - Message:" + returnValue.Message);

            ////if the write was a file.  move it back to the incoming directory to read it.
            if(connection.DatabaseCategory == Connection.ECategory.File)
            {
                var fileConnection = (ConnectionFlatFile)connection;
                var filename = fileConnection.LastWrittenFile;

                var filemoveResult = await fileConnection.MoveFile((FlatFile) table, filename,
                    FlatFile.EFlatFilePath.outgoing, FlatFile.EFlatFilePath.incoming);
                    
                Assert.True(filemoveResult.Success, filemoveResult.Message);
            }
            
            SelectQuery selectQuery;

            //run a select query with one row, sorted descending.  
            if (connection.CanFilter)
            {
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn(new TableColumn("StringColumn"), SelectColumn.EAggregate.None) },
                    Sorts = new List<Sort>() { new Sort { Column = new TableColumn("IntColumn"), Direction = Sort.EDirection.Descending } },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return value2 from second row
                var returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);

                //if the connection doesn't support sorting, don't bother with this test.
                if (connection.CanSort == true) 
                    Assert.True((string)returnScalar.Value == "value2", "SelectQuery - Message:" + returnScalar.Message);
            }

            if (connection.CanUpdate)
            {
                //run an update query which will change the second date value to 2001-01-21
                var updateQuery = new UpdateQuery()
                {
                    UpdateColumns = new List<QueryColumn>() { new QueryColumn(new TableColumn("DateColumn", DataType.ETypeCode.DateTime), new DateTime(2001, 01, 21)) },
                    Filters = new List<Filter>() { new Filter() { Column1 = new TableColumn("IntColumn"), Operator = Filter.ECompare.IsEqual, Value2 = 2, CompareDataType = DataType.ETypeCode.Int32 } }
                };

                var returnUpdate = await connection.ExecuteUpdate(table, new List<UpdateQuery>() { updateQuery }, CancellationToken.None);
                Assert.True(returnUpdate.Success, "UpdateQuery - Message:" + returnUpdate.Message);

                //run a select query to validate the updated row.
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn(new TableColumn("DateColumn", DataType.ETypeCode.DateTime)) },
                    Filters = new List<Filter>() { new Filter(new TableColumn("IntColumn"), Filter.ECompare.IsEqual, 2) },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return updated date 
                var returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);
                Assert.True((DateTime)returnScalar.Value == new DateTime(2001, 01, 21), "DateTime didn't match");
            }

            //run a simple aggregate query to get max value from decimaColumn
            if (connection.CanAggregate)
            {
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn("DecimalColumn", SelectColumn.EAggregate.Max) },
                    Sorts = new List<Sort>() { new Sort("DateColumn") },
                    Groups = new List<TableColumn>() { new TableColumn("DateColumn") },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return value2 from second row
                var returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True(returnScalar.Success, "SelectQuery2 - Message:" + returnScalar.Message);
                Assert.True(Decimal.Compare(Convert.ToDecimal(returnScalar.Value), (Decimal)1.2) == 0, "SelectQuery2 - returned value: " + returnScalar.Value.ToString() + " Message:" + returnScalar.Message);
            }

            if (connection.CanDelete)
            {
                //run a delete query.
                var deleteQuery = new DeleteQuery()
                {
                    Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.IsEqual, 1) },
                    Table = "test_table"
                };

                //should return value2 from second row
                var returnDelete = await connection.ExecuteDelete(table, new List<DeleteQuery>() { deleteQuery }, CancellationToken.None);
                Assert.True(returnDelete.Success, "Delete Query - Message:" + returnDelete.Message);

                //run a select query to check row is deleted
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn("DateColumn") },
                    Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.IsEqual, 1) },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return null
                var returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);
                Assert.True(returnScalar.Value == null);

                //run an aggregate query to check rows left
                if (connection.CanAggregate)
                {
                    selectQuery = new SelectQuery()
                    {
                        Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                        Rows = 1000,
                        Table = "test_table"
                    };

                    returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                    Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
                    Assert.True(Convert.ToInt64(returnScalar.Value) == 1, "Select count - value :" + returnScalar.Message);
                }

                //run a truncate
                var truncateResult = await connection.TruncateTable(table, CancellationToken.None);
                Assert.True(truncateResult.Success, "truncate error: " + truncateResult.Message);

                //check the table is empty following truncate 
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn("StringColumn") },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return null
                returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);
                Assert.True(returnScalar.Value == null);
                // }
            }

            if (connection.CanBulkLoad)
            {
                await connection.TruncateTable(table, CancellationToken.None);

                //start a datawriter and insert the test data
                await connection.DataWriterStart(table);
                var testData = DataSets.CreateTestData();

                var bulkResult = await connection.ExecuteInsertBulk(table, testData, CancellationToken.None);
                Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);

                await connection.DataWriterFinish(table);

                ////if the write was a file.  move it back to the incoming directory to read it.
                if(connection.DatabaseCategory == Connection.ECategory.File)
                {
                    var fileConnection = (ConnectionFlatFile)connection;
                    var filename = fileConnection.LastWrittenFile;

                    var filemoveResult = await fileConnection.MoveFile((FlatFile) table, filename,
                        FlatFile.EFlatFilePath.outgoing, FlatFile.EFlatFilePath.incoming);
                    
                    Assert.True(filemoveResult.Success, filemoveResult.Message);
                }

                //check the table loaded 10 rows successully
                Transform reader = connection.GetTransformReader(table, null, null, true);
                int count = 0;
                var openResult = await reader.Open(0, null, CancellationToken.None);
                Assert.True(openResult.Success, "Open Reader:" + openResult.Message);
                while (await reader.ReadAsync()) count++;
                Assert.True(count == 10, "Select count - value :" + count);
            }

            if (connection.CanFilter == true)
            {
                //run a lookup query.
                var filters = new List<Filter> { new Filter("IntColumn", Filter.ECompare.IsEqual, 5) };

                //should return value5
                var reader = connection.GetTransformReader(table, null);

                if (reader.CanLookupRowDirect)
                {
                    var openResult = await reader.Open(0, null, CancellationToken.None);
                    Assert.True(openResult.Success, "Open Reader:" + openResult.Message);

                    var returnLookup = await reader.LookupRow(filters, CancellationToken.None);
                    Assert.True(returnLookup.Success, "Lookup - Message:" + returnLookup.Message);
                    Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "LookupValue :" + returnLookup.Value[0]);

                    //run lookup again with caching set.
                    reader = connection.GetTransformReader(table, null);
                    openResult = await reader.Open(0, null, CancellationToken.None);
                    Assert.True(openResult.Success, "Open Reader:" + openResult.Message);
                    reader.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);
                    returnLookup = await reader.LookupRow(filters, CancellationToken.None);
                    Assert.True(returnLookup.Success, "Lookup - Message:" + returnLookup.Message);
                    Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "Select count - value :" + returnLookup.Value);
                }

                reader.Close();
            }

        }

    }
}
