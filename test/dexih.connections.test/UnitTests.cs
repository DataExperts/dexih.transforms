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
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.test
{
    public class UnitTests
    {

        public async Task Unit(Connection connection, string databaseName)
        {
            await connection.CreateDatabase(databaseName, CancellationToken.None);

            var newTable = DataSets.CreateTable();
            var table = await connection.InitializeTable(newTable, 1000);

            //create the table
            await connection.CreateTable(table, true, CancellationToken.None);

            //insert a single row
            InsertQuery insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                    new QueryColumn(new TableColumn("IntColumn", ETypeCode.Int32), 1),
                    new QueryColumn(new TableColumn("StringColumn", ETypeCode.String), "value1" ),
                    new QueryColumn(new TableColumn("DateColumn", ETypeCode.DateTime), new DateTime(2001, 01, 21, 0, 0, 0, DateTimeKind.Utc) ),
                    new QueryColumn(new TableColumn("BooleanColumn", ETypeCode.Boolean), true ),
                    new QueryColumn(new TableColumn("DoubleColumn", ETypeCode.Double), 1.1 ),
                    new QueryColumn(new TableColumn("DecimalColumn", ETypeCode.Decimal), 1.1m ),
                    new QueryColumn(new TableColumn("GuidColumn", ETypeCode.Guid), Guid.NewGuid() )
            });

            await connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None);

            //insert a second row
            insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                    new QueryColumn(new TableColumn("IntColumn", ETypeCode.Int32), 2 ),
                    new QueryColumn(new TableColumn("StringColumn", ETypeCode.String), "value2" ),
                    new QueryColumn(new TableColumn("BooleanColumn", ETypeCode.Boolean), false ),
                    new QueryColumn(new TableColumn("DateColumn", ETypeCode.DateTime), new DateTime(2001, 01, 21, 0, 0, 0, DateTimeKind.Utc) ),
                    new QueryColumn(new TableColumn("DoubleColumn", ETypeCode.Double), 1.1 ),
                    new QueryColumn(new TableColumn("DecimalColumn", ETypeCode.Decimal), 1.2m ),
                    new QueryColumn(new TableColumn("GuidColumn", ETypeCode.Guid), Guid.NewGuid() )
            });

            await connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None);

            ////if the write was a file.  move it back to the incoming directory to read it.
            if(connection.DatabaseConnectionCategory == Connection.EConnectionCategory.File)
            {
                var fileConnection = (ConnectionFlatFile)connection;
                var filename = fileConnection.LastWrittenFile;

                var filemoveResult = await fileConnection.MoveFile((FlatFile) table, filename,
                    EFlatFilePath.Outgoing, EFlatFilePath.Incoming);
                    
                Assert.True(filemoveResult);
            }
            
            SelectQuery selectQuery;

            //run a select query with one row, sorted descending.  
            if (connection.CanFilter)
            {
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn(new TableColumn("StringColumn")) },
                    Sorts = new List<Sort>() { new Sort { Column = new TableColumn("IntColumn"), Direction = Sort.EDirection.Descending } },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return value2 from second row
                var returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.NotNull(returnScalar);

                //if the connection doesn't support sorting, don't bother with this test.
                if (connection.CanSort == true) 
                    Assert.Equal("value2", (string)returnScalar);
            }

            if (connection.CanUpdate)
            {
                //run an update query which will change the second date value to 2001-01-21
                var updateQuery = new UpdateQuery()
                {
                    UpdateColumns = new List<QueryColumn>() { new QueryColumn(new TableColumn("DateColumn", ETypeCode.DateTime), new DateTime(2001, 01, 21, 0, 0, 0, DateTimeKind.Utc)) },
                    Filters = new List<Filter>() { new Filter() { Column1 = new TableColumn("IntColumn"), Operator = Filter.ECompare.IsEqual, Value2 = 2, CompareDataType = ETypeCode.Int32 } }
                };

                await connection.ExecuteUpdate(table, new List<UpdateQuery>() { updateQuery }, CancellationToken.None);

                //run a select query to validate the updated row.
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn(new TableColumn("DateColumn", ETypeCode.DateTime)) },
                    Filters = new List<Filter>() { new Filter(new TableColumn("IntColumn"), Filter.ECompare.IsEqual, 2) },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return updated date 
                var returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True((DateTime)returnScalar == new DateTime(2001, 01, 21), "DateTime didn't match");
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
                Assert.True(Decimal.Compare(Convert.ToDecimal(returnScalar), (Decimal)1.2) == 0, "SelectQuery2 - returned value: " + returnScalar.ToString());
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
                await connection.ExecuteDelete(table, new List<DeleteQuery>() { deleteQuery }, CancellationToken.None);

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
                Assert.True(returnScalar == null);

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
                    Assert.True(Convert.ToInt64(returnScalar) == 1, "Select count");
                }

                //run a truncate
                await connection.TruncateTable(table, CancellationToken.None);

                //check the table is empty following truncate 
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn("StringColumn") },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return null
                returnScalar = await connection.ExecuteScalar(table, selectQuery, CancellationToken.None);
                Assert.True(returnScalar == null);
                // }
            }

            if (connection.CanBulkLoad)
            {
                await connection.TruncateTable(table, CancellationToken.None);

                //start a datawriter and insert the test data
                await connection.DataWriterStart(table);
                var testData = DataSets.CreateTestData();

                await connection.ExecuteInsertBulk(table, testData, CancellationToken.None);

                await connection.DataWriterFinish(table);

                ////if the write was a file.  move it back to the incoming directory to read it.
                if(connection.DatabaseConnectionCategory == Connection.EConnectionCategory.File)
                {
                    var fileConnection = (ConnectionFlatFile)connection;
                    var filename = fileConnection.LastWrittenFile;

                    var filemoveResult = await fileConnection.MoveFile((FlatFile) table, filename,
                        EFlatFilePath.Outgoing, EFlatFilePath.Incoming);
                    
                    Assert.True(filemoveResult);
                }

                //check the table loaded 10 rows successully
                Transform reader = connection.GetTransformReader(table, true);
                int count = 0;
                var openResult = await reader.Open(0, null, CancellationToken.None);
                Assert.True(openResult, "Open Reader");
                while (await reader.ReadAsync()) count++;
                Assert.True(count == 10, "Select count - value :" + count);
            }

            if (connection.CanFilter == true)
            {
                //run a lookup query.
                var filters = new List<Filter> { new Filter("IntColumn", Filter.ECompare.IsEqual, 5) };
                var query = new SelectQuery()
                {
                    Filters = filters
                };

                //should return value5
                var reader = connection.GetTransformReader(table, true);

//                if (reader.CanLookupRowDirect)
//                {
                    // var openResult = await reader.Open(0, null, CancellationToken.None);
                    // Assert.True(openResult, "Open Reader");

                    var returnLookup = await reader.Lookup(query, Transform.EDuplicateStrategy.Abend, CancellationToken.None);
                    Assert.True(Convert.ToString(returnLookup.First()[0]) == "value5", "LookupValue :" + returnLookup.First()[0]);

                    //run lookup again with caching set.
                    reader = connection.GetTransformReader(table);
                    // var openResult = await reader.Open(0, null, CancellationToken.None);
                    // Assert.True(openResult, "Open Reader");
                    reader.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);
                    returnLookup = await reader.Lookup(query, Transform.EDuplicateStrategy.Abend, CancellationToken.None);
                    Assert.True(Convert.ToString(returnLookup.First()[0]) == "value5", "Select count - value :" + returnLookup.First()[0]);
                // }

                reader.Close();
            }

        }

    }
}
