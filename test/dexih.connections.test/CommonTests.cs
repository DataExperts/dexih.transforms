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
    public class CommonTests
    {
        public static ReaderMemory CreateTestData()
        {
            Table table = new Table("test", 0,
                new TableColumn("StringColumn", DataType.ETypeCode.String),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32),
                new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal),
                new TableColumn("DateColumn", DataType.ETypeCode.DateTime),
                new TableColumn("GuidColumn", DataType.ETypeCode.Guid)
                );

            table.Data.Add(new object[] { "value1", 1, 1.1, Convert.ToDateTime("2015/01/01"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value2", 2, 2.1, Convert.ToDateTime("2015/01/02"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value3", 3, 3.1, Convert.ToDateTime("2015/01/03"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value4", 4, 4.1, Convert.ToDateTime("2015/01/04"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value5", 5, 5.1, Convert.ToDateTime("2015/01/05"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value6", 6, 6.1, Convert.ToDateTime("2015/01/06"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value7", 7, 7.1, Convert.ToDateTime("2015/01/07"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value8", 8, 8.1, Convert.ToDateTime("2015/01/08"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value9", 9, 9.1, Convert.ToDateTime("2015/01/09"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), Guid.NewGuid() });

            ReaderMemory Adapter = new ReaderMemory(table);
            Adapter.Reset();
            return Adapter;
        }

        public static Table CreateTable()
        {
            Table table = new Table("testtable")
            {
                Description = "The testing table"
            };

            table.Columns.Add(new TableColumn()
            {
                ColumnName = "StringColumn",
                Description = "A string column",
                DataType = DataType.ETypeCode.String,
                DeltaType = TableColumn.EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn() {
                ColumnName = "IntColumn",
                Description = "An integer column",
                DataType = DataType.ETypeCode.Int32,
                DeltaType = TableColumn.EDeltaType.SurrogateKey
            });

            table.Columns.Add(new TableColumn()
            {
                ColumnName = "DecimalColumn",
                Description = "A decimal column",
                DataType = DataType.ETypeCode.Decimal,
                DeltaType = TableColumn.EDeltaType.TrackingField,
                Scale = 2,
                Precision = 10
            });

            table.Columns.Add(new TableColumn()
            {
                ColumnName = "DateColumn",
                Description = "A date column column",
                DataType = DataType.ETypeCode.DateTime,
                DeltaType = TableColumn.EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn()
            {
                ColumnName = "GuidColumn",
                Description = "A guid column",
                DataType = DataType.ETypeCode.Guid,
                DeltaType = TableColumn.EDeltaType.TrackingField
            });

            return table;
        }


        //run tests applicable to a managed database.
        public static void UnitTests(Connection connection, string databaseName, bool CanSort = true)
        {
            ReturnValue returnValue;

            returnValue = connection.CreateDatabase(databaseName).Result;
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            Table table = CreateTable();

            connection.AddMandatoryColumns(table, 1000);

            //create the table
            returnValue = connection.CreateTable(table, true).Result;
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);

            //insert a single row
            InsertQuery insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                    new QueryColumn("IntColumn", DataType.ETypeCode.Int32, 1),
                    new QueryColumn("StringColumn", DataType.ETypeCode.String, "value1" ),
                    new QueryColumn("DateColumn", DataType.ETypeCode.DateTime, "2001-01-21" ),
                    new QueryColumn("DecimalColumn", DataType.ETypeCode.Decimal, 1.1 ),
                    new QueryColumn("GuidColumn", DataType.ETypeCode.Guid, Guid.NewGuid() )
            });

            returnValue = connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None).Result;
            Assert.True(returnValue.Success, "InsertQuery - Message:" + returnValue.Message);

            //insert a second row
            insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                    new QueryColumn("IntColumn", DataType.ETypeCode.Int32, 2 ),
                    new QueryColumn("StringColumn", DataType.ETypeCode.String, "value2" ),
                    new QueryColumn("DateColumn", DataType.ETypeCode.DateTime, "2001-01-22" ),
                    new QueryColumn("DecimalColumn", DataType.ETypeCode.Decimal, 1.2 ),
                    new QueryColumn("GuidColumn", DataType.ETypeCode.Guid, Guid.NewGuid() )
            });

            returnValue = connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None).Result;
            Assert.True(returnValue.Success, "InsertQuery - Message:" + returnValue.Message);

            
            //run a select query with one row, sorted descending.  
            SelectQuery selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("StringColumn", SelectColumn.EAggregate.None) },
                Sorts = new List<Sort>() { new Sort { Column = "IntColumn", Direction = Sort.EDirection.Descending } },
                Rows = 1,
                Table = "test_table"
            };

            //should return value2 from second row
            var returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);

            if (CanSort == true) //azure can't sort, so don't bother with this test.
                Assert.True((string)returnScalar.Value == "value2", "SelectQuery - Message:" + returnScalar.Message);

            //run an update query which will change the second date value to 2001-01-21
            var updateQuery = new UpdateQuery()
            {
                UpdateColumns = new List<QueryColumn>() { new QueryColumn("DateColumn", DataType.ETypeCode.DateTime, "2001-01-21") } ,
                Filters = new List<Filter>() { new Filter() { Column1 = "IntColumn", Operator = Filter.ECompare.IsEqual, Value2 = 2, CompareDataType = DataType.ETypeCode.Int32 } }
            };

            var returnUpdate = connection.ExecuteUpdate(table, new List<UpdateQuery>() { updateQuery }, CancellationToken.None).Result;
            Assert.True(returnUpdate.Success, "UpdateQuery - Message:" + returnUpdate.Message);
            Assert.True(returnUpdate.Value == 1, "UpdateQuery - Message:" + returnUpdate.Message);

            //run a simple aggregate query.
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("DecimalColumn", SelectColumn.EAggregate.Max) },
                Sorts = new List<Sort>() { new Sort { Column = "DateColumn", Direction = Sort.EDirection.Ascending } },
                Groups = new List<string>() {   "DateColumn" },
                Rows = 1,
                Table = "test_table"
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "SelectQuery2 - Message:" + returnScalar.Message);
            Assert.True(Decimal.Compare(Convert.ToDecimal(returnScalar.Value), (Decimal)1.2) == 0, "SelectQuery2 - returned value: " + returnScalar.Value.ToString() + " Message:" + returnScalar.Message);


            //run a delete query.
            var deleteQuery = new DeleteQuery()
            {
                Filters = new List<Filter>() { new Filter() { Column1 = "IntColumn", Operator = Filter.ECompare.IsEqual, Value2 = 1 } },
                  Table = "test_table"
            };

            //should return value2 from second row
            var returnDelete = connection.ExecuteDelete(table, new List<DeleteQuery>() { deleteQuery }, CancellationToken.None).Result;
            Assert.True(returnDelete.Success, "Delete Query - Message:" + returnScalar.Message);

            //run a select query to check row is deleted
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                Rows = 1000,
                Table = "test_table"
            };

            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 1, "Select count - value :" + returnScalar.Message);

            //run a truncate
            var truncateResult = connection.TruncateTable(table, CancellationToken.None).Result;
            Assert.True(truncateResult.Success, "truncate error: " + truncateResult.Message);

            //check the table is empty following truncate 
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                Rows = 1000,
                Table = "test_table"
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 0, "Select count - value :" + returnScalar.Message);


            //start a datawriter and insert the test data
            connection.DataWriterStart(table).Wait();
            var testData = CreateTestData();

            var bulkResult = connection.ExecuteInsertBulk(table, testData, CancellationToken.None).Result;
            Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);


            //check the table loaded 10 rows successully
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                Rows = 1000,
                Table = "test_table"
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 10, "Select count - value :" + returnScalar.Value);

            //run a lookup query.
            var filters = new List<Filter> { new Filter("IntColumn", Filter.ECompare.IsEqual, 5) };

            //should return value5
            Transform reader = connection.GetTransformReader(table, null);
            var openResult = reader.Open().Result;
            Assert.True(openResult.Success, "Open Reader:" + openResult.Message);

            var returnLookup = reader.LookupRow(filters).Result;
            Assert.True(returnLookup.Success, "Lookup - Message:" + returnLookup.Message);
            Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "Select count - value :" + returnLookup.Value);

            //run lookup again with caching set.
            reader = connection.GetTransformReader(table, null);
            openResult = reader.Open().Result;
            Assert.True(openResult.Success, "Open Reader:" + openResult.Message);
            reader.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);
            returnLookup = reader.LookupRow(filters).Result;
            Assert.True(returnLookup.Success, "Lookup - Message:" + returnLookup.Message);
            Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "Select count - value :" + returnLookup.Value);



        }

        /// <summary>
        /// Perfromance tests should run in around 1 minute. 
        /// </summary>
        /// <param name="connection"></param>
        public static void PerformanceTests(Connection connection, string databaseName)
        {
            ReturnValue returnValue;

            returnValue = connection.CreateDatabase(databaseName).Result;
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            //create a table that utilizes every available datatype.
            Table table = new Table("large_table") ;

            table.Columns.Add(new TableColumn("SurrogateKey", DataType.ETypeCode.Int32, TableColumn.EDeltaType.SurrogateKey));
            table.Columns.Add(new TableColumn("UpdateTest", DataType.ETypeCode.Int32));

            foreach (DataType.ETypeCode typeCode in Enum.GetValues(typeof(DataType.ETypeCode)))
            {
                table.Columns.Add(new TableColumn() { ColumnName = "column" + typeCode.ToString(), DataType = typeCode, MaxLength = 50, DeltaType = TableColumn.EDeltaType.TrackingField });
            }

            //create the table
            returnValue = connection.CreateTable(table, true).Result;
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);

            //add 1 million rows.
            int buffer = 0;
            for (int i = 0; i < 100000; i++)
            {
                object[] row = new object[table.Columns.Count];

                row[0] = i;
                row[1] = 0;

                //load the rows with random values.
                for(int j =2; j < table.Columns.Count; j++)
                {
                    Type dataType = DataType.GetType(table.Columns[j].DataType);
                    if(i%2 == 0)
                        row[j] = DataType.GetDataTypeMaxValue(table.Columns[j].DataType);
                    else
                        row[j] = DataType.GetDataTypeMinValue(table.Columns[j].DataType);
                }
                table.Data.Add(row);
                buffer++;

                if(buffer >= 50000)
                {
                    //start a datawriter and insert the test data
                    connection.DataWriterStart(table).Wait();

                    var bulkResult = connection.ExecuteInsertBulk(table, new ReaderMemory(table), CancellationToken.None).Result;
                    Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);

                    table.Data.Clear();
                    buffer = 0;
                }
            }


            //check the table loaded 100,000 rows successully
            var selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("SurrogateKey", SelectColumn.EAggregate.Count) },
                Rows = -1,
                Table = table.TableName
            };

            //should return all rows
            var returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "Select count 1 - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 100000, "Select count - value :" + returnScalar.Message);

            List<UpdateQuery> updateQueries = new List<UpdateQuery>();

            //run a large 10,000 row update.
            for(int i = 0; i<10000; i++)
            {
                var updateColumns = new List<QueryColumn>();

                //use this column to validate the update success
                var updateColumn = new QueryColumn(table.Columns[1].ColumnName, table.Columns[1].DataType, 1);
                updateColumns.Add(updateColumn);

                //load the columns with random values.
                for (int j = 2; j < table.Columns.Count; j++)
                {
                    updateColumn = new QueryColumn(table.Columns[j].ColumnName, table.Columns[j].DataType, DataType.GetDataTypeMaxValue(table.Columns[j].DataType));
                    updateColumns.Add(updateColumn);
                }
                updateQueries.Add(new UpdateQuery()
                {
                     Filters = new List<Filter>() {  new Filter() { Column1 = "SurrogateKey", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.IsEqual, Value2 = i} },
                     Table = table.TableName,
                     UpdateColumns = updateColumns
                });
            }

            var updateResult = connection.ExecuteUpdate(table, updateQueries, CancellationToken.None).Result;
            Assert.True(updateResult.Success, "Update- Message:" + updateResult.Message);

            //check the table loaded 10,000 rows updated successully
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("UpdateTest", SelectColumn.EAggregate.Count) },
                Filters = new List<Filter>() { new Filter() { Column1 = "UpdateTest", CompareDataType = DataType.ETypeCode.Int32, Operator = Filter.ECompare.IsEqual, Value2 = 1 } },
                Rows = -1,
                Table = table.TableName
            };

            //should return udpated rows
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "Select count 2- Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 10000, "Select count - value :" + returnScalar.Message);

            List<DeleteQuery> deleteQueries = new List<DeleteQuery>();
            //delete 10,000 rows
            for (int i = 0; i < 10000; i++)
            {
                deleteQueries.Add(new DeleteQuery()
                {
                    Filters = new List<Filter>() {  new Filter() { Column1 = "UpdateTest", CompareDataType = DataType.ETypeCode.Int32, Operator = Filter.ECompare.IsEqual, Value2 = 1 } },
                    Table = table.TableName,
                });
            }

            var deleteResult = connection.ExecuteDelete(table, deleteQueries, CancellationToken.None).Result;
            Assert.True(deleteResult.Success, "Delete - Message:" + deleteResult.Message);

            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("SurrogateKey", SelectColumn.EAggregate.Count) },
//                Filters = new List<Filter>() { new Filter() { Column1 = "column1", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.NotEqual, Value2 = "updated" } },
                Rows = -1,
                Table = table.TableName
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "Select count 3 - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 90000, "Select count - value :" + returnScalar.Message);


            //run a preview
            var query = table.DefaultSelectQuery(1000);
            var previewTable = connection.GetPreview(table, query, 1000, CancellationToken.None).Result;
            Assert.Equal(true, previewTable.Success);
            Assert.Equal(1000, previewTable.Value.Data.Count);


        }

    }
}
