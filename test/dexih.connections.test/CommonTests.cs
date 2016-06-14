using dexih.connections;
using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.test
{
    public class CommonTests
    {
        public static SourceTable CreateTestData()
        {
            Table table = new Table("test", new List<TableColumn>() {
                new TableColumn("StringColumn", DataType.ETypeCode.String),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32),
                new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal),
                new TableColumn("DateColumn", DataType.ETypeCode.DateTime),
                });

            table.Data.Add(new object[] { "value1", 1, 1.1, Convert.ToDateTime("2015/01/01"), });
            table.Data.Add(new object[] { "value2", 2, 2.1, Convert.ToDateTime("2015/01/02"), });
            table.Data.Add(new object[] { "value3", 3, 3.1, Convert.ToDateTime("2015/01/03"), });
            table.Data.Add(new object[] { "value4", 4, 4.1, Convert.ToDateTime("2015/01/04"), });
            table.Data.Add(new object[] { "value5", 5, 5.1, Convert.ToDateTime("2015/01/05"), });
            table.Data.Add(new object[] { "value6", 6, 6.1, Convert.ToDateTime("2015/01/06"), });
            table.Data.Add(new object[] { "value7", 7, 7.1, Convert.ToDateTime("2015/01/07"), });
            table.Data.Add(new object[] { "value8", 8, 8.1, Convert.ToDateTime("2015/01/08"), });
            table.Data.Add(new object[] { "value9", 9, 9.1, Convert.ToDateTime("2015/01/09"), });
            table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), });

            SourceTable Adapter = new SourceTable(table);
            Adapter.Reset();
            return Adapter;
        }

        public static Table CreateTable()
        {
            Table table = new Table("test_table")
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

            return table;
        }




        //run tests applicable to a managed database.
        public static void UnitTests(Connection connection, string databaseName)
        {
            ReturnValue returnValue;

            returnValue = connection.CreateDatabase(databaseName).Result;
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            Table table = CreateTable();

            connection.AddMandatoryColumns(table, 1000);

            //create the table
            returnValue = connection.CreateManagedTable(table, true).Result;
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);

            //insert a single row
            InsertQuery insertQuery = new InsertQuery()
            {
                InsertColumns = new List<QueryColumn>() {
                    new QueryColumn() { Column = "IntColumn", ColumnType = DataType.ETypeCode.Int32, Value = 1 },
                    new QueryColumn() { Column = "StringColumn", ColumnType = DataType.ETypeCode.String, Value = "value1" },
                    new QueryColumn() { Column = "DateColumn", ColumnType = DataType.ETypeCode.DateTime, Value = "2001-01-21" },
                    new QueryColumn() { Column = "DecimalColumn", ColumnType = DataType.ETypeCode.Decimal, Value = 1.1 }
                },
                Table = "test_table"
            };

            returnValue = connection.ExecuteInsertQuery(table, new List<InsertQuery>() { insertQuery }).Result;
            Assert.True(returnValue.Success, "InsertQuery - Message:" + returnValue.Message);

            //insert a second row
            insertQuery = new InsertQuery()
            {
                InsertColumns = new List<QueryColumn>() {
                    new QueryColumn() { Column = "IntColumn", ColumnType = DataType.ETypeCode.Int32, Value = 2 },
                    new QueryColumn() { Column = "StringColumn", ColumnType = DataType.ETypeCode.String, Value = "value2" },
                    new QueryColumn() { Column = "DateColumn", ColumnType = DataType.ETypeCode.DateTime, Value = "2001-01-22" },
                    new QueryColumn() { Column = "DecimalColumn", ColumnType = DataType.ETypeCode.Decimal, Value = 1.2 }
                },
                Table = "test_table"
            };

            returnValue = connection.ExecuteInsertQuery(table, new List<InsertQuery>() { insertQuery }).Result;
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
            var returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);
            Assert.True((string)returnScalar.Value == "value2", "SelectQuery - Message:" + returnScalar.Message);

            //run an update query which will change the second date value to 2001-01-21
            var updateQuery = new UpdateQuery()
            {
                UpdateColumns = new List<QueryColumn>() { new QueryColumn() { Column = "DateColumn", ColumnType = DataType.ETypeCode.DateTime, Value = "2001-01-21" } },
                Filters = new List<Filter>() { new Filter() { Column1 = "IntColumn", Operator = Filter.ECompare.EqualTo, Value2 = 2 } }
            };

            var returnUpdate = connection.ExecuteUpdateQuery(table, new List<UpdateQuery>() { updateQuery }).Result;
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
            returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "SelectQuery2 - Message:" + returnScalar.Message);
            Assert.True(Decimal.Compare(Convert.ToDecimal(returnScalar.Value), (Decimal)1.2) == 0, "SelectQuery2 - returned value: " + returnScalar.Value.ToString() + " Message:" + returnScalar.Message);


            //run a delete query.
            var deleteQuery = new DeleteQuery()
            {
                Filters = new List<Filter>() { new Filter() { Column1 = "IntColumn", Operator = Filter.ECompare.EqualTo, Value2 = 1 } },
                  Table = "test_table"
            };

            //should return value2 from second row
            var returnDelete = connection.ExecuteDeleteQuery(table, new List<DeleteQuery>() { deleteQuery }).Result;
            Assert.True(returnDelete.Success, "Delete Query - Message:" + returnScalar.Message);

            //run a select query to check row is deleted
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                Rows = 1000,
                Table = "test_table"
            };

            returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 1, "Select count - value :" + returnScalar.Message);

            //run a truncate
            var truncateResult = connection.TruncateTable(table).Result;
            Assert.True(truncateResult.Success, "truncate error: " + truncateResult.Message);

            //check the table is empty following truncate 
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                Rows = 1000,
                Table = "test_table"
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 0, "Select count - value :" + returnScalar.Message);


            //start a datawriter and insert the test data
            connection.DataWriterStart(table);
            var testData = CreateTestData();

            var bulkResult = connection.WriteDataBulk(testData, table).Result;
            Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);


            //check the table loaded 10 rows successully
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("IntColumn", SelectColumn.EAggregate.Count) },
                Rows = 1000,
                Table = "test_table"
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 10, "Select count - value :" + returnScalar.Message);

            //run a lookup query.
            var filters = new List<Filter> { new Filter("IntColumn", Filter.ECompare.EqualTo, 5) };

            //should return value5
            connection.DataReaderStart(table, null, null).Wait();
            var returnLookup = connection.LookupRow(filters).Result;
            Assert.True(returnLookup.Success, "Lookup - Message:" + returnScalar.Message);
            Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "Select count - value :" + returnScalar.Message);

            //run lookup again with caching set.
            connection.CacheMethod = Transform.ECacheMethod.PreLoadCache;
            connection.DataReaderStart(table, null, null).Wait();
            returnLookup = connection.LookupRow(filters).Result;
            Assert.True(returnLookup.Success, "Lookup - Message:" + returnScalar.Message);
            Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "Select count - value :" + returnScalar.Message);
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

            Table table = new Table("large_table") ;
            for (int i = 0; i < 10; i++)
            {
                table.Columns.Add(new TableColumn() { ColumnName = "column" + i.ToString(), DataType = DataType.ETypeCode.String, MaxLength = 50, DeltaType = TableColumn.EDeltaType.TrackingField });
            }

            table.Columns[0].DeltaType = TableColumn.EDeltaType.SurrogateKey;

            //create the table
            returnValue = connection.CreateManagedTable(table, true).Result;
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);

            //add 1 million rows.
            int buffer = 0;
            for (int i = 0; i < 1000000; i++)
            {
                table.Data.Add(Enumerable.Range(0, 10).Select(c => "row-" + i.ToString() + " column-" + c.ToString()).ToArray());
                buffer++;

                if(buffer >= 50000)
                {
                    //start a datawriter and insert the test data
                    connection.DataWriterStart(table);

                    var bulkResult = connection.WriteDataBulk(new SourceTable(table), table).Result;
                    Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);

                    table.Data.Clear();
                    buffer = 0;
                }
            }


            //check the table loaded 100,000 rows successully
            var selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("column1", SelectColumn.EAggregate.Count) },
                Rows = -1,
                Table = table.TableName
            };

            //should return value2 from second row
            var returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "Select count 1 - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 1000000, "Select count - value :" + returnScalar.Message);

            List<UpdateQuery> updateQueries = new List<UpdateQuery>();

            //run a large 100,000 row update.
            for(int i = 0; i<100000; i++)
            {
                updateQueries.Add(new UpdateQuery()
                {
                     Filters = new List<Filter>() {  new Filter() { Column1 = "column0", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.EqualTo, Value2 = "row-" + i.ToString() + " column-0"} },
                     Table = table.TableName,
                     UpdateColumns = new List<QueryColumn>() {  new QueryColumn() {  Column = "column1", ColumnType = DataType.ETypeCode.String, Value = "updated"} }
                });
            }

            var updateResult = connection.ExecuteUpdateQuery(table, updateQueries).Result;
            Assert.True(updateResult.Success, "Update- Message:" + updateResult.Message);

            //check the table loaded 10,000 rows updated successully
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("column1", SelectColumn.EAggregate.Count) },
                Filters = new List<Filter>() { new Filter() { Column1 = "column1", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.EqualTo, Value2 = "updated" } },
                Rows = -1,
                Table = table.TableName
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "Select count 2- Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 100000, "Select count - value :" + returnScalar.Message);

            List<DeleteQuery> deleteQueries = new List<DeleteQuery>();
            //delete 100,000 rows
            for (int i = 0; i < 100000; i++)
            {
                deleteQueries.Add(new DeleteQuery()
                {
                    Filters = new List<Filter>() {  new Filter() { Column1 = "column0", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.EqualTo, Value2 = "row-" + i.ToString() + " column-0"} },
                    Table = table.TableName,
                });
            }

            var deleteResult = connection.ExecuteDeleteQuery(table, deleteQueries).Result;
            Assert.True(deleteResult.Success, "Delete - Message:" + deleteResult.Message);

            //check the table loaded 100,000 rows deleted, by selecting <> "updated"
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("column1", SelectColumn.EAggregate.Count) },
                Filters = new List<Filter>() { new Filter() { Column1 = "column1", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.NotEqual, Value2 = "updated" } },
                Rows = -1,
                Table = table.TableName
            };

            //should return value2 from second row
            returnScalar = connection.ExecuteScalar(table, selectQuery).Result;
            Assert.True(returnScalar.Success, "Select count 3 - Message:" + returnScalar.Message);
            Assert.True(Convert.ToInt64(returnScalar.Value) == 900000, "Select count - value :" + returnScalar.Message);



        }

    }
}
