﻿using dexih.connections;
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
    public class UnitTests
    {


        //run tests applicable to a managed database.
        public void Unit(Connection connection, string databaseName)
        {
            ReturnValue returnValue;

            returnValue = connection.CreateDatabase(databaseName).Result;
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            Table table = Helpers.CreateTable();

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

            SelectQuery selectQuery;

            //run a select query with one row, sorted descending.  
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("StringColumn", SelectColumn.EAggregate.None) },
                Sorts = new List<Sort>() { new Sort { Column = "IntColumn", Direction = Sort.EDirection.Descending } },
                Rows = 1,
                Table = "test_table"
            };

            //should return value2 from second row
            var returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);

            if (connection.CanSort == true) //azure can't sort, so don't bother with this test.
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


            //run a select query to validate the updated row.
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("DateColumn") },
                Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.IsEqual, 2) }, 
                Rows = 1,
                Table = "test_table"
            };

            //should return udpated date 
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);
            Assert.True((DateTime)returnScalar.Value == new DateTime(2001, 01, 21), "DateTime didn't match");


            //run a simple aggregate query to get max value from decimaColumn
            if (connection.CanAggregate)
            {
                selectQuery = new SelectQuery()
                {
                    Columns = new List<SelectColumn>() { new SelectColumn("DecimalColumn", SelectColumn.EAggregate.Max) },
                    Sorts = new List<Sort>() { new Sort { Column = "DateColumn", Direction = Sort.EDirection.Ascending } },
                    Groups = new List<string>() { "DateColumn" },
                    Rows = 1,
                    Table = "test_table"
                };

                //should return value2 from second row
                returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
                Assert.True(returnScalar.Success, "SelectQuery2 - Message:" + returnScalar.Message);
                Assert.True(Decimal.Compare(Convert.ToDecimal(returnScalar.Value), (Decimal)1.2) == 0, "SelectQuery2 - returned value: " + returnScalar.Value.ToString() + " Message:" + returnScalar.Message);
            }

            //run a delete query.
            var deleteQuery = new DeleteQuery()
            {
                Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.IsEqual, 1) },
                  Table = "test_table"
            };

            //should return value2 from second row
            var returnDelete = connection.ExecuteDelete(table, new List<DeleteQuery>() { deleteQuery }, CancellationToken.None).Result;
            Assert.True(returnDelete.Success, "Delete Query - Message:" + returnScalar.Message);

            //run a select query to check row is deleted
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("DateColumn") },
                Filters = new List<Filter>() { new Filter("IntColumn", Filter.ECompare.IsEqual, 1) },
                Rows = 1,
                Table = "test_table"
            };

            //should return null
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
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

                returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
                Assert.True(returnScalar.Success, "Select count - Message:" + returnScalar.Message);
                Assert.True(Convert.ToInt64(returnScalar.Value) == 1, "Select count - value :" + returnScalar.Message);
            }

            //run a truncate
            var truncateResult = connection.TruncateTable(table, CancellationToken.None).Result;
            Assert.True(truncateResult.Success, "truncate error: " + truncateResult.Message);

            //check the table is empty following truncate 
            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("StringColumn") },
                Rows = 1,
                Table = "test_table"
            };

            //should return null
            returnScalar = connection.ExecuteScalar(table, selectQuery, CancellationToken.None).Result;
            Assert.True(returnScalar.Success, "SelectQuery - Message:" + returnScalar.Message);
            Assert.True(returnScalar.Value == null);


            //start a datawriter and insert the test data
            connection.DataWriterStart(table).Wait();
            var testData = Helpers.CreateTestData();

            var bulkResult = connection.ExecuteInsertBulk(table, testData, CancellationToken.None).Result;
            Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);


            //check the table loaded 10 rows successully
            Transform reader = connection.GetTransformReader(table, null);
            int count = 0;
            var openResult = reader.Open().Result;
            Assert.True(openResult.Success, "Open Reader:" + openResult.Message);
            while (reader.Read()) count++;
            Assert.True(count == 10, "Select count - value :" + count);

            //run a lookup query.
            var filters = new List<Filter> { new Filter("IntColumn", Filter.ECompare.IsEqual, 5) };

            //should return value5
            reader = connection.GetTransformReader(table, null);
            openResult = reader.Open().Result;
            Assert.True(openResult.Success, "Open Reader:" + openResult.Message);

            var returnLookup = reader.LookupRow(filters).Result;
            Assert.True(returnLookup.Success, "Lookup - Message:" + returnLookup.Message);
            Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "LookupValue :" + returnLookup.Value[0]);

            //run lookup again with caching set.
            reader = connection.GetTransformReader(table, null);
            openResult = reader.Open().Result;
            Assert.True(openResult.Success, "Open Reader:" + openResult.Message);
            reader.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);
            returnLookup = reader.LookupRow(filters).Result;
            Assert.True(returnLookup.Success, "Lookup - Message:" + returnLookup.Message);
            Assert.True(Convert.ToString(returnLookup.Value[0]) == "value5", "Select count - value :" + returnLookup.Value);



        }

        

    }
}