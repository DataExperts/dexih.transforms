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
    public class PerformanceTests
    {

        /// <summary>
        /// Perfromance tests should run in around 1 minute. 
        /// </summary>
        /// <param name="connection"></param>
        public void Performance(Connection connection, string databaseName, int rows)
        {
            ReturnValue returnValue;

            returnValue = connection.CreateDatabase(databaseName).Result;
            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

            //create a table that utilizes every available datatype.
            Table table = new Table("LargeTable" + (Helpers.counter++).ToString()) ;

            table.Columns.Add(new TableColumn("SurrogateKey", DataType.ETypeCode.Int32, TableColumn.EDeltaType.SurrogateKey));
            table.Columns.Add(new TableColumn("UpdateTest", DataType.ETypeCode.Int32));

            foreach (DataType.ETypeCode typeCode in Enum.GetValues(typeof(DataType.ETypeCode)))
            {
                table.Columns.Add(new TableColumn() { ColumnName = "column" + typeCode.ToString(), DataType = typeCode, MaxLength = 50, DeltaType = TableColumn.EDeltaType.TrackingField });
            }

            //create the table
            returnValue = connection.CreateTable(table, true).Result;
            Assert.True(returnValue.Success, "CreateManagedTables - Message:" + returnValue.Message);

            //add rows.
            int buffer = 0;
            for (int i = 0; i < rows; i++)
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

                if(buffer >= 5000 || rows == i+1)
                {
                    //start a datawriter and insert the test data
                    connection.DataWriterStart(table).Wait();

                    var bulkResult = connection.ExecuteInsertBulk(table, new ReaderMemory(table), CancellationToken.None).Result;
                    Assert.True(bulkResult.Success, "WriteDataBulk - Message:" + bulkResult.Message);

                    table.Data.Clear();
                    buffer = 0;
                }
            }


            //count rows using reader
            int count = 0;
            var reader = connection.GetTransformReader(table);
            reader.Open().Wait();
            while (reader.Read()) count++;
            Assert.True(count == rows, "row count = " + count.ToString());


            List<UpdateQuery> updateQueries = new List<UpdateQuery>();

            //run a update on 10% of rows
            for(int i = 0; i<(rows/10); i++)
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
                     Filters = new List<Filter>() {  new Filter("SurrogateKey", Filter.ECompare.IsEqual, i) },
                     Table = table.TableName,
                     UpdateColumns = updateColumns                     
                });
            }

            var updateResult = connection.ExecuteUpdate(table, updateQueries, CancellationToken.None).Result;
            Assert.True(updateResult.Success, "Update- Message:" + updateResult.Message);

            //check the table loaded 1,000 rows updated successully
            var selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("UpdateTest") },
                Filters = new List<Filter>() { new Filter() { Column1 = "UpdateTest", CompareDataType = DataType.ETypeCode.Int32, Operator = Filter.ECompare.IsEqual, Value2 = 1 } },
                Rows = -1,
                Table = table.TableName
            };

            //count rows using reader
            count = 0;
            reader = connection.GetTransformReader(table);
            reader.Open(selectQuery).Wait();
            while (reader.Read()) count++;
            Assert.True(count == rows/10, "row count = " + count.ToString());

            List<DeleteQuery> deleteQueries = new List<DeleteQuery>();
            //delete 10,000 rows
                deleteQueries.Add(new DeleteQuery()
                {
                    Filters = new List<Filter>() {  new Filter() { Column1 = "UpdateTest", CompareDataType = DataType.ETypeCode.Int32, Operator = Filter.ECompare.IsEqual, Value2 = 1 } },
                    Table = table.TableName,
                });

            var deleteResult = connection.ExecuteDelete(table, deleteQueries, CancellationToken.None).Result;
            Assert.True(deleteResult.Success, "Delete - Message:" + deleteResult.Message);

            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() { new SelectColumn("SurrogateKey") },
//                Filters = new List<Filter>() { new Filter() { Column1 = "column1", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.NotEqual, Value2 = "updated" } },
                Rows = -1,
                Table = table.TableName
            };

            //count rows using reader
            count = 0;
            reader = connection.GetTransformReader(table);
            reader.Open().Wait();
            while (reader.Read()) count++;
            Assert.True(count == rows - rows / 10, "row count = " + count.ToString());


            //run a preview
            var query = table.DefaultSelectQuery(50);
            var previewTable = connection.GetPreview(table, query, 50, CancellationToken.None).Result;
            Assert.Equal(true, previewTable.Success);
            Assert.Equal(50, previewTable.Value.Data.Count);


        }

    }
}
