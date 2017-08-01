using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms;
using Xunit;

namespace dexih.connections.test
{
    /// <summary>
    /// Tests using sql in the table as a source.
    /// </summary>
    public class SqlReaderTests
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

            var guid = Guid.NewGuid();
            
            //insert a single row
            var insertQuery = new InsertQuery("test_table", new List<QueryColumn>() {
                new QueryColumn(new TableColumn("IntColumn", DataType.ETypeCode.Int32), 1),
                new QueryColumn(new TableColumn("StringColumn", DataType.ETypeCode.String), "value1" ),
                new QueryColumn(new TableColumn("DateColumn", DataType.ETypeCode.DateTime), new DateTime(2001, 01, 21) ),
                new QueryColumn(new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal), 1.1 ),
                new QueryColumn(new TableColumn("GuidColumn", DataType.ETypeCode.Guid), guid )
            });

            returnValue = await connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None);
            Assert.True(returnValue.Success, "InsertQuery - Message:" + returnValue.Message);

            Table sqlTable;
            if (connection.CanUseSql)
            {
                // create a simple table with a sql query.
                sqlTable = new Table("SqlTest")
                {
                    UseQuery = true,
                    QueryString = $"select * from {table.Name}"
                };
            }
            else
            {
                sqlTable = table;
            }

            // check the columns can be imported.
            var importTableResult = await connection.GetSourceTableInfo(sqlTable, CancellationToken.None);
            Assert.True(importTableResult.Success, importTableResult.Message);

            var importTable = importTableResult.Value;
            
            Assert.Equal(5, importTable.Columns.Count);
            
            Assert.Equal("IntColumn", importTable.Columns["IntColumn"].Name);
            Assert.True(DataType.ETypeCode.Int32 == importTable.Columns["IntColumn"].Datatype || DataType.ETypeCode.Int64 == importTable.Columns["IntColumn"].Datatype);
            Assert.Equal("StringColumn", importTable.Columns["StringColumn"].Name);
            Assert.Equal(DataType.ETypeCode.String, importTable.Columns["StringColumn"].Datatype);
            // commented date check as sqlite treats dates as string.  Value check below does the test adequately.
//            Assert.Equal("DateColumn", importTable.Columns["DateColumn"].Name);
//            Assert.Equal(DataType.ETypeCode.DateTime, importTable.Columns["DateColumn"].Datatype);
//            Assert.Equal("DecimalColumn", importTable.Columns["DecimalColumn"].Name);
//            Assert.Equal(DataType.ETypeCode.Decimal, importTable.Columns["DecimalColumn"].Datatype);
            Assert.Equal("GuidColumn", importTable.Columns["GuidColumn"].Name);
            Assert.True(DataType.ETypeCode.String == importTable.Columns["GuidColumn"].Datatype || DataType.ETypeCode.Guid == importTable.Columns["GuidColumn"].Datatype);

            // check rows can be read.
            var reader = connection.GetTransformReader(importTable);
            var openResult = await reader.Open(0, null, CancellationToken.None);
            Assert.True(openResult.Success, openResult.Message);

            var finished = await reader.ReadAsync();
            Assert.True(finished);
            Assert.Equal((Int64)1,  Int64.Parse(reader["IntColumn"].ToString()));
            Assert.Equal("value1", reader["StringColumn"]);
            Assert.Equal(new DateTime(2001, 01, 21), DateTime.Parse(reader["DateColumn"].ToString()));
            Assert.Equal((decimal)1.1, reader.GetDecimal(reader.GetOrdinal("DecimalColumn")));
            Assert.Equal(guid.ToString(), reader["GuidColumn"].ToString());

            // test the preview function returns one row.
            var previewResult = await connection.GetPreview(importTable, null, CancellationToken.None);
            Assert.True(previewResult.Success, previewResult.Message);
            Assert.Equal(1, previewResult.Value.Data.Count);
        }
    }
}