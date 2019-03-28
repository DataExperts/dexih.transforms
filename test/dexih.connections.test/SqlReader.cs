using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.sql;
using dexih.functions;
using dexih.transforms;
using Xunit;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.test
{
    /// <summary>
    /// Tests using sql in the table as a source.
    /// </summary>
    public class SqlReaderTests
    {
        public async Task Unit(Connection connection, string databaseName)
        {
            await connection.CreateDatabase(databaseName, CancellationToken.None);

            var newTable = DataSets.CreateTable(connection.CanUseDbAutoIncrement);
            var table = await connection.InitializeTable(newTable, 1000);

            //create the table
            await connection.CreateTable(table, true, CancellationToken.None);

            var guid = Guid.NewGuid();
            
            //insert a single row
            var insertQuery = new InsertQuery( new List<QueryColumn>() {
                new QueryColumn(new TableColumn("IntColumn", ETypeCode.Int32), 1),
                new QueryColumn(new TableColumn("StringColumn", ETypeCode.String), "value1" ),
                new QueryColumn(new TableColumn("DateColumn", ETypeCode.DateTime), new DateTime(2001, 01, 21) ),
                new QueryColumn(new TableColumn("DoubleColumn", ETypeCode.Decimal), 1.1 ),
                new QueryColumn(new TableColumn("DecimalColumn", ETypeCode.Decimal), 1.1m ),
                new QueryColumn(new TableColumn("BooleanColumn", ETypeCode.Boolean), true ),
                new QueryColumn(new TableColumn("GuidColumn", ETypeCode.Guid), guid ),
                new QueryColumn(new TableColumn("ArrayColumn", ETypeCode.Int32, rank: 1), new [] {1,1} ),
                new QueryColumn(new TableColumn("MatrixColumn", ETypeCode.Int32, rank: 2), new [,] {{1,1}, {2,2}} )
            });

            var insertReturn = await connection.ExecuteInsert(table, new List<InsertQuery>() { insertQuery }, CancellationToken.None);
//            Assert.True(insertReturn > 0, "InsertQuery");

            Table sqlTable;
            if (connection.CanUseSql)
            {
                var sqlConnection = (ConnectionSql) connection;
                // create a simple table with a sql query.
                sqlTable = new Table("SqlTest")
                {
                    UseQuery = true,
                    QueryString = $"select * from {sqlConnection.SqlTableName(table)}"
                };
            }
            else
            {
                sqlTable = table;
            }

            // check the columns can be imported.
            var importTable = await connection.GetSourceTableInfo(sqlTable, CancellationToken.None);

            Assert.Equal(10, importTable.Columns.Count);
            
            Assert.Equal("IntColumn", importTable.Columns["IntColumn"].Name);
            Assert.True(ETypeCode.Int32 == importTable.Columns["IntColumn"].DataType || ETypeCode.Int64 == importTable.Columns["IntColumn"].DataType);
            Assert.Equal("StringColumn", importTable.Columns["StringColumn"].Name);
            Assert.Equal(ETypeCode.String, importTable.Columns["StringColumn"].DataType);
            // commented date check as sqlite treats dates as string.  Value check below does the test adequately.
            Assert.Equal("DateColumn", importTable.Columns["DateColumn"].Name);
            Assert.True(ETypeCode.String == importTable.Columns["DateColumn"].DataType || ETypeCode.DateTime == importTable.Columns["DateColumn"].DataType);
            Assert.Equal("DecimalColumn", importTable.Columns["DecimalColumn"].Name);
            Assert.True(ETypeCode.Decimal == importTable.Columns["DecimalColumn"].DataType || ETypeCode.Double == importTable.Columns["DecimalColumn"].DataType);
            Assert.Equal("GuidColumn", importTable.Columns["GuidColumn"].Name);
            Assert.True(ETypeCode.String == importTable.Columns["GuidColumn"].DataType || ETypeCode.Guid == importTable.Columns["GuidColumn"].DataType);

            if (connection.CanUseArray)
            {
                Assert.Equal("ArrayColumn", importTable.Columns["ArrayColumn"].Name);
                Assert.Equal(ETypeCode.Int32, importTable.Columns["ArrayColumn"].DataType);
                Assert.Equal(1, importTable.Columns["ArrayColumn"].Rank);
                Assert.Equal("MatrixColumn", importTable.Columns["MatrixColumn"].Name);
                Assert.Equal(2, importTable.Columns["MatrixColumn"].Rank);
            }
            else
            {
                Assert.Equal("ArrayColumn", importTable.Columns["ArrayColumn"].Name);
                Assert.Equal(ETypeCode.String, importTable.Columns["ArrayColumn"].DataType);
                Assert.Equal("MatrixColumn", importTable.Columns["MatrixColumn"].Name);
                Assert.Equal(ETypeCode.String, importTable.Columns["MatrixColumn"].DataType);
                
            }

            // check rows can be read.
            var reader = connection.GetTransformReader(importTable);
            var openResult = await reader.Open(0, null, CancellationToken.None);
            Assert.True(openResult);

            var finished = await reader.ReadAsync();
            Assert.True(finished);
            Assert.Equal((long)1,  long.Parse(reader["IntColumn"].ToString()));
            Assert.Equal("value1", reader["StringColumn"]);
            Assert.Equal(new DateTime(2001, 01, 21), DateTime.Parse(reader["DateColumn"].ToString()));
            Assert.Equal((decimal)1.1, reader.GetDecimal(reader.GetOrdinal("DecimalColumn")));
            Assert.Equal(guid.ToString(), reader["GuidColumn"].ToString());

            // test the preview function returns one row.
            var previewResult = await connection.GetPreview(importTable, null, CancellationToken.None);
            Assert.Single(previewResult.Data);
        }
    }
}