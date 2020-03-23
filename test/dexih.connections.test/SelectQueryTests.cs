using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.sql;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using Dexih.Utils.DataType;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.test
{
    public class SelectQueryTests
    {
        private readonly ITestOutputHelper _output;

        public SelectQueryTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        public async Task SelectQuery(Connection connection, string databaseName)
        {
            // create a new table with data.
            var newTable = DataSets.CreateTable(connection.CanUseDbAutoIncrement);
            var table = await connection.InitializeTable(newTable, 1000, CancellationToken.None);
            await connection.CreateTable(newTable, false);
            
            //start a data writer and insert the test data
            await connection.DataWriterStart(table, CancellationToken.None);
            var testData = DataSets.CreateTestData();

            var convertedTestData = new ReaderConvertDataTypes(connection, testData);
            await convertedTestData.Open();
            await connection.ExecuteInsertBulk(table, convertedTestData, CancellationToken.None);

            await connection.DataWriterFinish(table, CancellationToken.None);

            _output.WriteLine("Testing select columns");
            var selectQuery = new SelectQuery()
            {
                Columns = new SelectColumns(table.Columns["StringColumn"], table.Columns["IntColumn"])
            };

            var reader = connection.GetTransformReader(table);
            await reader.Open(selectQuery);

            await reader.ReadAsync();
            Assert.Equal(2, reader.FieldCount);
            Assert.Equal(reader.GetName(0), table.Columns["StringColumn"].Name);
            Assert.Equal(reader.GetName(1), table.Columns["IntColumn"].Name);

            _output.WriteLine("Testing filter with equals");
            selectQuery.Filters.Add(new Filter(table["StringColumn"], ECompare.IsEqual, "value1"));
            selectQuery.Filters.Add(new Filter(table["IntColumn"], ECompare.IsEqual, 1));
            selectQuery.Filters.Add(new Filter(table["BooleanColumn"], ECompare.IsEqual, true));
            selectQuery.Filters.Add(new Filter(table["BooleanColumn"], ECompare.IsEqual, true));
            selectQuery.Filters.Add(new Filter(table["DateColumn"], ECompare.IsEqual, Convert.ToDateTime("2015/01/01")));

            reader = connection.GetTransformReader(table);
            await reader.Open(selectQuery);
            await reader.ReadAsync();
            Assert.Equal("value1", reader["StringColumn"] );

            var moreRows = await reader.ReadAsync();
            Assert.False(moreRows);
            
            _output.WriteLine("Group by boolean column");
            selectQuery.Filters = new Filters();
            selectQuery.Groups = new List<TableColumn>() {table.Columns["BooleanColumn"]};
            selectQuery.Columns = new SelectColumns()
            {
                new SelectColumn(table.Columns["BooleanColumn"], EAggregate.None),
                new SelectColumn(table["IntColumn"], EAggregate.Sum),
                new SelectColumn(table["DateColumn"], EAggregate.Max),
            };
            
            reader = connection.GetTransformReader(table);
            await reader.Open(selectQuery);
            var count = 0;
            while (await reader.ReadAsync())
            {
                count++;
                if ((bool) reader["BooleanColumn"] == true)
                {
                    Assert.Equal(25, reader["IntColumn"]);
                    Assert.Equal(Convert.ToDateTime("2015/01/09"), reader["DateColumn"]);
                }
                else
                {
                    Assert.Equal(30, reader["IntColumn"]);
                    Assert.Equal(Convert.ToDateTime("2015/01/10"), reader["DateColumn"]);
                }
            }

            Assert.Equal(2, count);
            
            _output.WriteLine("Group with a having clause");
            
            // filter less than 5
            selectQuery.Filters.Add(new Filter(table["IntColumn"], ECompare.LessThanEqual, 5));
            
            // having greater than 9
            selectQuery.GroupFilters.Add(new Filter(table["IntColumn"], ECompare.IsEqual, 9));
            reader = connection.GetTransformReader(table);
            await reader.Open(selectQuery);
            await reader.ReadAsync();
            
            Assert.Equal(9, reader["IntColumn"]);
            Assert.Equal(Convert.ToDateTime("2015/01/05"), reader["DateColumn"]);
            
            Assert.False(await reader.ReadAsync());
            
        }
    }
}