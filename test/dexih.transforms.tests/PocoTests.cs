using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Poco;
using Xunit;

namespace dexih.transforms.tests
{
    public class PocoTests
    {
        [Fact]
        public async Task PocoTest_Open()
        {
            await Task.Run(() =>
            {

                var reader = Helpers.CreateSortedTestData();
                var poco = new PocoLoader<SamplePocoClass>();
                poco.Open(reader);

                var count = 0;
                foreach (var item in poco)
                {
                    count++;
                    Assert.Equal("value" + count.ToString().PadLeft(2, '0'), item.StringColumn);
                    Assert.Equal(count, item.IntColumn);
                    Assert.Equal(Convert.ToDateTime("2015-01-" + count), item.TheDate);
                }

                Assert.Equal(10, count);
            });
        }
        
        [Fact]
        public async Task PocoTest_OpenCached()
        {
            await Task.Run(() =>
            {

                var reader = Helpers.CreateSortedTestData();
                var poco = new PocoLoader<SamplePocoClass>();
                poco.OpenCached(reader);

                var count = 0;
                foreach (var item in poco)
                {
                    count++;
                    Assert.Equal("value" + count.ToString().PadLeft(2, '0'), item.StringColumn);
                    Assert.Equal(count, item.IntColumn);
                    Assert.Equal(Convert.ToDateTime("2015-01-" + count), item.TheDate);
                }

                Assert.Equal(10, count);

                var oldItem = poco[5];
                Assert.Equal("value06", oldItem.StringColumn);

                Assert.Equal(10, poco.Count);
            });
        }
        
        [Fact]
        public async Task PocoTest_ToListAsync()
        {
            var reader = Helpers.CreateSortedTestData();
            var poco = new PocoLoader<SamplePocoClass>();
            var list = await poco.ToListAsync(reader);

            var count = 0;
            foreach (var item in list)
            {
                count++;
                Assert.Equal("value" + count.ToString().PadLeft(2, '0'), item.StringColumn);
                Assert.Equal(count, item.IntColumn);
                Assert.Equal(Convert.ToDateTime("2015-01-" + count), item.TheDate);
            }
            
            Assert.Equal(10, count);

        }

        [Fact]
        public async Task PocoTest_Reader()
        {
            var items = new List<SamplePocoClass>
            {
                new SamplePocoClass("column1", 1, new DateTime(2000, 01, 02)),
                new SamplePocoClass("column2", 2, new DateTime(2000, 01, 03)),
                new SamplePocoClass("column3", 3, new DateTime(2000, 01, 04))
            };
            
            var pocoReader = new PocoReader<SamplePocoClass>(items);
            await pocoReader.Open();
            
            var count = 0;
            while (await pocoReader.ReadAsync())
            {
                count++;
                Assert.Equal("column" + count, pocoReader["StringColumn"]);
                Assert.Equal(count, pocoReader["IntColumn"]);
                Assert.Equal(new DateTime(2000, 01, 01).AddDays(count), pocoReader["DateColumn"]);
            }

            Assert.Equal(3, count);
        }

        [Fact]
        public async Task PocoTest_Insert()
        {
            var items = new List<SamplePocoClass>
            {
                new SamplePocoClass("column1", 1, new DateTime(2000, 01, 02)),
                new SamplePocoClass("column2", 2, new DateTime(2000, 01, 03)),
                new SamplePocoClass("column3", 3, new DateTime(2000, 01, 04))
            };

            var pocoTable = new PocoTable<SamplePocoClass>();
            var connection = new ConnectionMemory();

            await pocoTable.CreateTable(connection, true, CancellationToken.None);
            await pocoTable.ExecuteInsert(connection, items[0], CancellationToken.None);
            Assert.Equal(1, items[0].Incremental);
            await pocoTable.ExecuteInsert(connection, items[1], CancellationToken.None);
            await pocoTable.ExecuteInsert(connection, items[2], CancellationToken.None);

            var reader = connection.GetTransformReader(pocoTable.Table);

            var count = 0;
            while (await reader.ReadAsync())
            {
                count++;
                Assert.Equal("column" + count, reader["StringColumn"]);
                Assert.Equal(count, reader["IntColumn"]);
                Assert.Equal(new DateTime(2000, 01, 01).AddDays(count), reader["DateColumn"]);
            }

            Assert.Equal(3, count);
        }

        [Fact]
        public async Task PocoTest_InsertBulk()
        {
            var items = new List<SamplePocoClass>
            {
                new SamplePocoClass("column1", 1, new DateTime(2000, 01, 02)),
                new SamplePocoClass("column2", 2, new DateTime(2000, 01, 03)),
                new SamplePocoClass("column3", 3, new DateTime(2000, 01, 04))
            };

            var pocoTable = new PocoTable<SamplePocoClass>();
            var connection = new ConnectionMemory();

            await pocoTable.CreateTable(connection, true, CancellationToken.None);
            await pocoTable.ExecuteInsertBulk(connection, items, CancellationToken.None);

            var reader = connection.GetTransformReader(pocoTable.Table);
            await reader.Open();

            var count = 0;
            while (await reader.ReadAsync())
            {
                count++;
                Assert.Equal("column" + count, reader["StringColumn"]);
                Assert.Equal(count, reader["IntColumn"]);
                Assert.Equal(new DateTime(2000, 01, 01).AddDays(count), reader["DateColumn"]);
            }

            Assert.Equal(3, count);
        }

        [Fact]
        public async Task PocoTest_Update()
        {
            var item = new SamplePocoClass("column1", 1, new DateTime(2000, 01, 02));
            var updateItem = new SamplePocoClass("column1", 2, new DateTime(2000, 01, 03));

            var pocoTable = new PocoTable<SamplePocoClass>();
            var connection = new ConnectionMemory();

            // creat table, insert sample column, and then update.
            await pocoTable.CreateTable(connection, true, CancellationToken.None);
            await pocoTable.ExecuteInsert(connection, item, CancellationToken.None);
            await pocoTable.ExecuteUpdate(connection, updateItem, CancellationToken.None);

            var reader = connection.GetTransformReader(pocoTable.Table);

            await reader.ReadAsync();
            Assert.Equal("column1", reader["StringColumn"]);
            Assert.Equal(2, reader["IntColumn"]);
            Assert.Equal(new DateTime(2000, 01, 03), reader["DateColumn"]);
        }

    
        [Fact]
        public async Task PocoTest_Delete()
        {
            var item = new SamplePocoClass("column1", 1, new DateTime(2000, 01, 02));

            var pocoTable = new PocoTable<SamplePocoClass>();
            var connection = new ConnectionMemory();

            // creat table, insert sample column, and then delete.
            await pocoTable.CreateTable(connection, true, CancellationToken.None);
            await pocoTable.ExecuteInsert(connection, item, CancellationToken.None);
            await pocoTable.ExecuteDelete(connection, item, CancellationToken.None);

            var reader = connection.GetTransformReader(pocoTable.Table);

            var moreRecords = await reader.ReadAsync();
            Assert.False(moreRecords);
        }

    }

    public class SamplePocoClass
    {
        public SamplePocoClass()
        {
        }

        public SamplePocoClass(string stringColumn, int intColumn, DateTime theDate)
        {
            StringColumn = stringColumn;
            IntColumn = intColumn;
            TheDate = theDate;
        }

        [PocoColumn(DeltaType = TableColumn.EDeltaType.DbAutoIncrement)]
        public long Incremental { get; set; }

        [PocoColumn(DeltaType = TableColumn.EDeltaType.NaturalKey)]
        public string StringColumn { get; set; }

        public int IntColumn { get; set; }
        
        // this uses the field attribute which will read the incoming field "DateColumn" rather then "TheDate"
        [PocoColumn("DateColumn")]
        public DateTime TheDate { get; set; }
    }
}