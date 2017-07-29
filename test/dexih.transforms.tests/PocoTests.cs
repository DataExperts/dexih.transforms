using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
                    Assert.Equal((DateTime) Convert.ToDateTime("2015-01-" + count.ToString()), item.TheDate);
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
                    Assert.Equal((DateTime) Convert.ToDateTime("2015-01-" + count.ToString()), item.TheDate);
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
                Assert.Equal((DateTime)Convert.ToDateTime("2015-01-" + count.ToString()), item.TheDate);
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
                new SamplePocoClass("column3", 3, new DateTime(2000, 01, 04)),
            };
            
            var pocoReader = new ReaderPoco<SamplePocoClass>(items);

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

        public string StringColumn { get; set; }
        public int IntColumn { get; set; }
        
        // this uses the field attribute which will read the incoming field "DateColumn" rather then "TheDate"
        [Field("DateColumn")]
        public DateTime TheDate { get; set; }
    }
}