using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformSortTests
    {
        [Theory]
        [InlineData("StringColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("StringColumn", Sort.EDirection.Descending, "SortColumn")]
        [InlineData("IntColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("IntColumn", Sort.EDirection.Descending, "SortColumn")]
        [InlineData("DecimalColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("DecimalColumn", Sort.EDirection.Descending, "SortColumn")]
        [InlineData("DateColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("DateColumn", Sort.EDirection.Descending, "SortColumn")]
        public async Task RunSingleColumnSort(string column, Sort.EDirection direction, string checkColumn)
        {
            var source = Helpers.CreateUnSortedTestData();
            var transformSort = new TransformSort(source, new Sorts() { new Sort(column, direction ) });
            await transformSort.Open();
            
            var sortCount = 1;

            Assert.Equal(6, transformSort.FieldCount);

            while (await transformSort.ReadAsync())
            {
                Assert.Equal(sortCount, transformSort[checkColumn]);
                sortCount++;
            }
        }
        
        [Theory]
        [InlineData("StringColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("StringColumn", Sort.EDirection.Descending, "SortColumn")]
        [InlineData("IntColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("IntColumn", Sort.EDirection.Descending, "SortColumn")]
        [InlineData("DecimalColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("DecimalColumn", Sort.EDirection.Descending, "SortColumn")]
        [InlineData("DateColumn", Sort.EDirection.Ascending, "IntColumn")]
        [InlineData("DateColumn", Sort.EDirection.Descending, "SortColumn")]
        public async Task RunSingleColumnSort2(string column, Sort.EDirection direction, string checkColumn)
        {
            var source = Helpers.CreateUnSortedTestData();
            var mappings = new Mappings {new MapSort(new TableColumn(column), direction)};
            var transformSort = new TransformSort(source, mappings);
            await transformSort.Open();
            var sortCount = 1;

            Assert.Equal(6, transformSort.FieldCount);

            while (await transformSort.ReadAsync())
            {
                Assert.Equal(sortCount, transformSort[checkColumn]);
                sortCount++;
            }
        }


        [Fact]
        public async Task  RunDoubleColumnSort()
        {
            var source = Helpers.CreateUnSortedTestData();
            var transformSort = new TransformSort(source, new Sorts() { new Sort("GroupColumn"), new Sort("IntColumn") });
            await transformSort.Open();
            
            Assert.Equal(6, transformSort.FieldCount);

            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 2);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 4);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 6);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 8);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 10);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 1);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 3);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 5);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 7);
            Assert.True(await transformSort.ReadAsync());
            Assert.True((int)transformSort["IntColumn"] == 9);

        }

        [Fact]
        public async Task SortLargeTable()
        {
            var source = Helpers.CreateLargeTable(100000);
            var transformSort = new TransformSort(source, new Sorts() { new Sort("random", Sort.EDirection.Ascending) });
            transformSort.SetInTransform(source);
            await transformSort.Open();

            var previousValue = "";
            while (await transformSort.ReadAsync())
            {
                var value = (string)transformSort["random"];
                Assert.True(string.Compare( previousValue, value) <= 0 );
            }
        }
    }
}
