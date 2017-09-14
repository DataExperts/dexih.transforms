using dexih.functions;
using dexih.functions.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformSortTests
    {
        [Fact]
        public async Task RunSingleColumnsSorts()
        {
            await RunSingleColumnSort("StringColumn", Sort.EDirection.Ascending, "IntColumn");
            await RunSingleColumnSort("StringColumn", Sort.EDirection.Descending, "SortColumn");
            await RunSingleColumnSort("IntColumn", Sort.EDirection.Ascending, "IntColumn");
            await RunSingleColumnSort("IntColumn", Sort.EDirection.Descending, "SortColumn");
            await RunSingleColumnSort("DecimalColumn", Sort.EDirection.Ascending, "IntColumn");
            await RunSingleColumnSort("DecimalColumn", Sort.EDirection.Descending, "SortColumn");
            await RunSingleColumnSort("DateColumn", Sort.EDirection.Ascending, "IntColumn");
            await RunSingleColumnSort("DateColumn", Sort.EDirection.Descending, "SortColumn");
        }

        public async Task RunSingleColumnSort(string column, Sort.EDirection direction, string checkColumn)
        {
            dexih.transforms.ReaderMemory Source = Helpers.CreateUnSortedTestData();
            TransformSort TransformSort = new TransformSort(Source, new List<Sort> { new Sort(column, direction ) });
            int SortCount = 1;

            Assert.Equal(6, TransformSort.FieldCount);

            while (await TransformSort.ReadAsync() == true)
            {
                Assert.Equal(SortCount, TransformSort[checkColumn]);
                SortCount++;
            }
        }

        [Fact]
        public async Task  RunDoubleColumnSort()
        {
            dexih.transforms.ReaderMemory Source = Helpers.CreateUnSortedTestData();
            TransformSort TransformSort = new TransformSort(Source, new List <Sort> { new Sort("GroupColumn"), new Sort("IntColumn") });

            Assert.Equal(6, TransformSort.FieldCount);

            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 2);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 4);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 6);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 8);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 10);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 1);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 3);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 5);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 7);
            Assert.True(await TransformSort.ReadAsync());
            Assert.True((int)TransformSort["IntColumn"] == 9);

        }

        [Fact]
        public async Task SortLargeTable()
        {
            dexih.transforms.ReaderMemory Source = Helpers.CreateLargeTable(100000);
            TransformSort TransformSort = new TransformSort(Source, new List <Sort> { new Sort("random", Sort.EDirection.Ascending) });
            TransformSort.SetInTransform(Source);

            string previousValue = "";
            while (await TransformSort.ReadAsync() == true)
            {
                string value = (string)TransformSort["random"];
                Assert.True(String.Compare( previousValue, value) <= 0 );
            }
        }
    }
}
