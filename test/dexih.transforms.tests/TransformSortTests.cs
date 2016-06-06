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
        public void RunSingleColumnsSorts()
        {
            RunSingleColumnSort("StringColumn", Sort.EDirection.Ascending, "IntColumn");
            RunSingleColumnSort("StringColumn", Sort.EDirection.Descending, "SortColumn");
            RunSingleColumnSort("IntColumn", Sort.EDirection.Ascending, "IntColumn");
            RunSingleColumnSort("IntColumn", Sort.EDirection.Descending, "SortColumn");
            RunSingleColumnSort("DecimalColumn", Sort.EDirection.Ascending, "IntColumn");
            RunSingleColumnSort("DecimalColumn", Sort.EDirection.Descending, "SortColumn");
            RunSingleColumnSort("DateColumn", Sort.EDirection.Ascending, "IntColumn");
            RunSingleColumnSort("DateColumn", Sort.EDirection.Descending, "SortColumn");
        }

        public void RunSingleColumnSort(string column, Sort.EDirection direction, string checkColumn)
        {
            dexih.transforms.SourceTable Source = Helpers.CreateUnSortedTestData();
            TransformSort TransformSort = new TransformSort();

            TransformSort.SetSortFields(new List<Sort> { new Sort() { Column = column, Direction = direction } });
            TransformSort.SetInTransform(Source);
            int SortCount = 1;

            Assert.Equal(6, TransformSort.FieldCount);

            while (TransformSort.Read() == true)
            {
                Assert.Equal(SortCount, TransformSort[checkColumn]);
                SortCount++;
            }
        }

        [Fact]
        public void RunDoubleColumnSort()
        {
            dexih.transforms.SourceTable Source = Helpers.CreateUnSortedTestData();
            TransformSort TransformSort = new TransformSort();

            TransformSort.SetSortFields(new List<Sort> { new Sort("GroupColumn"), new Sort("IntColumn") } );
            TransformSort.SetInTransform(Source);

            Assert.Equal(6, TransformSort.FieldCount);

            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 2);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 4);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 6);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 8);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 10);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 1);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 3);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 5);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 7);
            Assert.True(TransformSort.Read());
            Assert.True((int)TransformSort["IntColumn"] == 9);

        }

        [Fact]
        public void SortLargeTable()
        {
            dexih.transforms.SourceTable Source = Helpers.CreateLargeTable(100000);
            TransformSort TransformSort = new TransformSort();

            TransformSort.SetSortFields(new List<Sort> { new Sort() { Column = "random", Direction = Sort.EDirection.Ascending} });
            TransformSort.SetInTransform(Source);

            string previousValue = "";
            while (TransformSort.Read() == true)
            {
                string value = (string)TransformSort["random"];
                Assert.True(String.Compare( previousValue, value) <= 0 );
            }
        }
    }
}
