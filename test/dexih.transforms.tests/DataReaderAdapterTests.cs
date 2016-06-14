using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class DataReaderAdapterTests
    {
        [Fact]
        public void DataReaderAdapterAdapter_Tests()
        {
            ReaderMemory Table = Helpers.CreateSortedTestData();

            Assert.Equal(Table.FieldCount, 5);

            int count = 0;
            while (Table.Read() == true)
            {
                count = count + 1;
                Assert.Equal(Table[1], count);
                Assert.Equal(Table["IntColumn"], count);
            }

            Assert.Equal(10, count);
        }
    }
}
