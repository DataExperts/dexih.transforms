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
        public async Task DataReaderAdapterAdapter_Tests()
        {
            ReaderMemory Table = Helpers.CreateSortedTestData();

            Assert.Equal(5, Table.FieldCount);

            int count = 0;
            while (await Table.ReadAsync() == true)
            {
                count = count + 1;
                Assert.Equal(Table[1], count);
                Assert.Equal(Table["IntColumn"], count);
            }

            Assert.Equal(10, count);
        }
    }
}
