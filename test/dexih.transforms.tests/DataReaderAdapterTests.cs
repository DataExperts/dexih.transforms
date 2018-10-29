using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class DataReaderAdapterTests
    {
        [Fact]
        public async Task DataReaderAdapterAdapter_Tests()
        {
            var Table = Helpers.CreateSortedTestData();

            Assert.Equal(6, Table.FieldCount);

            var count = 0;
            while (await Table.ReadAsync())
            {
                count = count + 1;
                Assert.Equal(Table[1], count);
                Assert.Equal(Table["IntColumn"], count);
            }

            Assert.Equal(10, count);
        }
    }
}
