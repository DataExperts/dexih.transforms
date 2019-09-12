using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class lookup
    {
        [Fact]
        public async Task Lookup_With_PreloadCache()
        {
            var testTransform = Helpers.CreateSortedTestData();

            var query = new SelectQuery
            {
                Filters = new List<Filter> { new Filter("StringColumn", ECompare.IsEqual, "value04") }
            };
            var row = await testTransform.Lookup(query, EDuplicateStrategy.Abend, CancellationToken.None);
            Assert.True((string)row.First()[0] == "value04", "Correct row not found");
        }
    }
}
