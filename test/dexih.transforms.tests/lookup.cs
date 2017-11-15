using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class lookup
    {
        [Fact]
        public async Task Lookup_With_PreloadCache()
        {
            var TestTransform = Helpers.CreateSortedTestData();

            List<Filter> filters = new List<Filter>() { new Filter("StringColumn", Filter.ECompare.IsEqual, "value04") };
            var row = await TestTransform.LookupRow(filters, Transform.EDuplicateStrategy.Abend, CancellationToken.None);

            Assert.True((string)row.First()[0] == "value04", "Correct row not found");
        }
    }
}
