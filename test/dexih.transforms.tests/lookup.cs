using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class lookup
    {
        [Fact]
        public void Lookup_With_PreloadCache()
        {
            var TestTransform = Helpers.CreateSortedTestData();

            List<Filter> filters = new List<Filter>() { new Filter("StringColumn", Filter.ECompare.IsEqual, "value04") };
            var row = TestTransform.LookupRow(filters).Result;

            Assert.True(row.Success, "Row was not found");
            Assert.True((string)row.Value[0] == "value04", "Correct row not found");
        }
    }
}
