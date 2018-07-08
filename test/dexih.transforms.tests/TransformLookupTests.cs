using dexih.transforms;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using dexih.transforms.Exceptions;

namespace dexih.transforms.tests
{
    public class TransformLookupTests
    {

        [Fact]
        public async Task Lookup()
        {
            var source = Helpers.CreateSortedTestData();
            var joinPairs = new List<JoinPair>()
            {
                new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn"))
            }; 
            var transformLookup = new TransformLookup(
                source, 
                Helpers.CreateUnSortedJoinData(), 
                joinPairs, 
                "Lookup");

            Assert.Equal(8, transformLookup.FieldCount);

            await transformLookup.Open(1, null, CancellationToken.None);

            var pos = 0;
            while (await transformLookup.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformLookup["LookupValue"]);
                else
                    Assert.Null(transformLookup["LookupValue"]); //test the last Lookup which is not found.

            }
            Assert.Equal(10, pos);
        }


    }
}
