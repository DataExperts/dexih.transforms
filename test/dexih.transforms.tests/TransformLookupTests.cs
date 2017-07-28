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
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformLookup transformLookup = new TransformLookup(Source, Helpers.CreateUnSortedJoinData(), new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, "Lookup");

            Assert.Equal(8, transformLookup.FieldCount);

            await transformLookup.Open(1, null, CancellationToken.None);

            int pos = 0;
            while (await transformLookup.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformLookup["LookupValue"]);
                else
                    Assert.Equal(null, transformLookup["LookupValue"]); //test the last Lookup which is not found.

            }
            Assert.Equal(10, pos);
        }


    }
}
