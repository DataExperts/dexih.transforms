using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class dataadapter
    {
        /// <summary>
        /// This test should take about 450ms
        /// </summary>
        [Fact]
        public async Task TestDataAdapter()
        {
            var tableAdapter = Helpers.CreateLargeTable(100000);

            var count = 0;
            while(await tableAdapter.ReadAsync())
            {
                for (var j = 0; j < 10; j++)
                {
                    Assert.Equal( j, tableAdapter.GetValue(j));
                }
                count++;
            }

            Assert.Equal(100000, count);
        }
    }
}
