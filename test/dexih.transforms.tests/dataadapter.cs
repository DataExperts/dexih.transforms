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
        public void TestDataAdapter()
        {
            SourceTable tableAdapter = Helpers.CreateLargeTable(1000000);

            int count = 0;
            while(tableAdapter.Read())
            {
                for (int j = 0; j < 10; j++)
                {
                    Assert.True((int)tableAdapter.GetValue(j) == j);
                }
                count++;
            }

            Assert.True(count == 1000000);
        }
    }
}
