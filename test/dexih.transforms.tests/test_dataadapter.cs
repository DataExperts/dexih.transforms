using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.unittests
{
    public class test_dataadapter
    {
        /// <summary>
        /// This test should take about 450ms
        /// </summary>
        [Fact]
        public void TestDataAdapter()
        {
            object[] row;
            DataTableColumns columns = new DataTableColumns();
            for (int i = 0; i < 10; i++)
                columns.Add("column" + i.ToString(), functions.DataType.ETypeCode.Int32);

            DataTableSimple table = new DataTableSimple("test", columns);

            for (int i = 0; i < 1000000; i++)
            {
                row = new object[10];

                for(int j = 0; j < 10; j++)
                {
                    row[j] = j;
                }

                table.Data.Add(row);
            }

            DataTableAdapter tableAdapter = new DataTableAdapter(table);

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
