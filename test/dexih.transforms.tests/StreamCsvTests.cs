using dexih.functions.File;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using dexih.transforms.File;
using Xunit;

namespace dexih.transforms.tests
{
    public class StreamCsvTests
    {
        [Fact]
        public async Task StreamCsv_CheckHeaders_Test()
        {
            var testData = Helpers.CreateSortedTestData();
            var stream = new StreamCsv(testData);

            var handler = new FileHandlerText(testData.CacheTable, new FileConfiguration());
            var columns = (await handler.GetSourceColumns(stream)).ToArray();

            Assert.True(testData.FieldCount > 0);

            for(var i = 0; i<testData.CacheTable.Columns.Count; i++)
            {
                Assert.Equal(testData.CacheTable.Columns[i].Name, columns[i].Name);
            }


            stream = new StreamCsv(testData);

        }

        [Fact]
        public async Task StreamCsv_CheckData_Test()
        {
            var testData = Helpers.CreateSortedTestData();

            // convert to csv stream.
            var stream = new StreamCsv(testData);

            // parse converted csv stream
            var fileConfig = new FileConfiguration
            {
                Quote = '"'
            };

            var handler = new FileHandlerText(testData.CacheTable, fileConfig);
            await handler.SetStream(stream, null);

            Assert.True(testData.FieldCount > 0);

            var count = 0;
            var row = await handler.GetRow();
            while (row != null)
            {
                count++;
                Assert.Equal("value" + count.ToString().PadLeft(2, '0'), row[testData.GetOrdinal("StringColumn")]);
                Assert.Equal(new[] { 1, 1 }, row[testData.GetOrdinal("ArrayColumn")]);
                row = await handler.GetRow();
            }

            Assert.Equal(10, count);

            stream = new StreamCsv(testData);

        }

    }
}
