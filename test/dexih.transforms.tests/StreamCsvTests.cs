using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.File;
using dexih.transforms.Mapping;
using Xunit;

namespace dexih.transforms.tests
{
    public class StreamCsvTests
    {
        [Fact]
        public async Task StreamCsv_CheckHeaders_Test()
        {
            var testData = Helpers.CreateSortedTestData();
            await testData.Open();
            
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
            await testData.Open();

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

        // written to check if streamcsv was causing memory issue (note: it isn't).
        // [Fact]
        // public async Task StreamCsv_LargeFile_Test()
        // {
        //     var testData = new ReaderRowCreator();
        //     testData.InitializeRowCreator(0, Int32.MaxValue, 1);
        //     
        //     var connection = new ConnectionMemory();
        //     Transform transform = testData;
        //     
        //     transform = new TransformMapping(transform, new Mappings(true)
        //     {
        //         new MapColumn("Hi there", new TableColumn("hi"))
        //     });
        //     
        //     transform = new ReaderConvertDataTypes(connection, transform);
        //     
        //     await transform.Open();
        //     
        //     var stream = new StreamCsv(transform);
        //     var fileStream = System.IO.File.Create("test.txt");
        //
        //     var stopTask = false;
        //
        //     var copyTask = stream.CopyToAsync(fileStream);
        //     var memoryTask = Task.Run(async () =>
        //     {
        //         while (!stopTask)
        //         {
        //             Debug.WriteLine("Memory usage: " + GC.GetTotalMemory(true));
        //             await Task.Delay(1000);
        //         }
        //     });
        //
        //     await copyTask;
        //     stopTask = true;
        //     await memoryTask;
        // }
    }
}
