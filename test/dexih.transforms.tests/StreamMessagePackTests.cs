using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace dexih.transforms.tests
{
    public class StreamReaderTests
    {
        private readonly ITestOutputHelper _output;

        public StreamReaderTests(ITestOutputHelper output)
        {
            _output = output;
        }
        
        // [Fact]
        // public async void StreamMessagePack_Small_Test()
        // {
        //     var reader = Helpers.CreateUnSortedTestData();
        //     var stream = new StreamMessagePack("test", reader);
        //     
        //     var protoBufData = new DataPack();
        //     var data = await MessagePackSerializer.DeserializeAsync<DataPack>(stream);
        //     
        //     Assert.Equal(10, data.Data.Count);
        // }
        //
        // [Fact]
        // public async void StreamMessagePack_ParentChild_Test()
        // {
        //     var reader = Helpers.CreateParentChildReader();
        //     await reader.Open();
        //
        //     var timer = Stopwatch.StartNew();
        //     var stream = new StreamMessagePack("test", reader);
        //     
        //     var data = await MessagePackSerializer.DeserializeAsync<DataPack>(stream);
        //
        //     _output.WriteLine($"Time taken: {timer.ElapsedMilliseconds}");
        //     Assert.Equal(4, data.Data.Count);
        //     
        //
        // }
        //
        // [Theory]
        // [InlineData(1000000)] //should run in ~ 250ms
        // public async void StreamMessagePack_ParentChild_Large(int rows)
        // {
        //     var reader = Helpers.CreateLargeTable(rows);
        //     await reader.Open();
        //
        //     var timer = Stopwatch.StartNew();
        //     var stream = new StreamMessagePack("test", reader);
        //     var data = await MessagePackSerializer.DeserializeAsync<DataPack>(stream);
        //     timer.Stop();
        //     
        //     _output.WriteLine($"Time taken: {timer.ElapsedMilliseconds}");
        //     
        //     Assert.Equal(rows, data.Data.Count);
        // }
        
        [Theory]
        [InlineData(1000000)] //should run in ~ 250ms
        public async void StreamJson_ParentChild_Large(int rows)
        {
            var reader = Helpers.CreateLargeTable(rows);
            await reader.Open();

            var timer = Stopwatch.StartNew();
            var stream = new StreamJsonCompact("test", reader);
            var serializer = new JsonSerializer();
            using (var sr = new StreamReader(stream))
            using (var jsonTextReader = new JsonTextReader(sr))
            {
                var data = serializer.Deserialize<DataPack>(jsonTextReader);
                timer.Stop();

                _output.WriteLine($"Time taken: {timer.ElapsedMilliseconds}");

                Assert.Equal(rows, data.Data.Count);
            }
        }
    }
}