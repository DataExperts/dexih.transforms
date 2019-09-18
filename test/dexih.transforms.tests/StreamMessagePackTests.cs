using System.IO;
using MessagePack;
using Xunit;

namespace dexih.transforms.tests
{
    public class StreamProtoBufTests
    {
        [Fact]
        public async void StreamProtoBuf_Test()
        {
            var reader = Helpers.CreateUnSortedTestData();
            var stream = new StreamProtoBuf("test", reader);
            
            var protoBufData = new DataPack();
            var data = await MessagePackSerializer.DeserializeAsync<DataPack>(stream);
            
            Assert.Equal(10, data.Rows.Count);
        }

    }
}