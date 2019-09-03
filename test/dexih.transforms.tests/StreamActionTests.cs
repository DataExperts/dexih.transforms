using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;

namespace dexih.transforms.tests
{
    public class StreamActionTests
    {
        [Fact]
        void StreamAction_Test()
        {
            var stream = new StreamAction<int[]>(() =>
            {
                var values = new[] {1, 2, 3, 4, 5};
                return values;
            });

            var serializer = new JsonSerializer();

            using (var sr = new StreamReader(stream))
            using (var jsonTextReader = new JsonTextReader(sr))
            {
                var value = serializer.Deserialize<int[]>(jsonTextReader);
                
                Assert.Equal(1, value[0]);
                Assert.Equal(5, value.Length);
            }
        }
        
        [Fact]
        void StreamAsyncAction_Test()
        {
            var stream = new StreamAsyncAction<int[]>(() =>
            {
                var values = new[] {1, 2, 3, 4, 5};
                return Task.FromResult(values);
            });

            var serializer = new JsonSerializer();

            using (var sr = new StreamReader(stream))
            using (var jsonTextReader = new JsonTextReader(sr))
            {
                var value = serializer.Deserialize<int[]>(jsonTextReader);
                
                Assert.Equal(1, value[0]);
                Assert.Equal(5, value.Length);
            }
        }
    }
}