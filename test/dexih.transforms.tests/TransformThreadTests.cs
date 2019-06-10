using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformThreadTests
    {
        
        [Fact]
        public async Task RunTwoSimultaneousReads()
        {
            var source = Helpers.CreateSortedTestData();
            await source.Open();

            var reader1 = source.GetThread();
            var reader2 = source.GetThread();

            await reader1.Open();
            await reader2.Open();
            
            for (var i = 1; i <= 10; i++)
            {
                Assert.True(await reader1.ReadAsync());
                Assert.True(await reader2.ReadAsync());
                
                Assert.Equal(i, reader1["IntColumn"]);
                Assert.Equal(i, reader2["IntColumn"]);
            }

            Assert.False(await reader1.ReadAsync());
            Assert.False(await reader2.ReadAsync());
        }
        
        [Fact]
        public async Task RunTwoSequentialReads()
        {
            var source = Helpers.CreateSortedTestData();

            var reader1 = source.GetThread();
            await reader1.Open();

            for (var i = 1; i <= 10; i++)
            {
                Assert.True(await reader1.ReadAsync());
                Assert.Equal(i, reader1["IntColumn"]);
            }

            Assert.False(await reader1.ReadAsync());
            
            var reader2 = source.GetThread();
            await reader2.Open();
            
            for (var i = 1; i <= 10; i++)
            {
                Assert.True(await reader2.ReadAsync());
                Assert.Equal(i, reader2["IntColumn"]);
            }

            Assert.False(await reader2.ReadAsync());
        }

    }
}