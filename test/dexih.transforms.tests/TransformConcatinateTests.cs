using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.transforms.tests
{
    public class TransformConcatinateTests
    {
        private readonly ITestOutputHelper output;

        public TransformConcatinateTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Concatinate()
        {
            var reader1 = Helpers.CreateSortedTestData();
            var reader2 = Helpers.CreateSortedTestData();

            var concatinateTransform = new TransformConcatenate(reader1, reader2);

            Assert.Equal(5, concatinateTransform.FieldCount);

            var count = 0;
            while(await concatinateTransform.ReadAsync())
            {
                count++;
            }
            Assert.Equal(20, count);

        }
    }
}
