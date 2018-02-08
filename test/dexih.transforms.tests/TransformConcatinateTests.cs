using System;
using System.Collections.Generic;
using System.Text;
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
            ReaderMemory reader1 = Helpers.CreateSortedTestData();
            ReaderMemory reader2 = Helpers.CreateSortedTestData();

            TransformConcatenate concatinateTransform = new TransformConcatenate(reader1, reader2);

            Assert.Equal(5, concatinateTransform.FieldCount);

            int count = 0;
            while(await concatinateTransform.ReadAsync())
            {
                count++;
            }
            Assert.Equal(20, count);

        }
    }
}
