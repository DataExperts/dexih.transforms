using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformLookupTests
    {

        [Fact]
        public async Task Lookup()
        {
            var source = Helpers.CreateSortedTestData();
            var mappings = new Mappings { new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn")) };
            var transformLookup = new TransformLookup(source, Helpers.CreateUnSortedJoinData(), mappings, EDuplicateStrategy.Abend, EJoinNotFoundStrategy.NullJoin, "Lookup");

            Assert.Equal(9, transformLookup.FieldCount);

            await transformLookup.Open(1, null, CancellationToken.None);

            var pos = 0;
            while (await transformLookup.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformLookup["LookupValue"]);
                else
                    Assert.Null(transformLookup["LookupValue"]); //test the last Lookup which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public async Task LookupAbend()
        {
            var source = Helpers.CreateSortedTestData();
            var mappings = new Mappings { new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformLookup = new TransformLookup(source, Helpers.CreateUnSortedJoinData(), mappings, EDuplicateStrategy.Abend, EJoinNotFoundStrategy.Abend, "Lookup");

            Assert.Equal(9, transformLookup.FieldCount);

            await transformLookup.Open(1, null, CancellationToken.None);

            for (var i = 1; i < 10; i++)
            {
                await transformLookup.ReadAsync();
                Assert.Equal("lookup" + i, transformLookup["LookupValue"]);
            }

            await Assert.ThrowsAsync<TransformException>(async () => { while (await transformLookup.ReadAsync()) ; });
        }

        [Fact]
        public async Task LookupFilterNull()
        {
            var source = Helpers.CreateSortedTestData();
            var mappings = new Mappings { new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn")) };
            var transformLookup = new TransformLookup(source, Helpers.CreateUnSortedJoinData(), mappings, EDuplicateStrategy.Abend, EJoinNotFoundStrategy.Filter, "Lookup");

            Assert.Equal(9, transformLookup.FieldCount);

            await transformLookup.Open(1, null, CancellationToken.None);

            var pos = 0;
            while (await transformLookup.ReadAsync())
            {
                pos++;
                Assert.Equal("lookup" + pos, transformLookup["LookupValue"]);
            }
            Assert.Equal(9, pos);
        }
    }
}
