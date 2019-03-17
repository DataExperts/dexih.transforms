using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
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
//            var joinPairs = new List<Join>
//            {
//                new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn"))
//            }; 
//            var transformLookup = new TransformLookup(
//                source, 
//                Helpers.CreateUnSortedJoinData(), 
//                joinPairs, 
//                "Lookup");

            var mappings = new Mappings
            {
                new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))
            };
            
            var transformLookup = new TransformLookup(source, Helpers.CreateUnSortedJoinData(), mappings, "Lookup");
                
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


    }
}
