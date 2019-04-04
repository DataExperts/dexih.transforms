using System.Threading.Tasks;
using dexih.transforms;
using Xunit;

namespace dexih.connections.test
{
    public class ParentChildNavigate
    {
        [Theory]
        [InlineData(1000)]
        public async Task NavigateTest(int rows)
        {
            var reader = DataSets.CreateParentChildReader(rows);
            await reader.Open();

            var parentCount = 0;
            var childCount = 0;
            var grandChildCount = 0;
            
            
            while (await reader.ReadAsync())
            {
                parentCount++;

                var childReader = (Transform) reader["children"];
                await childReader.Open();
                
                while (await childReader.ReadAsync())
                {
                    childCount++;

                    var grandChildReader = (Transform) childReader["grandChildren"];
                    await grandChildReader.Open();
                    
                    while (await grandChildReader.ReadAsync())
                    {
                        grandChildCount++;
                    }
                }
            }

            Assert.Equal(rows, parentCount);
            Assert.Equal(rows, childCount);
            Assert.Equal(rows, grandChildCount);
        }
    }
}