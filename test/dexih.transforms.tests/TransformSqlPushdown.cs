using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformSqlPushdown
    {
        public Table SampleTable = new Table()
        {
            Name = "Sample",
            Columns = new TableColumns()
            {
                new TableColumn("key", TableColumn.EDeltaType.NaturalKey),
                new TableColumn("track", TableColumn.EDeltaType.TrackingField),
                new TableColumn("ignore", TableColumn.EDeltaType.IgnoreField),
            }
        };

        [Fact]
        public async Task TestSortPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);
            
            var sort = new TransformSort(reader, "key");

            sort.Open();
            
            Assert.Equal(1, reader.SelectQuery.Sorts.Count);
            Assert.Equal("key", reader.SelectQuery.Sorts[0].Column.Name);

        }
        
        [Fact]
        public async Task TestFilterPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);

            var mappings = new Mappings
            {
                new MapFilter(table.Columns[1], 5, ECompare.GreaterThan)
            };

            var filter = new TransformFilter(reader, mappings);

            filter.Open();
            
            Assert.Equal(1, reader.SelectQuery.Filters.Count);
            Assert.Equal(table.Columns[1].Name, reader.SelectQuery.Filters[0].Column1.Name);
            Assert.Equal(5, reader.SelectQuery.Filters[0].Value2);
            Assert.Equal(ECompare.GreaterThan, reader.SelectQuery.Filters[0].Operator);

        }
        
        [Fact]
        public async Task TestMapPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);

            var mappings = new Mappings(false)
            {
                new MapColumn(table.Columns[1])
            };

            var mapping = new TransformMapping(reader, mappings);

            mapping.Open();
            
            Assert.Equal(1, reader.SelectQuery.Columns.Count);
            Assert.Equal(table.Columns[1].Name, reader.SelectQuery.Columns[0].Column.Name);
        }
        
        [Fact]
        public async Task TestMapFilterSortPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);

            var mappings = new Mappings(false)
            {
                new MapColumn(table.Columns[0])
            };

            var mapping = new TransformMapping(reader, mappings);
            
            var filters = new Mappings
            {
                new MapFilter(table.Columns[0], 5, ECompare.GreaterThan)
            };

            var filter = new TransformFilter(mapping, filters);
            var sort = new TransformSort(filter, "key");

            sort.Open();
            
            Assert.Equal(1, reader.SelectQuery.Columns.Count);
            Assert.Equal(table.Columns[0].Name, reader.SelectQuery.Columns[0].Column.Name);
            
            Assert.Equal(1, reader.SelectQuery.Filters.Count);
            Assert.Equal(table.Columns[0].Name, reader.SelectQuery.Filters[0].Column1.Name);
            Assert.Equal(5, reader.SelectQuery.Filters[0].Value2);
            Assert.Equal(ECompare.GreaterThan, reader.SelectQuery.Filters[0].Operator);
            
            Assert.Equal(1, reader.SelectQuery.Sorts.Count);
            Assert.Equal("key", reader.SelectQuery.Sorts[0].Column.Name);
        }
    }
}