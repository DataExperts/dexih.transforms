using System.Linq;
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
                new TableColumn("key", EDeltaType.NaturalKey),
                new TableColumn("track", EDeltaType.TrackingField),
                new TableColumn("ignore", EDeltaType.IgnoreField),
            }
        };

        [Fact]
        public void TestSortPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);
            
            var sort = new TransformSort(reader, "key");

            sort.Open();
            
            Assert.Single(reader.SelectQuery.Sorts);
            Assert.Equal("key", reader.SelectQuery.Sorts[0].Column.Name);
            Assert.Equal("key", reader.SelectQuery.Sorts[0].Column.Name);

        }
        
        [Fact]
        public void TestFilterPushdown()
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
            
            Assert.Single(reader.SelectQuery.Filters);
            Assert.Equal(table.Columns[1].Name, reader.SelectQuery.Filters[0].Column1.Name);
            Assert.Equal(5, reader.SelectQuery.Filters[0].Value2);
            Assert.Equal(ECompare.GreaterThan, reader.SelectQuery.Filters[0].Operator);

        }
        
        [Fact]
        public void TestMapPushdown()
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
            
            Assert.Single(reader.SelectQuery.Columns);
            Assert.Equal(table.Columns[1].Name, reader.SelectQuery.Columns[0].Column.Name);
        }
        
        [Fact]
        public void TestMapFilterSortPushdown()
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
            
            Assert.Single(reader.SelectQuery.Columns);
            Assert.Equal(table.Columns[0].Name, reader.SelectQuery.Columns[0].Column.Name);
            
            Assert.Single(reader.SelectQuery.Filters);
            Assert.Equal(table.Columns[0].Name, reader.SelectQuery.Filters[0].Column1.Name);
            Assert.Equal(5, reader.SelectQuery.Filters[0].Value2);
            Assert.Equal(ECompare.GreaterThan, reader.SelectQuery.Filters[0].Operator);
            
            Assert.Single(reader.SelectQuery.Sorts);
            Assert.Equal("key", reader.SelectQuery.Sorts[0].Column.Name);
        }

        [Fact]
        public void TestGroupPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);

            var mappings = new Mappings(false)
            {
                new MapGroup(table.Columns[0]),
                new MapAggregate(table.Columns[1], new TableColumn("sum"), EAggregate.Sum)
            };

            var group = new TransformGroup(reader, mappings);

            group.Open();

            Assert.Single(reader.SelectQuery.Groups);
            Assert.Equal(table[0].Name, reader.SelectQuery.Groups[0].Name);
            Assert.Equal(2, reader.SelectQuery.Columns.Count());
            Assert.Equal(table.Columns[1].Name, reader.SelectQuery.Columns[1].Column.Name);
            
        }
        
        [Fact]
        public void TestGroupFilterHavingPushdown()
        {
            var table = SampleTable;
            
            var connection = new MockConnection();

            var reader = connection.GetTransformReader(table);
            
            var filter = new TransformFilter(reader, new Mappings()
            {
                new MapFilter(table.Columns[2], 5, ECompare.IsEqual)
            });
            
            var sumColumn = new TableColumn("sum");
            
            var mappings = new Mappings(false)
            {
                new MapGroup(table.Columns[0]),
                new MapAggregate(table.Columns[1], sumColumn, EAggregate.Sum)
            };

            var group = new TransformGroup(filter, mappings);
            
            var having = new TransformFilter(group, new Mappings()
            {
                new MapFilter(sumColumn, 10, ECompare.GreaterThan)
            });

            having.Open();

            Assert.Single(reader.SelectQuery.Groups);
            Assert.Equal(table[0].Name, reader.SelectQuery.Groups[0].Name);
            Assert.Equal(2, reader.SelectQuery.Columns.Count());
            Assert.Equal(table.Columns[1].Name, reader.SelectQuery.Columns[1].Column.Name);
            Assert.Single(reader.SelectQuery.Filters);
            Assert.Equal(table.Columns[2].Name, reader.SelectQuery.Filters[0].Column1.Name);
            Assert.Single(reader.SelectQuery.GroupFilters);
            Assert.Equal(sumColumn.Name, reader.SelectQuery.GroupFilters[0].Column1.Name);

        }
    }
}