using Xunit;

namespace dexih.functions.tests
{
    public class ExtensionsTests
    {
        [Fact]
        void UnSortedCompare()
        {
            var columns1 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };
            
            var columns2 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };

            Assert.True(columns1.UnsortedSequenceEquals(columns2));
            
            var columns3 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
                new TableColumn("col2"),
            };
            
            Assert.True(columns1.UnsortedSequenceEquals(columns3));

            var columns4 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
                new TableColumn("col4"),
            };
            
            Assert.False(columns1.UnsortedSequenceEquals(columns4));

        }
        
        [Fact]
        void UnSortedPropertyCompare()
        {
            var columns1 = new TableColumns()
            {
                new TableColumn("col1", EDeltaType.Url),
                new TableColumn("col2", EDeltaType.Url),
                new TableColumn("col3", EDeltaType.Url),
            };
            
            var columns2 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };

            Assert.False(columns1.UnsortedSequenceEquals(columns2));
            Assert.True(columns1.UnsortedSequenceEquals(columns2, a => a.Name));
            
            var columns3 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
                new TableColumn("col2"),
            };
            
            Assert.False(columns1.UnsortedSequenceEquals(columns3));
            Assert.True(columns1.UnsortedSequenceEquals(columns3, a => a.Name));

            var columns4 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
                new TableColumn("col4"),
            };
            
            Assert.False(columns1.UnsortedSequenceEquals(columns4));
            Assert.False(columns1.UnsortedSequenceEquals(columns4, a => a.Name));

        }
        
        [Fact]
        void SequenceContains()
        {
            var columns1 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };
            
            var columns2 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };

            Assert.True(columns1.SequenceContains(columns2));
            
            var columns3 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
                new TableColumn("col2"),
            };
            
            Assert.True(columns1.SequenceContains(columns3));

            var columns4 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
            };
            
            Assert.True(columns1.SequenceContains(columns4));

            var columns5 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col4"),
            };
            
            Assert.False(columns1.SequenceContains(columns5));

        }
        
        [Fact]
        void SequenceContainsProperty()
        {
            var columns1 = new TableColumns()
            {
                new TableColumn("col1", EDeltaType.Url),
                new TableColumn("col2", EDeltaType.Url),
                new TableColumn("col3", EDeltaType.Url),
            };
            
            var columns2 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };

            Assert.True(columns1.SequenceContains(columns2, c => c.Name));
            Assert.False(columns1.SequenceContains(columns2));
            
            var columns3 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
                new TableColumn("col2"),
            };
            
            Assert.True(columns1.SequenceContains(columns3, c => c.Name));
            Assert.False(columns1.SequenceContains(columns3));

            var columns4 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col1"),
            };
            
            Assert.True(columns1.SequenceContains(columns4, c => c.Name));
            Assert.False(columns1.SequenceContains(columns4));

            var columns5 = new TableColumns()
            {
                new TableColumn("col3"),
                new TableColumn("col4"),
            };
            
            Assert.False(columns1.SequenceContains(columns5, c => c.Name));
            Assert.False(columns1.SequenceContains(columns5));

        }

        [Fact]
        void AddIfNotExists()
        {
            var columns1 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };
            
            var columns2 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };
            
            columns1.AddIfNotExists(columns2);
            
            Assert.Equal(3, columns1.Count);
            
            var columns3 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col4"),
            };

            columns1.AddIfNotExists(columns3);

            Assert.Equal(4, columns1.Count);
        }
        
        [Fact]
        void AddIfNotExistsProperty()
        {
            var columns1 = new TableColumns()
            {
                new TableColumn("col1", EDeltaType.Error),
                new TableColumn("col2", EDeltaType.Error),
                new TableColumn("col3", EDeltaType.Error),
            };
            
            var columns2 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3"),
            };
            
            columns1.AddIfNotExists(columns2, c => c.Name);
            
            Assert.Equal(3, columns1.Count);
            
            var columns3 = new TableColumns()
            {
                new TableColumn("col1"),
                new TableColumn("col4"),
            };

            columns1.AddIfNotExists(columns3, c => c.Name);

            Assert.Equal(4, columns1.Count);
        }
    }
}