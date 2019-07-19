using dexih.functions.Query;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.functions.tests
{
    public class selectQuery
    {
        /// <summary>
        /// Tests the selectquery equality and hashcode functions are functioning
        /// </summary>
        [Fact]
        void SelectQuery_Equality()
        {
            var select1 = new SelectQuery();
            var select2 = new SelectQuery();
            
            Assert.Equal(select1, select2);
            Assert.Equal(select1.GetHashCode(), select2.GetHashCode());

            select1.Rows = 100;
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());

            select2.Rows = 100;
            select1.Columns.Add(new SelectColumn("test1", SelectColumn.EAggregate.Sum));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());

            select2.Columns.Add(new SelectColumn("test2", SelectColumn.EAggregate.Sum));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());

            select2.Columns[0].Column.Name = "test1";
            select2.Columns[0].Column.LogicalName = "test1";
            Assert.Equal(select1, select2);
            Assert.Equal(select1.GetHashCode(), select2.GetHashCode());
            
            select1.Filters.Add(new Filter(new TableColumn("filter1"), ECompare.IsEqual, new TableColumn("filter2") ));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());

            select2.Filters.Add(new Filter(new TableColumn("filter1"), ECompare.IsEqual, new TableColumn("filter3") ));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());

            select2.Filters.Clear();
            select2.Filters.Add(new Filter(new TableColumn("filter1"), ECompare.IsEqual, new TableColumn("filter2") ));
            Assert.Equal(select1, select2);
            Assert.Equal(select1.GetHashCode(), select2.GetHashCode());
            
            select1.Groups.Add(new TableColumn("group1"));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());
            
            select2.Groups.Add(new TableColumn("group2"));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());
            
            select2.Groups.Clear();
            select2.Groups.Add(new TableColumn("group1"));
            Assert.Equal(select1, select2);
            Assert.Equal(select1.GetHashCode(), select2.GetHashCode());
            
            select1.Sorts.Add(new Sort("sort1", Sort.EDirection.Ascending));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());
            
            select2.Sorts.Add(new Sort("sort1", Sort.EDirection.Descending));
            Assert.NotEqual(select1, select2);
            Assert.NotEqual(select1.GetHashCode(), select2.GetHashCode());
            
            select2.Sorts.Clear();
            select2.Sorts.Add(new Sort("sort1", Sort.EDirection.Ascending));
            Assert.Equal(select1, select2);
            Assert.Equal(select1.GetHashCode(), select2.GetHashCode());
            
        }
    }
}