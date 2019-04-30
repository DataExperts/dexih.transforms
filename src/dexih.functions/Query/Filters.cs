using System.Collections.Generic;

namespace dexih.functions.Query
{
    public class Filters: List<Filter>
    {
        public Filters(string columnName, object value)
        {
            Add(new Filter(columnName, value));
        }
        
        public Filters(string columnName, Filter.ECompare compare, object value)
        {
            Add(new Filter(columnName, compare, value));
        }

        public Filters(params (string columnName, object value)[] filters)
        {
            foreach (var filter in filters)
            {
                Add(new Filter(filter.columnName, filter.value));
            }
        }
        
        public Filters(params (string columnName, Filter.ECompare compare, object value)[] filters)
        {
            foreach (var filter in filters)
            {
                Add(new Filter(filter.columnName, filter.compare, filter.value));
            }
        }

    }
}