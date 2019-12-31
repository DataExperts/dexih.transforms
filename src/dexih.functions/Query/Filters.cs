using System.Collections.Generic;
using System.Net.Sockets;
using Dexih.Utils.DataType;

namespace dexih.functions.Query
{
    public class Filters: List<Filter>
    {
        public Filters() {}

        public Filters(IEnumerable<Filter> filters)
        {
            if (filters == null) return;

            base.AddRange(filters);
        }

        public Filters(string columnName, object value)
        {
            Add(new Filter(columnName, value));
        }
        
        public Filters(string columnName, ECompare compare, object value)
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
        
        public Filters(params (string columnName, ECompare compare, object value)[] filters)
        {
            foreach (var filter in filters)
            {
                Add(new Filter(filter.columnName, filter.compare, filter.value));
            }
        }

        public new void Add(Filter filter)
        {
            if (!FilterExists(filter))
            {
                base.Add(filter);
            }
        }

        public bool FilterExists(Filter checkFilter)
        {
            var exists = false;
            foreach (var filter in this)
            {
                if (filter.Column1?.Name == checkFilter.Column1?.Name && filter.Operator == checkFilter.Operator )
                {
                    if(filter.Column2 == null && checkFilter.Column2 == null && checkFilter.Value2 == filter.Value2) return true;
                    if(filter.Column2 != null && checkFilter.Column2 != null && filter.Column2.Name == checkFilter.Column2.Name) return true;
                }
                if (filter.Column2?.Name == checkFilter.Column2?.Name && filter.Operator == checkFilter.Operator )
                {
                    if(filter.Column1 == null && checkFilter.Column1 == null && checkFilter.Value1 == filter.Value1) return true;
                    if(filter.Column1 != null && checkFilter.Column1 != null && filter.Column1.Name == checkFilter.Column1.Name) return true;
                }
            }

            return exists;
        }

    }
}