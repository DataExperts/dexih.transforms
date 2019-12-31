using MessagePack;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class UpdateQuery
    {
        public UpdateQuery(IEnumerable<QueryColumn> updateColumns, IEnumerable<Filter> filters)
        {
            UpdateColumns = new QueryColumns(updateColumns);
            Filters = new Filters(filters);
        }

        public UpdateQuery()
        {
            UpdateColumns = new QueryColumns();
            Filters = new Filters();
        }

        public UpdateQuery(string updateColumn, object updateValue, string filterColumn = null, object filterValue = null)
        {
            UpdateColumns = new QueryColumns(updateColumn, updateValue);

            if (filterColumn != null)
            {
                Filters = new Filters(filterColumn, filterValue);
            }
        }

        [Key(0)]
        public QueryColumns UpdateColumns { get; set; }

        [Key(1)]
        public Filters Filters { get; set; }
    }
}