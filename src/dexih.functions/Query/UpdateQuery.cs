using MessagePack;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class UpdateQuery
    {
        public UpdateQuery(List<QueryColumn> updateColumns, List<Filter> filters)
        {
            UpdateColumns = updateColumns;
            Filters = filters;
        }

        public UpdateQuery()
        {
            UpdateColumns = new List<QueryColumn>();
            Filters = new List<Filter>();
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
        public List<QueryColumn> UpdateColumns { get; set; }

        [Key(1)]
        public List<Filter> Filters { get; set; }
    }
}