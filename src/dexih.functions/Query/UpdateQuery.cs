using System.Collections.Generic;

namespace dexih.functions.Query
{
    public class UpdateQuery
    {
        public UpdateQuery(string table, List<QueryColumn> updateColumns, List<Filter> filters)
        {
            Table = table;
            UpdateColumns = updateColumns;
            Filters = filters;
        }

        public UpdateQuery()
        {
            UpdateColumns = new List<QueryColumn>();
            Filters = new List<Filter>();
        }

        public List<QueryColumn> UpdateColumns { get; set; }
        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
    }
}