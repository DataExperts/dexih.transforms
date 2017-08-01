using System.Collections.Generic;

namespace dexih.functions
{
    public class DeleteQuery
    {
        public DeleteQuery(string table, List<Filter> filters)
        {
            Table = table;
            Filters = filters;
        }
        public DeleteQuery()
        {
            Filters = new List<Filter>();
        }

        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
    }
}