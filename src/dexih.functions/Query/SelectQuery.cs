using System.Collections.Generic;

namespace dexih.functions.Query
{
    public class SelectQuery
    {
        public SelectQuery()
        {
            Columns = new List<SelectColumn>();
            Filters = new List<Filter>();
            Sorts = new List<Sort>();
            Groups = new List<TableColumn>();
            Rows = -1; //-1 means show all rows.
        }

        public List<SelectColumn> Columns { get; set; }
        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
        public List<Sort> Sorts { get; set; }
        public List<TableColumn> Groups { get; set; }
        public int Rows { get; set; }

    }
}