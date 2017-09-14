using System.Collections.Generic;

namespace dexih.functions.Query
{
    public class InsertQuery
    {
        public InsertQuery(string table, List<QueryColumn> insertColumns)
        {
            Table = table;
            InsertColumns = insertColumns;
        }

        public string Table { get; set; }
        public List<QueryColumn> InsertColumns { get; set; }
    }
}