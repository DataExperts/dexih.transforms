using System.Collections.Generic;
using System.Net.Sockets;

namespace dexih.functions.Query
{

    public class QueryColumns : List<QueryColumn>
    {
        public QueryColumns() {}
        public QueryColumns(IEnumerable<QueryColumn> queryColumns)
        {
            if (queryColumns == null) return;

            AddRange(queryColumns);
        }
        
        public QueryColumns(string column, object value)
        {
            Add(new QueryColumn(column, value));
        }

        public QueryColumns(params (string column, object value)[] queryColumns)
        {
            foreach (var queryColumn in queryColumns)
            {
                Add(new QueryColumn(queryColumn.column, queryColumn.value));
            }
        }

        public void Add(string columnName, object value)
        {
            base.Add(new QueryColumn(columnName, value));
        }
        
        public void Add(TableColumn column, object value)
        {
            base.Add(new QueryColumn(column, value));
        }
    }
}