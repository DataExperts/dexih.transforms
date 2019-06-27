using System;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [Serializable]
    public class QueryColumns : List<QueryColumn>
    {
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
    }
}