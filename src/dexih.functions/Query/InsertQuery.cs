using System;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [Serializable]
    public class InsertQuery
    {
        public InsertQuery(List<QueryColumn> insertColumns)
        {
            InsertColumns = insertColumns;
        }

        public List<QueryColumn> InsertColumns { get; set; }
    }
}