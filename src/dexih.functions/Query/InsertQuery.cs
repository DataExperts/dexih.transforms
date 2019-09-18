using MessagePack;
using System;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class InsertQuery
    {
        public InsertQuery()
        {
            InsertColumns = new List<QueryColumn>();
        }

        public InsertQuery(List<QueryColumn> insertColumns)
        {
            InsertColumns = insertColumns;
        }

        [Key(0)]
        public List<QueryColumn> InsertColumns { get; set; }
    }
}