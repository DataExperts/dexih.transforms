using MessagePack;
using System;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [MessagePackObject]
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

        [Key(0)]
        public string Table { get; set; }

        [Key(1)]
        public List<Filter> Filters { get; set; }
    }
}