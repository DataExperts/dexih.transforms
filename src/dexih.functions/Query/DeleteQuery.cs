using MessagePack;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class DeleteQuery
    {
        public DeleteQuery(string table, Filters filters)
        {
            Table = table;
            Filters = filters;
        }
        public DeleteQuery()
        {
            Filters = new Filters();
        }

        [Key(0)]
        public string Table { get; set; }

        [Key(1)]
        public Filters Filters { get; set; }
    }
}