using ProtoBuf;
using System;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [ProtoContract]
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

        [ProtoMember(1)]
        public string Table { get; set; }

        [ProtoMember(2)]
        public List<Filter> Filters { get; set; }
    }
}