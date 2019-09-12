using ProtoBuf;
using System;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [ProtoContract]
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

        [ProtoMember(1)]
        public List<QueryColumn> InsertColumns { get; set; }
    }
}