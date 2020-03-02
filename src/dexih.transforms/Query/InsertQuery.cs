
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace dexih.functions.Query
{
    [DataContract]
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

        [DataMember(Order = 0)]
        public List<QueryColumn> InsertColumns { get; set; }
    }
}