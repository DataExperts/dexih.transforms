using System.Runtime.Serialization;

namespace dexih.functions.Query
{
    [DataContract]
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

        [DataMember(Order = 0)]
        public string Table { get; set; }

        [DataMember(Order = 1)]
        public Filters Filters { get; set; }
    }
}