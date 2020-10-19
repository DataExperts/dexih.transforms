using System.Collections.Generic;
using System.Runtime.Serialization;

namespace dexih.functions
{
    [DataContract]
    public class TableIndex
    {
        [DataMember(Order = 0)]
        public string Name { get; set; }
        
        [DataMember(Order = 1)]
        public List<TableIndexColumn> Columns { get; set; }
    }
}