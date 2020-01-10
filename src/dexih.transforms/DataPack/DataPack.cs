using System.Collections.Generic;
using System.Runtime.Serialization;


namespace dexih.transforms
{
    /// <summary>
    /// Used to transmit data previews
    /// </summary>
    [DataContract]
    public class DataPack
    {
        [DataMember(Order = 0)]
        public string Name { get; set; }

        [DataMember(Order = 1)]
        public DataPackColumn[] Columns { get; set; }

        [DataMember(Order = 2)]
        public List<object[]> Data { get; set; } = new List<object[]>();
        
        [DataMember(Order = 3)]
        public TransformProperties TransformProperties { get; set; }
    }
}