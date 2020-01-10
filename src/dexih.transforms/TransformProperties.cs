using System.Collections.Generic;
using System.Runtime.Serialization;
using dexih.functions.Query;


namespace dexih.transforms
{
    [DataContract]
    public class TransformProperties
    {
        [DataMember(Order = 0)]
        public string Name { get; set; }

        [DataMember(Order = 1)]
        public ETransformType TransformType { get; set; }

        [DataMember(Order = 2)]
        public string TransformName { get; set; }

        [DataMember(Order = 3)]
        public SelectQuery SelectQuery { get; set; }

        [DataMember(Order = 4)]
        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();

        [DataMember(Order = 5)]
        public long Rows { get; set; }

        [DataMember(Order = 6)]
        public double Seconds { get; set; }

        [DataMember(Order = 7)]
        public TransformProperties PrimaryProperties { get; set; }

        [DataMember(Order = 8)]
        public TransformProperties ReferenceProperties { get; set; }
    }
}