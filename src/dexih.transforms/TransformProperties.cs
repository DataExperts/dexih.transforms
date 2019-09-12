using System.Collections.Generic;
using dexih.functions.Query;
using dexih.transforms.Transforms;
using ProtoBuf;

namespace dexih.transforms
{
    [ProtoContract]
    public class TransformProperties
    {
        [ProtoMember(1)]
        public string Name { get; set; }

        [ProtoMember(2)]
        public ETransformType TransformType { get; set; }

        [ProtoMember(3)]
        public string TransformName { get; set; }

        [ProtoMember(4)]
        public SelectQuery SelectQuery { get; set; }

        [ProtoMember(5)]
        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();

        [ProtoMember(6)]
        public long Rows { get; set; }

        [ProtoMember(7)]
        public double Seconds { get; set; }

        [ProtoMember(8)]
        public TransformProperties PrimaryProperties { get; set; }

        [ProtoMember(9)]
        public TransformProperties ReferenceProperties { get; set; }
    }
}