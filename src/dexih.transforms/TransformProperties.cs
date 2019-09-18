using System.Collections.Generic;
using dexih.functions.Query;
using dexih.transforms.Transforms;
using MessagePack;

namespace dexih.transforms
{
    [MessagePackObject]
    public class TransformProperties
    {
        [Key(0)]
        public string Name { get; set; }

        [Key(1)]
        public ETransformType TransformType { get; set; }

        [Key(2)]
        public string TransformName { get; set; }

        [Key(3)]
        public SelectQuery SelectQuery { get; set; }

        [Key(4)]
        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();

        [Key(5)]
        public long Rows { get; set; }

        [Key(6)]
        public double Seconds { get; set; }

        [Key(7)]
        public TransformProperties PrimaryProperties { get; set; }

        [Key(8)]
        public TransformProperties ReferenceProperties { get; set; }
    }
}