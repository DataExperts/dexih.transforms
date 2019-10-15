using System;
using dexih.functions;


using MessagePack;

namespace dexih.transforms.Transforms
{
    [MessagePackObject]
    [Union(0, typeof(TransformReference))]
    public class TransformAttribute: Attribute
    {
        [IgnoreMember]
        public override object TypeId { get; }

        [Key(0)]
        public ETransformType TransformType { get; set; }

        [Key(1)]
        public string Name { get; set; }

        [Key(2)]
        public string Description { get; set; }
        
    }
}