using System;
using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using MessagePack;

namespace dexih.transforms.Transforms
{
    [MessagePackObject]
    [ProtoInherit(1000)]
    [MessagePack.Union(0, typeof(TransformReference))]
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