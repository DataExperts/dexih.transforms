using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.transforms.Transforms
{
    [ProtoContract]
    [ProtoInclude(100, typeof(TransformReference))]
    public class TransformAttribute: Attribute
    {
        [ProtoMember(1)]
        public ETransformType TransformType { get; set; }

        [ProtoMember(2)]
        public string Name { get; set; }

        [ProtoMember(3)]
        public string Description { get; set; }
        
    }
}