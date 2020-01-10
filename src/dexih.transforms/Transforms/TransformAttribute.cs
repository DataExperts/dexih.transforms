using System;
using System.Runtime.Serialization;


namespace dexih.transforms.Transforms
{
    [DataContract]
    // [Union(0, typeof(TransformReference))]
    public class TransformAttribute: Attribute
    {
        [IgnoreDataMember]
        public override object TypeId { get; }

        [DataMember(Order = 0)]
        public ETransformType TransformType { get; set; }

        [DataMember(Order = 1)]
        public string Name { get; set; }

        [DataMember(Order = 2)]
        public string Description { get; set; }
        
    }
}