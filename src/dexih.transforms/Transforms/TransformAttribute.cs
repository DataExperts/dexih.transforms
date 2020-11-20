using System;
using System.Runtime.Serialization;


namespace dexih.transforms.Transforms
{
    [DataContract]
    // [Union(0, typeof(TransformReference))]
    public class TransformAttribute: Attribute
    {
        // This is overridden as it causes the json serializer to 
        // Method may only be called on a Type for which Type.IsGenericParameter is true error
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