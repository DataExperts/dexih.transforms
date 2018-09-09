using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.transforms.Transforms
{
    public class TransformAttribute: Attribute
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum ETransformType
        {
            Mapping, Filter, Sort, Group, Aggregate, Series, Join, Rows, Lookup, Validation, Delta, Concatenate, Profile
        }
        
        public ETransformType TransformType { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        
    }
}