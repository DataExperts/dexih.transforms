using System.Collections.Generic;
using dexih.functions.Query;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    public class TransformProperties
    {
        public string Name { get; set; }
        
        public TransformAttribute.ETransformType TransformType { get; set; }
        public string TransformName { get; set; }
        public SelectQuery SelectQuery { get; set; }
        
        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();
        
        public long Rows { get; set; }
        public double Seconds { get; set; }

        public TransformProperties PrimaryProperties { get; set; }
        public TransformProperties ReferenceProperties { get; set; }
    }
}