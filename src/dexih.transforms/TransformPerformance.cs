
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace dexih.transforms
{
    [DataContract]
    public class TransformPerformance
    {
        public TransformPerformance()
        {
        }

        public TransformPerformance(string transformName, long rows, double seconds)
        {
            TransformName = transformName;
            Rows = rows;
            Seconds = seconds;
        }
        
        [DataMember(Order = 0)]
        public string TransformName { get; set; }

        [DataMember(Order = 1)]
        public string Action { get; set; }

        [DataMember(Order = 2)]
        public long Rows { get; set; }

        [DataMember(Order = 3)]
        public double Seconds { get; set; }

        [DataMember(Order = 4)]
        public List<TransformPerformance> Children { get; set; } = new List<TransformPerformance>();
    }
}