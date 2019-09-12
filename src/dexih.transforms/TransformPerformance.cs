using ProtoBuf;
using System.Collections.Generic;

namespace dexih.transforms
{
    [ProtoContract]
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
        
        [ProtoMember(1)]
        public string TransformName { get; set; }

        [ProtoMember(2)]
        public string Action { get; set; }

        [ProtoMember(3)]
        public long Rows { get; set; }

        [ProtoMember(4)]
        public double Seconds { get; set; }

        [ProtoMember(5)]
        public List<TransformPerformance> Children { get; set; } = new List<TransformPerformance>();
    }
}