using MessagePack;
using System.Collections.Generic;

namespace dexih.transforms
{
    [MessagePackObject]
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
        
        [Key(0)]
        public string TransformName { get; set; }

        [Key(1)]
        public string Action { get; set; }

        [Key(2)]
        public long Rows { get; set; }

        [Key(3)]
        public double Seconds { get; set; }

        [Key(4)]
        public List<TransformPerformance> Children { get; set; } = new List<TransformPerformance>();
    }
}