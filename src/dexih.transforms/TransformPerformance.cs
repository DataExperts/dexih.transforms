using System.Collections.Generic;
using dexih.functions.Query;

namespace dexih.transforms
{
    public class TransformPerformance
    {
        public TransformPerformance()
        {
        }

        public TransformPerformance(string transformName, string action, long rows, double seconds)
        {
            TransformName = transformName;
            Action = action;
            Rows = rows;
            Seconds = seconds;
        }
        
        public string TransformName { get; set; }
        public string Action { get; set; }
        public long Rows { get; set; }
        public double Seconds { get; set; }

        public List<TransformPerformance> Children { get; set; } = new List<TransformPerformance>();
    }
}