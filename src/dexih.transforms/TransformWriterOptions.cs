using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms
{
    public class TransformWriterOptions
    {
        public bool TruncateTarget { get; set; } = false; //once off truncate of the target table.  

        public bool ResetIncremental { get; set; } = false;

        public object ResetIncrementalValue { get; set; } = null;

        public GlobalVariables GlobalVariables { get; set; } = null;
        public SelectQuery SelectQuery { get; set; } = null;
        public bool PreviewMode { get; set; } = false;

        public TransformWriterResult.ETriggerMethod TriggerMethod { get; set; } = TransformWriterResult.ETriggerMethod.Manual;
        public string TriggerInfo { get; set; } = "";
    }
}