using System.Collections.Generic;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms
{
    public class TransformWriterOptions
    {
        public enum ETargetAction
        {
            None,
            Truncate,
            DropCreate,
            CreateNotExists
        }

        /// <summary>
        /// Indicates the preload action to be applied to the target table.
        /// </summary>
        public ETargetAction TargetAction { get; set; } = ETargetAction.None;  

        /// <summary>
        /// Indicates any previously stored incremental values should be ignored and <see cref="ResetIncremental"/> used instead
        /// </summary>
        public bool ResetIncremental { get; set; } = false;

        /// <summary>
        /// Value to use when filtering the incremental value from the source table.  Only applies when <see cref="ResetIncremental"/> is true.
        /// </summary>
        public object ResetIncrementalValue { get; set; } = null;

        /// <summary>
        /// Adds a default row to the target table based on default values of each table column.
        /// </summary>
        public bool AddDefaultRow { get; set; } = false;

        public GlobalVariables GlobalVariables { get; set; } = null;

        public SelectQuery SelectQuery { get; set; } = null;
        
        /// <summary>
        /// Preview mode indicates that actions such as moving read files, updating incremental counters will not be done.
        /// </summary>
        public bool PreviewMode { get; set; } = false;

        /// <summary>
        /// Indicates how the job was triggered.  Result is stored in audit table.
        /// </summary>
        public TransformWriterResult.ETriggerMethod TriggerMethod { get; set; } = TransformWriterResult.ETriggerMethod.Manual;
        public string TriggerInfo { get; set; } = "";

        public int CommitSize = 1000;

        public bool IsEmptyTarget()
        {
            return TargetAction == ETargetAction.Truncate || TargetAction == ETargetAction.DropCreate;
        }

        private HashSet<string> targets = new HashSet<string>();
        
        /// <summary>
        /// Used to check target tables with have already been truncated.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool CheckTarget(string name)
        {
            if (targets.Contains(name)) return false;

            targets.Add(name);

            return true;
        }
    }
}