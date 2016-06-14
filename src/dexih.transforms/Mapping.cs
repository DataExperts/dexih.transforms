using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class ColumnPair
    {
        /// <summary>
        /// Sets the source and target mappings to the same column name
        /// </summary>
        /// <param name="sourceTargetColumn">Column Name</param>
        public ColumnPair(string sourceTargetColumn)
        {
            SourceColumn = sourceTargetColumn;
            TargetColumn = sourceTargetColumn;
        }

        /// <summary>
        /// Sets the source and column mapping.
        /// </summary>
        /// <param name="sourceColumn">Source Column Name</param>
        /// <param name="targetColumn">Target Column Name</param>
        public ColumnPair(string sourceColumn, string targetColumn)
        {
            SourceColumn = sourceColumn;
            TargetColumn = targetColumn;
        }

        public string SourceColumn { get; set; }
        public string TargetColumn { get; set; }
    }

    public class JoinPair
    {
        public JoinPair() { }
        public JoinPair(string sourceColumn, string joinColumn)
        {
            SourceColumn = sourceColumn;
            JoinColumn = joinColumn;
        }

        public string SourceColumn { get; set; }
        public string JoinColumn { get; set; }
        public string JoinValue { get; set; }
    }
}
