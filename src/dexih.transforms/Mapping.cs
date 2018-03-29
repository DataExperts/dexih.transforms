using System.Globalization;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms
{
    public class ColumnPair
    {
        /// <summary>
        /// Sets the source and target mappings to the same column name
        /// </summary>
        /// <param name="sourceTargetColumn">Column Name</param>
        public ColumnPair(TableColumn sourceTargetColumn)
        {
            SourceColumn = sourceTargetColumn;
            TargetColumn = sourceTargetColumn;
        }

        /// <summary>
        /// Sets the source and column mapping.
        /// </summary>
        /// <param name="sourceColumn">Source Column Name</param>
        /// <param name="targetColumn">Target Column Name</param>
        public ColumnPair(TableColumn sourceColumn, TableColumn targetColumn)
        {
            SourceColumn = sourceColumn;
            TargetColumn = targetColumn;
        }

        public TableColumn SourceColumn { get; set; }
        public TableColumn TargetColumn { get; set; }
    }

    /// <summary>
    /// Specifies joins to column or joins to static values.
    /// </summary>
    public class JoinPair
    {
        public JoinPair() { }
        public JoinPair(TableColumn sourceColumn, TableColumn joinColumn)
        {
            SourceColumn = sourceColumn;
            JoinColumn = joinColumn;
        }

        public JoinPair(TableColumn joinColumn, object joinValue)
        {
            JoinColumn = joinColumn;
            JoinValue = joinValue;
        }

        public TableColumn SourceColumn { get; set; }
        public TableColumn JoinColumn { get; set; }
        public object JoinValue { get; set; }
    }

    public class FilterPair
    {
        public FilterPair()
        {
        }

        public FilterPair(TableColumn column1, TableColumn column2, Filter.ECompare compare = Filter.ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = column2;
            FilterValue = null;
            Compare = compare;
        }
        
        public FilterPair(TableColumn column1, object filterValue, Filter.ECompare compare = Filter.ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = null;
            FilterValue = filterValue;
            Compare = compare;
        }

        public TableColumn Column1 { get; set; }
        public TableColumn Column2 { get; set; }
        public object FilterValue { get; set; }
        public Filter.ECompare Compare { get; set; }
    }
}
