//using dexih.functions;
//
//namespace dexih.functions.Mappings
//{
//    public class ColumnPair
//    {
//        public ColumnPair()
//        {}
//        
//        /// <summary>
//        /// Sets the source and target mappings to the same column name
//        /// </summary>
//        /// <param name="sourceTargetColumn">Column Name</param>
//        public ColumnPair(TableColumn sourceTargetColumn)
//        {
//            SourceColumn = sourceTargetColumn;
//            TargetColumn = sourceTargetColumn;
//        }
//
//        /// <summary>
//        /// Sets the source and column mapping.
//        /// </summary>
//        /// <param name="sourceColumn">Source Column Name</param>
//        /// <param name="targetColumn">Target Column Name</param>
//        public ColumnPair(TableColumn sourceColumn, TableColumn targetColumn)
//        {
//            SourceColumn = sourceColumn;
//            TargetColumn = targetColumn;
//        }
//
//        public object SourceValue { get; set; }
//        public TableColumn SourceColumn { get; set; }
//        public TableColumn TargetColumn { get; set; }
//    }
//}