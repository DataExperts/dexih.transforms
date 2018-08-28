//using dexih.functions;
//
//namespace dexih.functions.Mappings
//{
//    /// <summary>
//    /// Specifies joins to column or joins to static values.
//    /// </summary>
//    public class Join
//    {
//        public Join() { }
//        public Join(TableColumn sourceColumn, TableColumn joinColumn)
//        {
//            SourceColumn = sourceColumn;
//            JoinColumn = joinColumn;
//        }
//
//        public Join(TableColumn joinColumn, object joinValue)
//        {
//            JoinColumn = joinColumn;
//            JoinValue = joinValue;
//        }
//
//        public object SourceValue { get; set; }
//        public TableColumn SourceColumn { get; set; }
//        public TableColumn JoinColumn { get; set; }
//        public object JoinValue { get; set; }
//    }
//}