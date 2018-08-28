//using dexih.functions;
//using Dexih.Utils.DataType;
//using static dexih.functions.Query.SelectColumn;
//
//namespace dexih.functions.Mappings
//{
//    public class AggregatePair
//    {
//        public AggregatePair()
//        { }
//
//        /// <summary>
//        /// Sets the source and target mappings to the same column name
//        /// </summary>
//        /// <param name="sourceTargetColumn">Column Name</param>
//        public AggregatePair(TableColumn sourceTargetColumn, EAggregate aggregate = EAggregate.Sum)
//        {
//            SourceColumn = sourceTargetColumn;
//            TargetColumn = sourceTargetColumn;
//            Aggregate = aggregate;
//        }
//
//        /// <summary>
//        /// Sets the source and column mapping.
//        /// </summary>
//        /// <param name="sourceColumn">Source Column Name</param>
//        /// <param name="targetColumn">Target Column Name</param>
//        public AggregatePair(TableColumn sourceColumn, TableColumn targetColumn, EAggregate aggregate = EAggregate.Sum)
//        {
//            SourceColumn = sourceColumn;
//            TargetColumn = targetColumn;
//            Aggregate = aggregate;
//        }
//
//        public object SourceValue { get; set; }
//        public TableColumn SourceColumn { get; set; }
//        public TableColumn TargetColumn { get; set; }
//
//        public EAggregate Aggregate { get; set; }
//
//        public long Count { get; set; }
//        public object Value { get; set; }
//
//        public void Reset()
//        {
//            Value = null;
//            Count = 0;
//        }
//
//        public void AddValue(object value)
//        {
//            Count++;
//
//            if(Value == null && value != null)
//            {
//                Value = value;
//            }
//            else
//            {
//                switch (Aggregate)
//                {
//                    case EAggregate.Sum:
//                    case EAggregate.Average:
//                        Value = DataType.Add(SourceColumn.DataType, Value ?? 0, value);
//                        break;
//                    case EAggregate.Min:
//                        var compare = DataType.Compare(SourceColumn.DataType, value, Value);
//                        if (compare == DataType.ECompareResult.Less)
//                        {
//                            Value = value;
//                        }
//                        break;
//                    case EAggregate.Max:
//                        var compare1 = DataType.Compare(SourceColumn.DataType, value, Value);
//                        if (compare1 == DataType.ECompareResult.Greater)
//                        {
//                            Value = value;
//                        }
//                        break;
//                }
//            }
//        }
//
//        public object GetValue()
//        {
//            switch (Aggregate)
//            {
//                case EAggregate.Count:
//                    return Count;
//                case EAggregate.Max:
//                case EAggregate.Min:
//                case EAggregate.Sum:
//                    return Value;
//                case EAggregate.Average:
//                    return Count == 0 ? 0 : DataType.Divide(TargetColumn.DataType, Value, Count);
//            }
//
//            return null;
//        }
//    }
//}
