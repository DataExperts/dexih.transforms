using dexih.functions;
using Dexih.Utils.DataType;
using System;
using System.Collections.Generic;
using System.Text;
using static dexih.functions.Query.SelectColumn;

namespace dexih.transforms
{
    public class AggregatePair
    {
        public AggregatePair()
        { }

        /// <summary>
        /// Sets the source and target mappings to the same column name
        /// </summary>
        /// <param name="sourceTargetColumn">Column Name</param>
        public AggregatePair(TableColumn sourceTargetColumn)
        {
            SourceColumn = sourceTargetColumn;
            TargetColumn = sourceTargetColumn;
        }

        /// <summary>
        /// Sets the source and column mapping.
        /// </summary>
        /// <param name="sourceColumn">Source Column Name</param>
        /// <param name="targetColumn">Target Column Name</param>
        public AggregatePair(TableColumn sourceColumn, TableColumn targetColumn)
        {
            SourceColumn = sourceColumn;
            TargetColumn = targetColumn;
        }

        public TableColumn SourceColumn { get; set; }
        public TableColumn TargetColumn { get; set; }

        public EAggregate Aggregate { get; set; }

        public long Count { get; set; }
        public object Value { get; set; }

        public void Reset()
        {
            Value = null;
        }

        public void AddValue(object value)
        {
            Count++;

            if(Value == null && value != null)
            {
                Value = value;
            }
            else
            {
                switch (Aggregate)
                {
                    case EAggregate.Sum:
                    case EAggregate.Average:
                        Value = DataType.Add(SourceColumn.DataType, Value ?? 0, value);
                        break;
                    case EAggregate.Min:
                        var compare = DataType.Compare(SourceColumn.DataType, value, Value);
                        if (compare == DataType.ECompareResult.Less)
                        {
                            Value = value;
                        }
                        break;
                    case EAggregate.Max:
                        var compare1 = DataType.Compare(SourceColumn.DataType, value, Value);
                        if (compare1 == DataType.ECompareResult.Greater)
                        {
                            Value = value;
                        }
                        break;
                }
            }
        }

        public object GetValue()
        {
            switch (Aggregate)
            {
                case EAggregate.Count:
                    return Count;
                case EAggregate.Max:
                case EAggregate.Min:
                case EAggregate.Sum:
                    return Value;
                case EAggregate.Average:
                    return Count == 0 ? 0 : DataType.Divide(SourceColumn.DataType, Value, Count);
            }

            return null;
        }
    }
}
