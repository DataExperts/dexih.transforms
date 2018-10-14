using System;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.Mappings
{
    public class MapAggregate: Mapping
    {
        public MapAggregate(TableColumn inputColumn, TableColumn outputColumn, SelectColumn.EAggregate aggregate = SelectColumn.EAggregate.Sum)
        {
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
            Aggregate = aggregate;
        }

        public TableColumn InputColumn;
        public TableColumn OutputColumn;
        public SelectColumn.EAggregate Aggregate { get; set; }

        private int _inputOrdinal;
        private int _outputOrdinal;

        public long Count { get; set; }
        public object Value { get; set; }

        public override void InitializeInputOrdinals(Table table, Table joinTable = null)
        {
            _inputOrdinal = table.GetOrdinal(InputColumn);
        }
        
        public override void AddOutputColumns(Table table)
        {
            table.Columns.Add(OutputColumn);
            _outputOrdinal = table.Columns.Count - 1;
        }

        public override bool ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            Count++;
            var value = _inputOrdinal == -1 ? InputColumn.DefaultValue : row[_inputOrdinal];
            
            if(Value == null && value != null)
            {
                Value = value;
            }
            else
            {
                switch (Aggregate)
                {
                    case SelectColumn.EAggregate.Sum:
                    case SelectColumn.EAggregate.Average:
                        if (value != null)
                        {
                            Value = DataType.Add(InputColumn.DataType, Value ?? 0, value);
                        }

                        break;
                    case SelectColumn.EAggregate.Min:
                        var compare = DataType.Compare(InputColumn.DataType, value, Value);
                        if (compare == DataType.ECompareResult.Less)
                        {
                            Value = value;
                        }
                        break;
                    case SelectColumn.EAggregate.Max:
                        var compare1 = DataType.Compare(InputColumn.DataType, value, Value);
                        if (compare1 == DataType.ECompareResult.Greater)
                        {
                            Value = value;
                        }
                        break;
                }
            }
            
            return true;
        }

        public override void MapOutputRow(object[] row)
        {
            return;
        }

        public override bool ProcessResultRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            if (functionType == EFunctionType.Aggregate)
            {
                object value = null;
                switch (Aggregate)
                {
                    case SelectColumn.EAggregate.Count:
                        value = Count;
                        break;
                    case SelectColumn.EAggregate.Max:
                    case SelectColumn.EAggregate.Min:
                    case SelectColumn.EAggregate.Sum:
                        value = Value;
                        break;
                    case SelectColumn.EAggregate.Average:
                        value = Count == 0 ? 0 : DataType.Divide(OutputColumn.DataType, Value, Count);
                        break;
                }

                row[_outputOrdinal] = value;
            }

            return false;
        }


        public override object GetInputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override void Reset(EFunctionType functionType)
        {
            if (functionType == EFunctionType.Aggregate)
            {
                Value = null;
                Count = 0;
            }
        }
        
        public override void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            switch (Aggregate)
            {
                case SelectColumn.EAggregate.Sum:
                case SelectColumn.EAggregate.Average:
                case SelectColumn.EAggregate.Count:
                    fillerRow[_inputOrdinal] = 0;
                    break;
            }   
        }
    }
}