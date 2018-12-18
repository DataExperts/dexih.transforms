using System;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms.Mapping
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

        public int Count { get; set; }
        public object Value { get; set; }

        public override void InitializeColumns(Table table, Table joinTable = null)
        {
            _inputOrdinal = table.GetOrdinal(InputColumn);
        }
        
        public override void AddOutputColumns(Table table)
        {
            table.Columns.Add(OutputColumn);
            _outputOrdinal = table.Columns.Count - 1;
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
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
                            Value = Operations.Add(InputColumn.DataType, Value ?? 0, value);
                        }

                        break;
                    case SelectColumn.EAggregate.Min:
                        if (Operations.LessThan(InputColumn.DataType, value, Value))
                        {
                            Value = value;
                        }
                        break;
                    case SelectColumn.EAggregate.Max:
                        if (Operations.GreaterThan(InputColumn.DataType, value, Value))
                        {
                            Value = value;
                        }
                        break;
                }
            }

            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] row)
        {
            return;
        }

        public override string Description()
        {
            return $"{Aggregate}({InputColumn?.Name} => {OutputColumn?.Name}";
        }

        public override Task<bool> ProcessResultRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
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
                    // average may have a different output datatype than input, so parse it.  
                    // TODO: Find way to avoid parse as this causes minor performance.
                        var input = Operations.Parse(OutputColumn.DataType, Value);
                        value = Count == 0 ? 0 : Operations.DivideInt(OutputColumn.DataType, input, Count);
                        break;
                }

                row[_outputOrdinal] = value;
            }

            return Task.FromResult(false);
        }


        public override object GetOutputTransform(object[] row = null)
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