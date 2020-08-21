using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms.Mapping
{
    public class MapAggregate: Mapping
    {
        public MapAggregate(TableColumn inputColumn, TableColumn outputColumn, EAggregate aggregate = EAggregate.Sum)
        {
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
            Aggregate = aggregate;
        }

        public TableColumn InputColumn;
        public TableColumn OutputColumn;
        public EAggregate Aggregate { get; set; }

        private int _inputOrdinal;
        private int _outputOrdinal;

        public int Count { get; set; }
        public object Value { get; set; }
        private bool _firstRow = true;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            _inputOrdinal = table.GetOrdinal(InputColumn);
        }
        
        public override void AddOutputColumns(Table table)
        {
            table.Columns.Add(OutputColumn);
            _outputOrdinal = table.Columns.Count - 1;
        }

        public override Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
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
                    case EAggregate.Sum:
                    case EAggregate.Average:
                        if (value != null)
                        {
                            Value = Operations.Add(InputColumn.DataType, Value ?? 0, value);
                        }

                        break;
                    case EAggregate.Min:
                        if (Operations.LessThan(InputColumn.DataType, value, Value))
                        {
                            Value = value;
                        }
                        break;
                    case EAggregate.Max:
                        if (Operations.GreaterThan(InputColumn.DataType, value, Value))
                        {
                            Value = value;
                        }
                        break;
                    case EAggregate.First:
                        if (_firstRow)
                        {
                            Value = value;
                            _firstRow = false;
                        }
                        break;
                    case EAggregate.Last:
                        Value = value;
                        break;
                }
            }

            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] row)
        {
        }

        public override string Description()
        {
            return $"{Aggregate}({InputColumn?.Name} => {OutputColumn?.Name}";
        }

        public override Task<bool> ProcessResultRowAsync(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            if (functionType == EFunctionType.Aggregate)
            {
                object value = null;
                switch (Aggregate)
                {
                    case EAggregate.Count:
                        value = Count;
                        break;
                    case EAggregate.First:
                    case EAggregate.Last:
                    case EAggregate.Max:
                    case EAggregate.Min:
                    case EAggregate.Sum:
                        value = Value;
                        break;
                    case EAggregate.Average:
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

        public override Task<bool> ProcessFillerRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            if (functionType == EFunctionType.Aggregate)
            {
                object value = null;
                switch (Aggregate)
                {
                    case EAggregate.Count:
                        value = 0;
                        break;
                    case EAggregate.Max:
                    case EAggregate.Min:
                    case EAggregate.Sum:
                    case EAggregate.Average:
                        value = null;
                        break;
                }

                row[_outputOrdinal] = value;
            }

            return Task.FromResult(false);
        }


        public override object GetOutputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override void Reset(EFunctionType functionType)
        {
            if (functionType == EFunctionType.Aggregate)
            {
                Value = null;
                Count = 0;
                _firstRow = true;
            }
        }

        public override IEnumerable<SelectColumn> GetRequiredColumns(bool includeAggregate)
        {
            if (includeAggregate)
            {
                yield return new SelectColumn(InputColumn, Aggregate, OutputColumn);    
            }
            else
            {
                yield return new SelectColumn(InputColumn, EAggregate.None, InputColumn);
            }
        }

        public override void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            switch (Aggregate)
            {
                case EAggregate.Sum:
                case EAggregate.Average:
                case EAggregate.Count:
                    fillerRow[_inputOrdinal] = 0;
                    break;
            }   
        }

        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            if(selectQuery.Columns == null || 
               !selectQuery.Columns.Any() ||
               InputColumn == null ||
               Aggregate == EAggregate.First ||
               Aggregate == EAggregate.Last)
            {
                return false;
            }

            foreach (var selectColumn in selectQuery.Columns)
            {
                if (selectColumn.Column.Name == InputColumn.Name && selectColumn.Aggregate == Aggregate)
                {
                    return true;
                }
            }

            return false;
        }
    }
}