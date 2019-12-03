using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms.Mapping
{
    public class MapJoin: Mapping
    {
        public MapJoin(TableColumn inputColumn, TableColumn joinColumn)
        {
            InputColumn = inputColumn;
            JoinColumn = joinColumn;
        }

        public MapJoin(object inputValue, TableColumn inputColumn, object joinValue, TableColumn joinColumn)
        {
            InputValue = inputValue;
            InputColumn = inputColumn;
            JoinValue = joinValue;
            JoinColumn = joinColumn;
        }

        public TableColumn InputColumn { get; set; }
        public TableColumn JoinColumn { get; set; }
        public object InputValue { get; set; }
        public object JoinValue { get; set; }

        public ECompare Compare { get; set; } = ECompare.IsEqual;
        
        /// <summary>
        /// Stores the actual compare result.
        /// </summary>
        public int CompareResult { get; set; }

        private int _column1Ordinal;
        private int _column2Ordinal;

        private object[] _row;
        private object[] _joinRow;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            if (InputColumn != null)
            {
                _column1Ordinal = table.GetOrdinal(InputColumn);
                if (_column1Ordinal < 0 && InputValue == null)
                {
                    InputValue = InputColumn.DefaultValue;
                }
            }
            else
            {
                _column1Ordinal = -1;
            }

            if (JoinColumn != null)
            {
                _column2Ordinal = joinTable.GetOrdinal(JoinColumn);
                if (_column2Ordinal < 0 && JoinValue == null)
                {
                    JoinValue = JoinColumn.DefaultValue;
                }
            }
            else
            {
                _column2Ordinal = -1;
            }
        }

        public override void AddOutputColumns(Table table)
        {
        }

        public override Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            if (row != null)
            {
                _row = row;
            }

            if (joinRow != null)
            {
                _joinRow = joinRow;
            }

            var value1 = GetOutputValue();
            var value2 = GetJoinValue();

            var dataType = JoinColumn?.DataType ?? InputColumn?.DataType;
            if (dataType == null)
            {
                CompareResult = Operations.Compare(value1, value2);
            }
            else
            {
                CompareResult = Operations.Compare(dataType.Value, value1, value2);    
            }
            
            bool returnResult;

            switch (Compare)
            {
                case ECompare.GreaterThan:
                    returnResult = CompareResult > 0;
                    break;
                case ECompare.IsEqual:
                    returnResult = CompareResult == 0;
                    break;
                case ECompare.GreaterThanEqual:
                    returnResult = CompareResult <= 0;
                    break;
                case ECompare.LessThan:
                    returnResult = CompareResult < 0;
                    break;
                case ECompare.LessThanEqual:
                    returnResult = CompareResult < 1;
                    break;
                case ECompare.NotEqual:
                    returnResult = CompareResult != 0;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return Task.FromResult(returnResult);
        }

        public override void MapOutputRow(object[] row)
        {
        }
       
        public override object GetOutputValue(object[] row = null)
        {
            if (_column1Ordinal == -1)
            {
                return InputValue;
            }

            if (_row == null && row == null)
            {
                return null;
            }

            return row?[_column1Ordinal]??_row[_column1Ordinal];
        }

        public override string Description()
        {
            var item1 = _column1Ordinal == -1 ? InputValue : InputColumn.Name;
            var item2 = _column2Ordinal == -1 ? JoinValue : JoinColumn.Name;
            return $"Join({item1} {Compare} {item2}";
        }

        public override IEnumerable<SelectColumn> GetRequiredColumns()
        {
            if (InputColumn == null)
            {
                return new SelectColumn[0];
            }

            return new[] {new SelectColumn(InputColumn)};
        }

        public override IEnumerable<TableColumn> GetRequiredReferenceColumns()
        {
            if (JoinColumn == null)
            {
                return new TableColumn[0];
            }

            return new[] {JoinColumn};
        }

        public object GetJoinValue(object[] row = null)
        {
            if (_column2Ordinal == -1)
            {
                return JoinValue;
            }

            if (_joinRow == null && row == null)
            {
                return null;
            }
            
            return row == null ? _joinRow[_column2Ordinal] : row[_column2Ordinal];
        }


    }
}