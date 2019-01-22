using System;
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
        
        public Filter.ECompare Compare { get; set; }
        
        /// <summary>
        /// Stores the actual compare result.
        /// </summary>
        public int CompareResult { get; set; }

        private int _column1Ordinal;
        private int _column2Ordinal;

        private object[] _row;
        private object[] _joinRow;

        public override void InitializeColumns(Table table, Table joinTable)
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
            return;
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow, CancellationToken cancellationToken = default)
        {
            if (row != null)
            {
                _row = row;
            }

            if (joinRow != null)
            {
                _joinRow = joinRow;
            }

            var value1 = GetOutputTransform();
            var value2 = GetJoinValue();

            CompareResult = Operations.Compare(InputColumn.DataType, value1, value2);
            bool returnResult;

            switch (Compare)
            {
                case Filter.ECompare.GreaterThan:
                    returnResult = CompareResult == 1;
                    break;
                case Filter.ECompare.IsEqual:
                    returnResult = CompareResult == 0;
                    break;
                case Filter.ECompare.GreaterThanEqual:
                    returnResult = CompareResult != -1;
                    break;
                case Filter.ECompare.LessThan:
                    returnResult = CompareResult == -1;
                    break;
                case Filter.ECompare.LessThanEqual:
                    returnResult = CompareResult != 1;
                    break;
                case Filter.ECompare.NotEqual:
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
       
        public override object GetOutputTransform(object[] row = null)
        {
            if (_column1Ordinal == -1)
            {
                return InputValue;
            }

            return row == null ? _row[_column1Ordinal] : row[_column1Ordinal];
        }

        public override string Description()
        {
            var item1 = _column1Ordinal == -1 ? InputValue : InputColumn.Name;
            var item2 = _column2Ordinal == -1 ? JoinValue : JoinColumn.Name;
            return $"Join({item1} {Compare} {item2}";
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