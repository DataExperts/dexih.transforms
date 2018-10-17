using System;
using System.Threading.Tasks;
using dexih.functions.BuiltIn;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.Mappings
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
        public Object InputValue { get; set; }
        public Object JoinValue { get; set; }
        
        public Filter.ECompare Compare { get; set; }
        
        /// <summary>
        /// Stores the actual compare result.
        /// </summary>
        public DataType.ECompareResult CompareResult { get; set; }

        private int _column1Ordinal;
        private int _column2Ordinal;

        private object[] _row;
        private object[] _joinRow;

        public override Task Initialize(Table table, Table joinTable)
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

            return Task.CompletedTask;
        }

        public override void AddOutputColumns(Table table)
        {
            return;
        }

        public override bool ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow)
        {
            if (row != null)
            {
                _row = row;
            }

            if (joinRow != null)
            {
                _joinRow = joinRow;
            }

            var value1 = GetInputValue();
            var value2 = GetJoinValue();
            
            CompareResult = DataType.Compare(InputColumn.DataType, value1, value2);

            switch (Compare)
            {
                case Filter.ECompare.GreaterThan:
                    if (CompareResult != DataType.ECompareResult.Greater)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.IsEqual:
                    if (CompareResult != DataType.ECompareResult.Equal)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.GreaterThanEqual:
                    if (CompareResult == DataType.ECompareResult.Less)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.LessThan:
                    if (CompareResult != DataType.ECompareResult.Less)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.LessThanEqual:
                    if (CompareResult == DataType.ECompareResult.Greater)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.NotEqual:
                    if (CompareResult == DataType.ECompareResult.Equal)
                    {
                        return false;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        public override void MapOutputRow(object[] row)
        {
            return;
        }

       
        public override object GetInputValue(object[] row = null)
        {
            if (_column1Ordinal == -1)
            {
                return InputValue;
            }
            else
            {
                return row == null ? _row[_column1Ordinal] : row[_column1Ordinal];    
            }
        }

        public object GetJoinValue(object[] row = null)
        {
            if (_column2Ordinal == -1)
            {
                return JoinValue;
            }
            else
            {
                if (_joinRow == null && row == null)
                {
                    return null;
                }
                return row == null ? _joinRow[_column2Ordinal] : row[_column2Ordinal];    
            }
        }


    }
}