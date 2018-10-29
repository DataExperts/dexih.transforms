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

            CompareResult = Operations.Compare(InputColumn.DataType, value1, value2);

            switch (Compare)
            {
                case Filter.ECompare.GreaterThan:
                    return CompareResult == 1;
                case Filter.ECompare.IsEqual:
                    return CompareResult == 0;
                case Filter.ECompare.GreaterThanEqual:
                    return CompareResult != -1;
                case Filter.ECompare.LessThan:
                    return CompareResult == -1;
                case Filter.ECompare.LessThanEqual:
                    return CompareResult != 1;
                case Filter.ECompare.NotEqual:
                    return CompareResult != 0;
                default:
                    throw new ArgumentOutOfRangeException();
            }
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