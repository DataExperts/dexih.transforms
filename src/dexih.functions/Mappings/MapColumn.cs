using dexih.functions;

namespace dexih.functions.Mappings
{
    public class MapColumn: Mapping
    {
        public MapColumn() {}

        public MapColumn(TableColumn inputColumn)
        {
            InputColumn = inputColumn;
            OutputColumn = inputColumn;
        }

        public MapColumn(TableColumn inputColumn, TableColumn outputColumn)
        {
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
        }
        
        public MapColumn(object inputValue, TableColumn outputColumn)
        {
            InputValue = inputValue;
            OutputColumn = outputColumn;
        }

        public object InputValue;
        public TableColumn InputColumn;
        public TableColumn OutputColumn;

        private int _inputOrdinal = -1;
        private int _outputOrdinal = -1;

        private object[] _rowData;

        public override void InitializeInputOrdinals(Table table, Table joinTable = null)
        {
            if (InputColumn != null)
            {
                _inputOrdinal = table.GetOrdinal(InputColumn);
                if (_inputOrdinal < 0 && InputValue == null)
                {
                    InputValue = InputColumn.DefaultValue;
                }
            }
        }

        public override void AddOutputColumns(Table table)
        {
            _outputOrdinal = AddOutputColumn(table, OutputColumn);
        }

        public override bool ProcessInputRow(object[] rowData, object[] joinRow = null)
        {
            _rowData = rowData;
            return true;
        }

        public override void ProcessOutputRow(object[] data)
        {
            if (_inputOrdinal == -1)
            {
                data[_outputOrdinal] = InputValue;
            }
            else
            {
                data[_outputOrdinal] = _rowData[_inputOrdinal];    
            }
        }

        public override void ProcessResultRow(int index, object[] row) {}
        
        public override object GetInputValue(object[] row = null)
        {
            if (_inputOrdinal == -1)
            {
                return InputValue;
            }
            else
            {
                return row == null ? _rowData[_inputOrdinal] : row[_inputOrdinal];    
            }        
        }

        public override void Reset()
        {
        }

    }
}