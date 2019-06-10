using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms.Mapping
{
    public class MapInputColumn: Mapping
    {
        public MapInputColumn() {}

        public MapInputColumn(TableColumn inputColumn)
        {
            _inputColumn = inputColumn;
            _inputValue = inputColumn.DefaultValue;
        }


        private TableColumn _inputColumn;
        private object _inputValue;

        protected int InputOrdinal = -1;
        protected int OutputOrdinal = -1;

        protected object[] RowData;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            if (_inputColumn == null) return;
            
            InputOrdinal = table.GetOrdinal(_inputColumn);
        }

        public override void AddOutputColumns(Table table)
        {
            OutputOrdinal = AddOutputColumn(table, _inputColumn);
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow, CancellationToken cancellationToken)
        {
            RowData = row;
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data)
        {
            data[OutputOrdinal] = _inputValue;
        }

        public override object GetOutputValue(object[] row = null)
        {
            return _inputValue;
        }

        public override string Description()
        {
            return $"Input ({_inputColumn?.Name}";
        }

        public override void Reset(EFunctionType functionType)
        {
        }

        public void SetInput(IEnumerable<TableColumn> inputColumns)
        {
            var column = inputColumns.SingleOrDefault(c => c.Name == _inputColumn.Name);
            if (column != null)
            {
                _inputValue = column.DefaultValue;
            }
        }

    }
}