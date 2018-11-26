using System.Threading.Tasks;
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

        public MapColumn(object inputValue, TableColumn inputColumn, TableColumn outputColumn)
        {
            InputValue = inputValue;
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
        }

        
        public object InputValue;
        public TableColumn InputColumn;
        public TableColumn OutputColumn;

        protected int InputOrdinal = -1;
        protected int OutputOrdinal = -1;

        protected object[] RowData;

        public override void InitializeColumns(Table table, Table joinTable = null)
        {
            if (InputColumn != null)
            {
                InputOrdinal = table.GetOrdinal(InputColumn);
                if (InputOrdinal < 0 && InputValue == null)
                {
                    InputValue = InputColumn.DefaultValue;
                }
            }
        }

        public override void AddOutputColumns(Table table)
        {
            OutputOrdinal = AddOutputColumn(table, OutputColumn);
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            RowData = row;
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data)
        {
            data[OutputOrdinal] = GetInputValue();
        }
        
        public override object GetInputValue(object[] row = null)
        {
            if (InputOrdinal == -1 )
            {
                return InputValue;
            }
            else
            {
                return row == null ? RowData?[InputOrdinal] : row[InputOrdinal];    
            }        
        }

        public override string Description()
        {
            return $"Mapping ({InputColumn?.Name} => {OutputColumn?.Name}";
        }

//        public override void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue)
//        {
//            fillerRow[InputOrdinal] = row == null ? RowData?[InputOrdinal] : row[InputOrdinal];   
//        }

        public override void Reset(EFunctionType functionType)
        {
//            RowData = null;
//            InputValue = null;
        }

    }
}