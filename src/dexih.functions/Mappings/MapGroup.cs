using dexih.functions;

namespace dexih.functions.Mappings
{
    public class MapGroup: MapColumn
    {
        public MapGroup(TableColumn inputColumn, TableColumn outputColumn)
        {
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
        }

        public MapGroup(TableColumn inputColumn)
        {
            InputColumn = inputColumn;
            OutputColumn = inputColumn;
        }

        public MapGroup(object inputValue, TableColumn outputColumn)
        {
            InputValue = inputValue;
            OutputColumn = outputColumn;
        }
        
        public override void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            fillerRow[InputOrdinal] = row == null ? RowData?[InputOrdinal] : row[InputOrdinal];   
        }


    }
}