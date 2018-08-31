﻿using dexih.functions;

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

        public override void InitializeInputOrdinals(Table table, Table joinTable = null)
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

        public override bool ProcessInputRow(object[] rowData, object[] joinRow = null)
        {
            RowData = rowData;
            return true;
        }

        public override void ProcessOutputRow(object[] data)
        {
            data[OutputOrdinal] = GetInputValue();
        }

        public override void ProcessResultRow(int index, object[] row) {}
        
        public override object GetInputValue(object[] row = null)
        {
            if (InputOrdinal == -1 )
            {
                return InputValue;
            }
            else
            {
                return row == null ? RowData[InputOrdinal] : row[InputOrdinal];    
            }        
        }

        public override void ProcessFillerRow(object[] fillerRow, object seriesValue)
        {
            fillerRow[InputOrdinal] = RowData[InputOrdinal];
        }

        public override void Reset()
        {
            RowData = null;
            InputValue = null;
        }

    }
}