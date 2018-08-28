using System;
using Dexih.Utils.DataType;

namespace dexih.functions.Parameter
{
    public class ParameterOutputColumn : Parameter
    {
        public ParameterOutputColumn()
        {
        }

        /// <summary>
        /// Initializes are parameter
        /// </summary>
        /// <param name="name">Paramter name</param>
        /// <param name="parameterType">Parameter datatype</param>
        /// <param name="column"></param>
        public ParameterOutputColumn(
            string name, 
            TableColumn column
        )
        {
            Name = name;
            DataType = column.DataType;
            Column = column;
        }

        public ParameterOutputColumn(string columName, DataType.ETypeCode dataType)
        {
            Name = columName;
            DataType = dataType;
            Column = new TableColumn(columName, dataType);
        }

        public TableColumn Column;

        /// <summary>
        /// The index of the datarow to get the value from.
        /// </summary>
        private int _rowOrdinal;

        public override void InitializeOrdinal(Table table, Table joinTable = null)
        {
            _rowOrdinal = table.GetOrdinal(Column);
            if (_rowOrdinal < 0)
            {
                table.Columns.Add(Column);
                _rowOrdinal = table.Columns.Count - 1;
            }
        }

        public override void SetInputData(object[] data, object[] joinRow = null)
        {
            SetValue(data?[_rowOrdinal]);
        }

        public override void PopulateRowData(object value, object[] data, object[] joinRow = null)
        {
            SetValue(value);
            data[_rowOrdinal] = Value;
        }
        
        public override Parameter Copy()
        {
            return new ParameterOutputColumn(Name, Column);
        }


    }
}