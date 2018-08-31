using System;
using Dexih.Utils.DataType;

namespace dexih.functions.Parameter
{
    public class ParameterColumn : Parameter
    {
        public ParameterColumn()
        {
        }

        /// <summary>
        /// Initializes are parameter
        /// </summary>
        /// <param name="name">Paramter name</param>
        /// <param name="parameterType">Parameter datatype</param>
        /// <param name="column"></param>
        public ParameterColumn(
            string name, 
            TableColumn column
        )
        {
            Name = name;
            DataType = column.DataType;
            Column = column;
        }
        
        public ParameterColumn(
            string name, 
            DataType.ETypeCode dataType,
            TableColumn column
        )
        {
            Name = name;
            DataType = dataType;
            Column = column;
        }

        public ParameterColumn(string columName, DataType.ETypeCode dataType)
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
            return new ParameterColumn(Name, Column);
        }

    }
}