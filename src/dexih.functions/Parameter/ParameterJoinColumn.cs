using System;
using Dexih.Utils.DataType;

namespace dexih.functions.Parameter
{
    public class ParameterJoinColumn : Parameter
    {
        public ParameterJoinColumn()
        {
        }

        /// <summary>
        /// Initializes are parameter
        /// </summary>
        /// <param name="name">Paramter name</param>
        /// <param name="parameterType">Parameter datatype</param>
        /// <param name="column"></param>
        public ParameterJoinColumn(
            string name, 
            TableColumn column
        )
        {
            Name = name;
            DataType = column.DataType;
            Rank = column.Rank;
            Column = column;
        }

        public ParameterJoinColumn(string columName, DataType.ETypeCode dataType, int rank)
        {
            Name = columName;
            DataType = dataType;
            Rank = rank;
            Column = new TableColumn(columName, dataType);
        }

        public TableColumn Column;

        /// <summary>
        /// The index of the datarow to get the value from.
        /// </summary>
        private int _rowOrdinal;

        public override void InitializeOrdinal(Table table, Table joinTable)
        {
            if (joinTable == null)
            {
                throw new Exception("There is a join parameter set, but no join table.");
            }
            
            _rowOrdinal = joinTable.GetOrdinal(Column);
            if (_rowOrdinal < 0)
            {
                joinTable.Columns.Add(Column);
                _rowOrdinal = joinTable.Columns.Count - 1;
            }
        }

        public override void SetInputData(object[] data, object[] joinData)
        {
            SetValue(joinData?[_rowOrdinal]);
        }

        public override void PopulateRowData(object value, object[] data, object[] joinData)
        {
            SetValue(value);
            joinData[_rowOrdinal] = Value;
        }
        
        public override Parameter Copy()
        {
            return new ParameterJoinColumn(Name, Column);
        }

    }
}