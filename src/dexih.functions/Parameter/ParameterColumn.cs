using System;
using System.Collections.Generic;
using Dexih.Utils.DataType;
using ProtoBuf;

namespace dexih.functions.Parameter
{
    [ProtoContract]
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
            Rank = column.Rank;
            Column = column;
            
        }
        
        public ParameterColumn(
            string name, 
            DataType.ETypeCode dataType,
            int rank,
            TableColumn column
        )
        {
            Name = name;
            DataType = dataType;
            Rank = rank;
            Column = column;
        }

        public ParameterColumn(string columnName, DataType.ETypeCode dataType)
        {
            Name = columnName;
            DataType = dataType;
            Column = new TableColumn(columnName, dataType);
        }

        [ProtoMember(1)]
        public TableColumn Column;

        /// <summary>
        /// The index of the datarow to get the value from.
        /// </summary>
        private int _rowOrdinal = -1;

        private bool _useJoinTable = false;

        public override void InitializeOrdinal(Table table, Table joinTable = null)
        {
            if (joinTable != null && !string.IsNullOrEmpty(Column.ReferenceTable))
            {
                _useJoinTable = true;
                _rowOrdinal = joinTable.GetOrdinal(Column);
            }
            else
            {
                _useJoinTable = false;
                _rowOrdinal = table.GetOrdinal(Column);    
            }
            

            if (_rowOrdinal < 0)
            {
                throw new Exception($"Failed to initialize parameter.  Could not find the column {Column.Name} in table {table.Name}.");
            }
        }

        public override void SetInputData(object[] data, object[] joinRow = null)
        {
            if (_useJoinTable)
            {
                SetValue(joinRow?[_rowOrdinal]);
            }
            else
            {
                SetValue(data?[_rowOrdinal]);    
            }
            
        }

        public override void PopulateRowData(object value, object[] data, object[] joinRow = null)
        {
            SetValue(value);
            if (_useJoinTable)
            {
                joinRow[_rowOrdinal] = Value;
            }
            else
            {
                data[_rowOrdinal] = Value;
            }
        }
        
        public override Parameter Copy()
        {
            return new ParameterColumn(Name, Column);
        }

        public override IEnumerable<TableColumn> GetRequiredColumns()
        {
            if (Column != null)
            {
                return new[] {Column};
            }

            return new TableColumn[0];
        }
    }
}