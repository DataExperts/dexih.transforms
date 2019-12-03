using System;
using System.Collections.Generic;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.functions.Parameter
{
    [MessagePackObject]
    public class ParameterColumn : Parameter
    {
        public ParameterColumn()
        {
        }

        /// <summary>
        /// Initializes are parameter
        /// </summary>
        /// <param name="name">Paramter name</param>
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
            ETypeCode dataType,
            int rank,
            TableColumn column
        )
        {
            Name = name;
            DataType = dataType;
            Rank = rank;
            Column = column;
        }

        public ParameterColumn(string columnName, ETypeCode dataType)
        {
            Name = columnName;
            DataType = dataType;
            Column = new TableColumn(columnName, dataType);
        }

        [Key(0)]
        public TableColumn Column;

        /// <summary>
        /// The index of the datarow to get the value from.
        /// </summary>
        private int _rowOrdinal = -1;

        private bool _useJoinTable;

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

        public override IEnumerable<SelectColumn> GetRequiredColumns()
        {
            if (Column != null)
            {
                return new[] {new SelectColumn(Column)};
            }

            return new SelectColumn[0];
        }
    }
}