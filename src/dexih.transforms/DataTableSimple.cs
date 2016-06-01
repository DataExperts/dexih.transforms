using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static dexih.functions.DataType;

namespace dexih.transforms
{
    public class DataTableSimple
    {
        public string TableName {get;set;}
        public DataTableColumns Columns { get; set; }
        public List<object[]> Data { get; set; }

        public DataTableSimple()
        {

        }
        public DataTableSimple(string tableName, DataTableColumns columns, List<object[]> data )
        {
            TableName = tableName;
            Columns = columns;
            Data = data;
        }

        public DataTableSimple(string tableName, DataTableColumns columns)
        {
            TableName = tableName;
            Columns = columns;
            Data = new List<object[]>();
        }
    }

    public class DataTableColumns : List<DataTableColumn>
    {
        public DataTableColumn this[string ColumnName] => this.SingleOrDefault(c=>c.ColumnName == ColumnName);

        public int GetOrdinal(string ColumnName)
        {
            return this.FindIndex(c => c.ColumnName == ColumnName);
        }

        public void Add(string columnName)
        {
            this.Add(new DataTableColumn(columnName, ETypeCode.String));
        }

        public void Add(string columnName, ETypeCode dataType)
        {
            this.Add(new DataTableColumn(columnName, dataType));
        }
    }

    public class DataTableColumn
    {
        public string ColumnName { get; set; }
        public ETypeCode DataType { get; set; }

        public DataTableColumn(string columnName, ETypeCode dataType)
        {
            ColumnName = columnName;
            DataType = dataType;
        }

    }
}
