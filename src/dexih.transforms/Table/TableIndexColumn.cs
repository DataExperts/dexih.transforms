using System.Runtime.Serialization;

namespace dexih.functions
{
    [DataContract]
    public class TableIndexColumn
    {
        [DataMember(Order = 0)]
        public string ColumnName { get; set; }

        [DataMember(Order = 1)] 
        public ESortDirection Direction { get; set; } = ESortDirection.Ascending;

        public TableIndexColumn() {}
        
        public TableIndexColumn(string columnName, ESortDirection direction = ESortDirection.Ascending)
        {
            ColumnName = columnName;
            Direction = direction;
        }
    }
}