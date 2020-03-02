using System;
using System.Runtime.Serialization;


namespace dexih.functions.Query
{
    [DataContract]
    public class SelectColumn: IEquatable<SelectColumn>
    {
        public SelectColumn() { }

        public SelectColumn(TableColumn column, EAggregate aggregate = EAggregate.None, TableColumn outputColumn = null)
        {
            Column = column;
            Aggregate = aggregate;
            OutputColumn = outputColumn;
        }

        public SelectColumn(string columnName, EAggregate aggregate = EAggregate.None, string outputColumnName = null)
        {
            Column = new TableColumn(columnName);
            Aggregate = aggregate;
            if (!string.IsNullOrEmpty(outputColumnName))
            {
                OutputColumn = new TableColumn(outputColumnName);
            }
        }
        
        [DataMember(Order = 0)]
        public TableColumn Column { get; set; }

        [DataMember(Order = 1)] 
        public EAggregate Aggregate { get; set; } = EAggregate.None;

        [DataMember(Order = 2)] 
        public TableColumn OutputColumn { get; set; }
        
        public string GetOutputName()
        {
            return OutputColumn?.Name ?? Column.Name;
        }

        public bool Equals(SelectColumn other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Column, other.Column) && Aggregate == other.Aggregate;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((SelectColumn) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Column != null ? Column.GetHashCode() : 0) * 397) ^ Aggregate.GetHashCode();
            }
        }
    }
}