using System;


using MessagePack;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class SelectColumn: IEquatable<SelectColumn>
    {
        public SelectColumn() { }

        public SelectColumn(TableColumn column)
        {
            Column = column;
            Aggregate = EAggregate.None;
        }

        public SelectColumn(TableColumn column, EAggregate aggregate, TableColumn outputColumn)
        {
            Column = column;
            Aggregate = aggregate;
            OutputColumn = outputColumn;
        }

        public SelectColumn(string columnName)
        {
            Column = new TableColumn(columnName);
            Aggregate = EAggregate.None;
        }

        public SelectColumn(string columnName, EAggregate aggregate)
        {
            Column = new TableColumn(columnName);
            Aggregate = aggregate;
            OutputColumn = new TableColumn(columnName);
        }
        public SelectColumn(string columnName, EAggregate aggregate, string outputColumnName)
        {
            Column = new TableColumn(columnName);
            Aggregate = aggregate;
            OutputColumn = new TableColumn(outputColumnName);
        }

        // [JsonConverter(typeof(StringEnumConverter))]
        public enum EAggregate
        {
            None = 0,
            Sum,
            Average,
            Min,
            Max,
            Count,
            First,
            Last,
        }

        [Key(0)]
        public TableColumn Column { get; set; }

        [Key(1)] 
        public EAggregate Aggregate { get; set; } = EAggregate.None;

        [Key(2)] 
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