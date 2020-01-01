using System;


using MessagePack;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class Sort : IEquatable<Sort>
    {
        // [JsonConverter(typeof(StringEnumConverter))]


        public Sort()
        {
        }

        public Sort(TableColumn column, ESortDirection direction = ESortDirection.Ascending)
        {
            Column = column;
            Direction = direction;
        }

        public Sort(string columnName, ESortDirection direction = ESortDirection.Ascending)
        {
            Column = new TableColumn(columnName);
            Direction = direction;
        }

        [Key(0)]
        public TableColumn Column { get; set; }

        [Key(1)]
        public ESortDirection Direction { get; set; }

        public bool Equals(Sort other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Column, other.Column) && Direction == other.Direction;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Sort)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Column != null ? Column.GetHashCode() : 0) * 397) ^ (int)Direction;
            }
        }
    }
}
