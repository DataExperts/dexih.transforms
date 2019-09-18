using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using MessagePack;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class Sort : IEquatable<Sort>
    {
        // [JsonConverter(typeof(StringEnumConverter))]
        public enum EDirection
        {
            Ascending = 1,
            Descending
        }

        public Sort()
        {
        }

        public Sort(TableColumn column, EDirection direction = EDirection.Ascending)
        {
            Column = column;
            Direction = direction;
        }

        public Sort(string columnName, EDirection direction = EDirection.Ascending)
        {
            Column = new TableColumn(columnName);
            Direction = direction;
        }

        [Key(0)]
        public TableColumn Column { get; set; }

        [Key(1)]
        public EDirection Direction { get; set; }

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
