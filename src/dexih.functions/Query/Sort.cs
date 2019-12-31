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

        public Sort(TableColumn column, ESortDirection sortDirection = ESortDirection.Ascending)
        {
            Column = column;
            SortDirection = sortDirection;
        }

        public Sort(string columnName, ESortDirection sortDirection = ESortDirection.Ascending)
        {
            Column = new TableColumn(columnName);
            SortDirection = sortDirection;
        }

        [Key(0)]
        public TableColumn Column { get; set; }

        [Key(1)]
        public ESortDirection SortDirection { get; set; }

        public bool Equals(Sort other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Column, other.Column) && SortDirection == other.SortDirection;
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
                return ((Column != null ? Column.GetHashCode() : 0) * 397) ^ (int)SortDirection;
            }
        }
    }
}
