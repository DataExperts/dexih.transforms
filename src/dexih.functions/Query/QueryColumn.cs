using ProtoBuf;
using System;

namespace dexih.functions.Query
{
    [ProtoContract]
    public class QueryColumn: IEquatable<QueryColumn>
    {
        public QueryColumn() { }

        public QueryColumn(TableColumn column, object value)
        {
            Column = column;
            Value = value;
        }

        public QueryColumn(string columnName, object value)
        {
            Column = new TableColumn(columnName);
            Value = value;
        }

        [ProtoMember(1)]
        public TableColumn Column { get; }

        [ProtoMember(2)]
        public object Value { get; }

        public bool Equals(QueryColumn other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Column, other.Column) && Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((QueryColumn) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Column != null ? Column.GetHashCode() : 0) * 397) ^ (Value != null ? Value.GetHashCode() : 0);
            }
        }
    }
}