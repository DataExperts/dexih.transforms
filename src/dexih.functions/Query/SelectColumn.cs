using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.functions.Query
{
    [ProtoContract]
    public class SelectColumn: IEquatable<SelectColumn>
    {
        public SelectColumn() { }

        public SelectColumn(TableColumn column)
        {
            Column = column;
            Aggregate = null;
        }

        public SelectColumn(TableColumn column, EAggregate aggregate)
        {
            Column = column;
            Aggregate = aggregate;
        }

        public SelectColumn(string columnName)
        {
            Column = new TableColumn(columnName);
            Aggregate = null;
        }

        public SelectColumn(string columnName, EAggregate aggregate)
        {
            Column = new TableColumn(columnName);
            Aggregate = aggregate;
        }

        // [JsonConverter(typeof(StringEnumConverter))]
        public enum EAggregate
        {
            Sum = 1,
            Average,
            Min,
            Max,
            Count,
            First,
            Last,
        }

        [ProtoMember(1)]
        public TableColumn Column { get; set; }

        [ProtoMember(2)]
        public EAggregate? Aggregate { get; set; }

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