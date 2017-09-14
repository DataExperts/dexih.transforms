using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions.Query
{
    public class SelectColumn
    {
        public SelectColumn() { }

        public SelectColumn(TableColumn column)
        {
            Column = column;
            Aggregate = EAggregate.None;
        }

        public SelectColumn(TableColumn column, EAggregate aggregate)
        {
            Column = column;
            Aggregate = aggregate;
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
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EAggregate
        {
            None,
            Sum,
            Average,
            Min,
            Max,
            Count
        }
        public TableColumn Column { get; set; }
        public EAggregate Aggregate { get; set; }

    }
}