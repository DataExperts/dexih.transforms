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

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EAggregate
        {
            Sum,
            Average,
            Min,
            Max,
            Count,
            First,
            Last
        }
        public TableColumn Column { get; set; }
        public EAggregate? Aggregate { get; set; }

    }
}