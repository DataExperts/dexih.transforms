using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{
    public class Sort
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EDirection
        {
            Ascending,
            Descending
        }

        public Sort() { }

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
        public TableColumn Column { get; set; }
        public EDirection Direction { get; set; }
    }
}
