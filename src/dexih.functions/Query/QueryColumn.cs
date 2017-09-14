namespace dexih.functions.Query
{
    public class QueryColumn
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

        public TableColumn Column { get; set; }
        public object Value { get; set; }
    }
}