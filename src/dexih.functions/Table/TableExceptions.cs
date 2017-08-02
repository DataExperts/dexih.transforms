using System;

namespace dexih.functions
{
    public class TableException : Exception
    {
        public TableException()
        {
        }
        public TableException(string message) : base(message)
        {
        }
        public TableException(string message, Exception innerException): base(message, innerException)
		{
        }
    }

    public class TableDuplicateColumnNameException : TableException
    {
        public TableColumn Column { get; set; }
        public Table Table { get; set; }

        public TableDuplicateColumnNameException(Table table, TableColumn column) :
            base($"A column with the name {column.Name} already exists in the table {table.Name}.")
        {
            Column = column;
            Table = table;
        }

        public TableDuplicateColumnNameException(Table table, string columnName) :
            base($"A column with the name {columnName} already exists in the table {table.Name}.")
        {
            Table = table;
        }
    }
}
