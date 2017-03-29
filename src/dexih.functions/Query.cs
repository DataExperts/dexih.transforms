using dexih.functions;
using System;
using System.Collections.Generic;
using System.Data.Common;
using static dexih.functions.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{
    public class UpdateQueries
    {
        public UpdateQuery BaseUpdateQuery { get; set; }
        public List<object[]> Data { get; set; }

    }

    public class SelectQuery
    {
        public SelectQuery()
        {
            Columns = new List<SelectColumn>();
            Filters = new List<Filter>();
            Sorts = new List<Sort>();
            Groups = new List<TableColumn>();
            Rows = -1; //-1 means show all rows.
        }

        public List<SelectColumn> Columns { get; set; }
        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
        public List<Sort> Sorts { get; set; }
        public List<TableColumn> Groups { get; set; }
        public int Rows { get; set; }

     }

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

    public class UpdateQuery
    {
        public UpdateQuery(string table, List<QueryColumn> updateColumns, List<Filter> filters)
        {
            Table = table;
            UpdateColumns = updateColumns;
            Filters = filters;
        }

        public UpdateQuery()
        {
            UpdateColumns = new List<QueryColumn>();
            Filters = new List<Filter>();
        }

        public List<QueryColumn> UpdateColumns { get; set; }
        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
    }

    public class DeleteQuery
    {
        public DeleteQuery(string table, List<Filter> filters)
        {
            Table = table;
            Filters = filters;
        }
        public DeleteQuery()
        {
            Filters = new List<Filter>();
        }

        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
    }

    public class InsertQuery
    {
        public InsertQuery(string table, List<QueryColumn> insertColumns)
        {
            Table = table;
            InsertColumns = insertColumns;
        }

        public string Table { get; set; }
        public List<QueryColumn> InsertColumns { get; set; }
    }

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

    public class Filter
    {
        public Filter() { }

        /// <summary>
        /// Converts the function to a filter object.
        /// </summary>
        /// <param name="function"></param>
        public static ReturnValue<Filter> GetFilterFromFunction(Function function)
        {
            if (function.ReturnType != ETypeCode.Boolean)
                return new ReturnValue<Filter>(false, "The function did not have a return type of boolean.", null);

            ECompare compare;

            switch(function.FunctionName)
            {
                case "IsEqual":
                    compare = ECompare.IsEqual;
                    break;
                case "LessThan":
                    compare = ECompare.LessThan;
                    break;
                case "LessThanEqual":
                    compare = ECompare.LessThanEqual;
                    break;
                case "GreaterThan":
                    compare = ECompare.GreaterThan;
                    break;
                case "GreaterThanEqual":
                    compare = ECompare.GreaterThanEqual;
                    break;
                default:
                    return new ReturnValue<Filter>(false, "The function " + function.FunctionName + " was not converted.", null);
            }

            Filter filter = new Filter();

            filter.Column1 = function.Inputs[0].IsColumn == true ? function.Inputs[0].Column : null;
            filter.Value1 = function.Inputs[0].IsColumn == false ? function.Inputs[0].Value : null;
            filter.Column2 = function.Inputs[1].IsColumn == true ? function.Inputs[1].Column : null;
            filter.Value2 = function.Inputs[1].IsColumn == false ? function.Inputs[1].Value : null;

            filter.CompareDataType = function.Inputs[0].IsColumn ? function.Inputs[0].DataType : function.Inputs[1].DataType;
            filter.Operator = compare;

            return new ReturnValue<Filter>(true, filter);
        }

        /// <summary>
        /// Sets a simple filter comparing a column against a static value.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="value2">Static value to compare to</param>
        public Filter(TableColumn column1, ECompare operator1, object value2)
        {
            Column1 = column1;
            Operator = operator1;
            Value2 = value2;

            if (Value2 == null)
                CompareDataType = ETypeCode.String;
            else if(Value2.GetType().IsArray)
                CompareDataType = GetTypeCode(Value2.GetType().GetElementType());
            else
                CompareDataType = GetTypeCode(Value2.GetType());
        }

        public Filter(string columnName1, ECompare operator1, object value2)
        {
            Operator = operator1;
            Value2 = value2;

            if (Value2 == null)
                CompareDataType = ETypeCode.String;
            else if (Value2.GetType().IsArray)
                CompareDataType = GetTypeCode(Value2.GetType().GetElementType());
            else
                CompareDataType = GetTypeCode(Value2.GetType());

            Column1 = new TableColumn(columnName1, CompareDataType);
        }

        /// <summary>
        /// Sets a simple filter comparing two columns against each other.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="column2">Static value to compare to</param>
        /// <param name="dataType">Data type of the column</param>
        public Filter(TableColumn column1, ECompare operator1, TableColumn column2, ETypeCode dataType)
        {
            Column1 = column1;
            Operator = operator1;
            Column2 = column2;
            CompareDataType = dataType;
        }

        public Filter(string columnName1, ECompare operator1, string columnName2, ETypeCode dataType)
        {
            Column1 = new TableColumn(columnName1);
            Operator = operator1;
            Column2 = new TableColumn(columnName2);
            CompareDataType = dataType;
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ECompare
        {
            IsEqual,
            GreaterThan,
            GreaterThanEqual,
            LessThan,
            LessThanEqual,
            NotEqual,
            IsIn
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EAndOr
        {
            And, Or
        }

        public TableColumn Column1 { get; set; }
        public object Value1 { get; set; }
        public ETypeCode CompareDataType { get; set; }

        public TableColumn Column2 { get; set; }
        public object Value2 { get; set; }


        public ECompare Operator { get; set; }
        public EAndOr AndOr { get; set; }
    }

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
