using dexih.functions;
using System;
using System.Collections.Generic;
using System.Data.Common;
using static dexih.functions.DataType;

namespace dexih.transforms
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
            Groups = new List<string>();
            Rows = -1; //-1 means show all rows.
        }

        public List<SelectColumn> Columns { get; set; }
        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
        public List<Sort> Sorts { get; set; }
        public List<string> Groups { get; set; }
        public int Rows { get; set; }

     }

    public class SelectColumn
    {
        public SelectColumn() { }

        public SelectColumn(string column)
        {
            Column = column;
            Aggregate = EAggregate.None;
        }

        public SelectColumn(string column, EAggregate aggregate)
        {
            Column = column;
            Aggregate = aggregate;
        }
        public enum EAggregate
        {
            None,
            Sum,
            Average,
            Min,
            Max,
            Count
        }
        public string Column { get; set; }
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

        public QueryColumn(string column, ETypeCode columnType, object value)
        {
            Column = column;
            ColumnType = columnType;
            Value = value;
        }

        public string Column { get; set; }
        public object Value { get; set; }
        public ETypeCode ColumnType { get; set; }
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

            filter.Column1 = function.Inputs[0].IsColumn == true ? function.Inputs[0].ColumnName : null;
            filter.Value1 = function.Inputs[0].IsColumn == false ? function.Inputs[0].Value : null;
            filter.Column2 = function.Inputs[1].IsColumn == true ? function.Inputs[1].ColumnName : null;
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
        public Filter(string column1, ECompare operator1, object value2)
        {
            Column1 = column1;
            Operator = operator1;
            Value2 = value2;

            CompareDataType = DataType.GetTypeCode(Value2.GetType());
        }

        /// <summary>
        /// Sets a simple filter comparing two columns against each other.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="column2">Static value to compare to</param>
        /// <param name="dataType">Data type of the column</param>
        public Filter(string column1, ECompare operator1, string column2, ETypeCode dataType)
        {
            Column1 = column1;
            Operator = operator1;
            Column2 = column2;
            CompareDataType = dataType;
        }

        public enum ECompare
        {
            IsEqual,
            GreaterThan,
            GreaterThanEqual,
            LessThan,
            LessThanEqual,
            NotEqual
        }

        public enum EAndOr
        {
            And, Or
        }

        public string Column1 { get; set; }
        public object Value1 { get; set; }
        public ETypeCode CompareDataType { get; set; }

        public string Column2 { get; set; }
        public object Value2 { get; set; }


        public ECompare Operator { get; set; }
        public EAndOr AndOr { get; set; }
    }

    public class Sort
    {
        public enum EDirection
        {
            Ascending,
            Descending
        }

        public Sort() { }

        public Sort(string column, EDirection direction = EDirection.Ascending)
        {
            Column = column;
            Direction = direction;
        }

        public string Column { get; set; }
        public EDirection Direction { get; set; }
    }
}
