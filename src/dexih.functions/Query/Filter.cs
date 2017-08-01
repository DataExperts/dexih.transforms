using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{
 public class Filter
    {
        public Filter() { }

        /// <summary>
        /// Converts a standard function to a filter object.
        /// </summary>
        /// <param name="function"></param>
        public static ReturnValue<Filter> GetFilterFromFunction(Function function)
        {
            if (function.ReturnType != DataType.ETypeCode.Boolean)
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

            var filter = new Filter();

            filter.Column1 = function.Inputs[0].IsColumn ? function.Inputs[0].Column : null;
            filter.Value1 = function.Inputs[0].IsColumn == false ? function.Inputs[0].Value : null;
            filter.Column2 = function.Inputs[1].IsColumn ? function.Inputs[1].Column : null;
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
                CompareDataType = DataType.ETypeCode.String;
            else if(Value2.GetType().IsArray)
                CompareDataType = DataType.GetTypeCode(Value2.GetType().GetElementType());
            else
                CompareDataType = DataType.GetTypeCode(Value2.GetType());
        }

        public Filter(string columnName1, ECompare operator1, object value2)
        {
            Operator = operator1;
            Value2 = value2;

            if (Value2 == null)
                CompareDataType = DataType.ETypeCode.String;
            else if (Value2.GetType().IsArray)
                CompareDataType = DataType.GetTypeCode(Value2.GetType().GetElementType());
            else
                CompareDataType = DataType.GetTypeCode(Value2.GetType());

            Column1 = new TableColumn(columnName1, CompareDataType);
        }

        /// <summary>
        /// Sets a simple filter comparing two columns against each other.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="column2">Static value to compare to</param>
        /// <param name="dataType">Data type of the column</param>
        public Filter(TableColumn column1, ECompare operator1, TableColumn column2, DataType.ETypeCode dataType)
        {
            Column1 = column1;
            Operator = operator1;
            Column2 = column2;
            CompareDataType = dataType;
        }

        public Filter(string columnName1, ECompare operator1, string columnName2, DataType.ETypeCode dataType)
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
        public DataType.ETypeCode CompareDataType { get; set; }

        public TableColumn Column2 { get; set; }
        public object Value2 { get; set; }


        public ECompare Operator { get; set; }
        public EAndOr AndOr { get; set; }

        public bool Evaluate(object column1Value, object column2Value)
        {
            var value1 = Column1 == null ? Value1 : column1Value;
            var value2 = Column2 == null ? Value2 : column2Value;

            var compareResult = DataType.Compare(CompareDataType, value1, value2);

            if (!compareResult.Success)
            {
                return false;
            }

            switch (Operator)
            {
                case ECompare.IsEqual:
                    return compareResult.Value == DataType.ECompareResult.Equal;
                case ECompare.GreaterThan:
                    return compareResult.Value == DataType.ECompareResult.Greater;
                case ECompare.GreaterThanEqual:
                    return compareResult.Value == DataType.ECompareResult.Greater || compareResult.Value == DataType.ECompareResult.Equal;
                case ECompare.LessThan:
                    return compareResult.Value == DataType.ECompareResult.Less;
                case ECompare.LessThanEqual:
                    return compareResult.Value == DataType.ECompareResult.Less || compareResult.Value == DataType.ECompareResult.Equal;
                case ECompare.NotEqual:
                    return compareResult.Value == DataType.ECompareResult.Equal;
                case ECompare.IsIn:
                    throw new Exception("The IsIn is not currently supported in the query evaluation.");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

        }
    }
}