using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;
using System.Collections;

namespace dexih.functions.Query
{
 public class Filter
    {
        public Filter() { }

        /// <summary>
        /// Converts a standard function to a filter object.
        /// </summary>
        /// <param name="transformFunction"></param>
        public static Filter GetFilterFromFunction(TransformFunction transformFunction)
        {
            if (transformFunction.ReturnType != ETypeCode.Boolean)
                throw new QueryException($"The function {transformFunction.FunctionName} does not have a return type of boolean and cannot be used as a filter.");

            if (transformFunction.CompareEnum == null)
            {
                return null;
            }
            var compare = (ECompare) transformFunction.CompareEnum;

            var filter = new Filter
            {
                Column1 = transformFunction.Inputs[0].IsColumn ? transformFunction.Inputs[0].Column : null,
                Value1 = transformFunction.Inputs[0].IsColumn == false ? transformFunction.Inputs[0].Value : null,
                Column2 = transformFunction.Inputs[1].IsColumn ? transformFunction.Inputs[1].Column : null,
                Value2 = transformFunction.Inputs[1].IsColumn == false ? transformFunction.Inputs[1].Value : null,

                CompareDataType = transformFunction.Inputs[0].IsColumn ? transformFunction.Inputs[0].DataType : transformFunction.Inputs[1].DataType,
                Operator = compare
            };

            return filter;
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

        public bool Evaluate(object column1Value, object column2Value)
        {
            var value1 = Column1 == null ? Value1 : column1Value;
            var value2 = Column2 == null ? Value2 : column2Value;

            if(Operator == ECompare.IsIn && Value2.GetType().IsArray)
            {
                foreach(var value in (IEnumerable)Value2)
                {
                    var compare = Compare(CompareDataType, value1, value);
                    if(compare == ECompareResult.Equal)
                    {
                        return true;
                    }
                }
                return false;
            }

            var compareResult = Compare(CompareDataType, value1, value2);

            switch (Operator)
            {
                case ECompare.IsEqual:
                    return compareResult == ECompareResult.Equal;
                case ECompare.GreaterThan:
                    return compareResult == ECompareResult.Greater;
                case ECompare.GreaterThanEqual:
                    return compareResult == ECompareResult.Greater || compareResult == ECompareResult.Equal;
                case ECompare.LessThan:
                    return compareResult == ECompareResult.Less;
                case ECompare.LessThanEqual:
                    return compareResult == ECompareResult.Less || compareResult == ECompareResult.Equal;
                case ECompare.NotEqual:
                case ECompare.IsIn:
                    throw new Exception("The IsIn is not currently supported in the query evaluation.");
                default:
                    throw new QueryException($"The {Operator} is not currently supported in the query evaluation.");
            }

        }
    }
}