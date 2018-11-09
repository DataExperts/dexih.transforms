using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;
using System.Collections;
using System.Linq;
using CsvHelper;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using Dexih.Utils.DataType;

namespace dexih.functions.Query
{
 public class Filter
    {
        public Filter() { }

        /// <summary>
        /// Converts a standard function to a filter object.
        /// </summary>
        /// <param name="mapFunction"></param>
        public static Filter GetFilterFromFunction(MapFunction mapFunction)
        {
//            if (mapFunction.Function.Parameters.ReturnParameter.DataType != ETypeCode.Boolean)
//            {
//                throw new QueryException(
//                    $"The function {mapFunction.Function.FunctionName} does not have a return type of boolean and cannot be used as a filter.");
//            }

            if (mapFunction.Function.CompareEnum == null)
            {
                return null;
            }

            var inputsArray = mapFunction.Parameters.Inputs.ToArray();
            if (inputsArray.Length != 2)
            {
                return null;
            }
            
            var compare = (ECompare) mapFunction.Function.CompareEnum;

            var filter = new Filter
            {
                
                Column1 = inputsArray[0] is ParameterColumn parameterColumn1 ? parameterColumn1.Column : null,
                Value1 = inputsArray[0] is ParameterColumn parameterValue1 ? parameterValue1.Value : null,
                Column2 = inputsArray[1] is ParameterColumn parameterColumn2 ? parameterColumn2.Column : null,
                Value2 = inputsArray[1] is ParameterColumn parameterValue2 ? parameterValue2.Value : null,
                CompareDataType = inputsArray[0].DataType,
                Operator = compare
            };

            return filter;
        }
        
        /// <summary>
        /// Sets a simple filter comparing a column against a static value.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="column2">Column to compare with</param>
        public Filter(TableColumn column1, ECompare operator1, TableColumn column2)
        {
            Column1 = column1;
            Operator = operator1;
            Column2 = column2;

            if (Column2 == null)
                CompareDataType = ETypeCode.String;
            else
                CompareDataType = column2.DataType;
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
                CompareDataType = GetTypeCode(Value2.GetType().GetElementType(), out _);
            else
                CompareDataType = GetTypeCode(Value2.GetType(), out _);
        }

        public Filter(string columnName1, ECompare operator1, object value2)
        {
            Operator = operator1;
            Value2 = value2;

            if (Value2 == null)
                CompareDataType = ETypeCode.String;
            else if (Value2.GetType().IsArray)
                CompareDataType = GetTypeCode(Value2.GetType().GetElementType(), out _);
            else
                CompareDataType = GetTypeCode(Value2.GetType(), out _);

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
            IsIn,
            IsNull,
            IsNotNull
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

        public ETypeCode BestDataType()
        {
            var typeCode = ETypeCode.String;

            if (Column1 != null)
            {
                typeCode = Column1.DataType;
            } else if (Column2 != null)
            {
                typeCode = Column2.DataType;
            }
            else
            {
                typeCode = CompareDataType;
            }

            return typeCode;
        }

        public bool Evaluate(object column1Value, object column2Value)
        {
            var value1 = Column1 == null ? Value1 : column1Value;
            var value2 = Column2 == null ? Value2 : column2Value;

            var parsedValue1 = Operations.Parse(CompareDataType, value1);

            if(Operator == ECompare.IsIn && Value2.GetType().IsArray)
            {
                foreach(var value in (IEnumerable)Value2)
                {
                    var parsedValue = Operations.Parse(CompareDataType, value);
                    var compare = Operations.Equal(CompareDataType, parsedValue1, parsedValue);
                    if(compare)
                    {
                        return true;
                    }
                }
                return false;
            }
            
            var parsedValue2 = Operations.Parse(CompareDataType, value2);


            if (Operator == ECompare.IsNull)
            {
                return parsedValue1 == null || parsedValue1 is DBNull;
            }

            if (Operator == ECompare.IsNotNull)
            {
                return parsedValue1 != null && !(parsedValue1 is DBNull);
            }

            switch (Operator)
            {
                case ECompare.IsEqual:
                    return Operations.Equal(CompareDataType, parsedValue1, parsedValue2);
                case ECompare.GreaterThan:
                    return Operations.GreaterThan(CompareDataType, parsedValue1, parsedValue2);
                case ECompare.GreaterThanEqual:
                    return Operations.GreaterThanOrEqual(CompareDataType, parsedValue1, parsedValue2);
                case ECompare.LessThan:
                    return Operations.LessThan(CompareDataType, parsedValue1, parsedValue2);
                case ECompare.LessThanEqual:
                    return Operations.LessThanOrEqual(CompareDataType, parsedValue1, parsedValue2);
                case ECompare.NotEqual:
                    return !Operations.Equal(CompareDataType, parsedValue1, parsedValue2);
                default:
                    throw new QueryException($"The {Operator} is not currently supported in the query evaluation.");
            }

        }
    }
}