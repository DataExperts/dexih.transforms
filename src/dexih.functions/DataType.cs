using System;

namespace dexih.functions
{
    public static class DataType
    {

        /// <summary>
        /// A simplified list of primary possible datatypes.
        /// </summary>
        public enum EBasicType
        {
            Unknown,
            String,
            Numeric,
            Date,
            Time,
            Boolean
        }


        /// <summary>
        /// List of supported type codes.  This is a cutdown version of the standard "typecode" enum.
        /// </summary>
        public enum ETypeCode
        {
            Byte,
            SByte,
            UInt16,
            UInt32,
            UInt64,
            Int16,
            Int32,
            Int64,
            Decimal,
            Double,
            Single, 
            String, 
            Boolean, 
            DateTime,
            Time,
            Unknown
        }


        /// <summary>
        /// Converts a datatype to a simplified basic type.
        /// </summary>
        /// <param name="dataType">Data Type</param>
        /// <returns>Basic Datatype</returns>
        public static EBasicType GetBasicType(ETypeCode dataType)
        {
            switch (dataType)
            {
                case ETypeCode.Byte:
                case ETypeCode.SByte:
                case ETypeCode.UInt16:
                case ETypeCode.UInt32:
                case ETypeCode.UInt64:
                case ETypeCode.Int16:
                case ETypeCode.Int32:
                case ETypeCode.Int64:
                case ETypeCode.Decimal:
                case ETypeCode.Double:
                case ETypeCode.Single: return EBasicType.Numeric;
                case ETypeCode.String: return EBasicType.String;
                case ETypeCode.Boolean: return EBasicType.Boolean;
                case ETypeCode.DateTime: return EBasicType.Date;
                case ETypeCode.Time: return EBasicType.Time;
                default: return EBasicType.Unknown;
            }
        }

        /// <summary>
        /// Converts a Type into a ETypeCode
        /// </summary>
        /// <param name="dataType">Type value</param>
        /// <returns>ETypeCode</returns>
        public static ETypeCode GetTypeCode(Type dataType)
        {
            if(dataType == typeof(Byte))
                return ETypeCode.Byte;
            if (dataType == typeof(SByte))
                return ETypeCode.SByte;
            if (dataType == typeof(UInt16))
                return ETypeCode.UInt16;
            if (dataType == typeof(UInt32))
                return ETypeCode.UInt32;
            if (dataType == typeof(UInt64))
                return ETypeCode.UInt64;
            if (dataType == typeof(Int16))
                return ETypeCode.Int16;
            if (dataType == typeof(Int32))
                return ETypeCode.Int32;
            if (dataType == typeof(Int64))
                return ETypeCode.Int64;
            if (dataType == typeof(Decimal))
                return ETypeCode.Decimal;
            if (dataType == typeof(Double))
                return ETypeCode.Double;
            if (dataType == typeof(Single))
                return ETypeCode.Single;
            if (dataType == typeof(String))
                return ETypeCode.String;
            if (dataType == typeof(Boolean))
                return ETypeCode.Boolean;
            if (dataType == typeof(DateTime))
                return ETypeCode.DateTime;
            if (dataType == typeof(TimeSpan))
                return ETypeCode.Time;

            return ETypeCode.Unknown;
        }


        /// <summary>
        /// Result of a data comparison
        /// </summary>
        public enum ECompareResult
        {
            Null,
            Greater,
            Less,
            Equal,
            NotEqual
        }

       /// <summary>
       /// Compares two values of the specified typecode and returns a result indicating null, greater, less ,equal, not equal.  for example if inputValue is greater than comparevalue the return will be "Greater".
       /// </summary>
       /// <param name="dataType">data type to compare</param>
       /// <param name="inputValue">primary value</param>
       /// <param name="compareValue">value to compare against</param>
       /// <returns></returns>
        public static ReturnValue<ECompareResult> Compare(ETypeCode dataType, object inputValue, object compareValue)
        {
            try
            {
                if (inputValue == null || compareValue == null || inputValue.ToString() == "" || compareValue.ToString() == "")
                    return new ReturnValue<ECompareResult>(true, ECompareResult.Null);

                var try1 = TryParse(dataType, inputValue);
                if (try1.Success == false)
                    return new ReturnValue<ECompareResult>(false, "Could not parse the value " + inputValue + " as a type " + dataType + ". Reason: " + try1.Message, null);
                var result1 = try1.Value;

                var try2 = TryParse(dataType, compareValue);
                if (try2.Success == false)
                    return new ReturnValue<ECompareResult>(false, "Could not parse the value " + compareValue + " as a type " + dataType + ". Reason: " + try2.Message, null);
                var result2 = try2.Value;

                switch (dataType)
                {
                    case ETypeCode.Byte:
                        return new ReturnValue<ECompareResult>(true, (Byte)result1 == (Byte)result2 ? ECompareResult.Equal : (Byte)result1 > (Byte)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.SByte:
                        return new ReturnValue<ECompareResult>(true, (SByte)result1 == (SByte)result2 ? ECompareResult.Equal : (SByte)result1 > (SByte)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt16:
                        return new ReturnValue<ECompareResult>(true, (UInt16)result1 == (UInt16)result2 ? ECompareResult.Equal : (UInt16)result1 > (UInt16)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt32:
                        return new ReturnValue<ECompareResult>(true, (UInt32)result1 == (UInt32)result2 ? ECompareResult.Equal : (UInt32)result1 > (UInt32)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt64:
                        return new ReturnValue<ECompareResult>(true, (UInt64)result1 == (UInt64)result2 ? ECompareResult.Equal : (UInt64)result1 > (UInt64)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int16:
                        return new ReturnValue<ECompareResult>(true, (Int16)result1 == (Int16)result2 ? ECompareResult.Equal : (Int16)result1 > (Int16)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int32:
                        return new ReturnValue<ECompareResult>(true, (Int32)result1 == (Int32)result2 ? ECompareResult.Equal : (Int32)result1 > (Int32)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int64:
                        return new ReturnValue<ECompareResult>(true, (Int64)result1 == (Int64)result2 ? ECompareResult.Equal : (Int64)result1 > (Int64)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Decimal:
                        return new ReturnValue<ECompareResult>(true, (Decimal)result1 == (Decimal)result2 ? ECompareResult.Equal : (Decimal)result1 > (Decimal)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Double:
                        return new ReturnValue<ECompareResult>(true, Math.Abs((Double)result1 - (Double)result2) < 0.0001 ? ECompareResult.Equal : (Double)result1 > (Double)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Single:
                        return new ReturnValue<ECompareResult>(true, Math.Abs((Single)result1 - (Single)result2) < 0.0001 ? ECompareResult.Equal : (Single)result1 > (Single)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.String:
                        return new ReturnValue<ECompareResult>(true, (string)result1 == (String)result2 ? ECompareResult.Equal : ECompareResult.NotEqual);
                    case ETypeCode.Boolean:
                        return new ReturnValue<ECompareResult>(true, (Boolean)result1 == (Boolean)result2 ? ECompareResult.Equal : ECompareResult.NotEqual);
                    case ETypeCode.DateTime:
                        return new ReturnValue<ECompareResult>(true, (DateTime)result1 == (DateTime)result2 ? ECompareResult.Equal : (DateTime)result1 > (DateTime)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Time:
                        return new ReturnValue<ECompareResult>(true, (TimeSpan)result1 == (TimeSpan)result2 ? ECompareResult.Equal : (DateTime)result1 > (DateTime)result2 ? ECompareResult.Greater : ECompareResult.Less);
                    default:
                        return new ReturnValue<ECompareResult>(false, "Unsupported datatype: " + dataType, null);
                }
            }
            catch(Exception ex)
            {
                return new ReturnValue<ECompareResult>(false, "The compare of " + inputValue.ToString() + " to " + compareValue.ToString() + " failed with the following errro: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Attempts to parse and convert the input value to the specified datatype.
        /// </summary>
        /// <param name="tryDataType">DataType to convert to</param>
        /// <param name="inputValue">Input Value to convert</param>
        /// <returns>True and the converted value for success, false and a message for conversion fail.</returns>
        public static ReturnValue<object> TryParse(ETypeCode tryDataType, object inputValue)
        {
            Object result = null;
            try
            {
                if (inputValue == null)
                {
                    return new ReturnValue<object>(true, null);
                }

                if (tryDataType == ETypeCode.String || tryDataType == ETypeCode.Unknown)
                {
                    result = inputValue is DBNull ? null : inputValue.ToString();
                    return new ReturnValue<object>(true, result);
                }

                if (inputValue is DBNull)
                {
                    result = inputValue;
                    return new ReturnValue<object>(true, result);
                }

                ETypeCode inputType = GetTypeCode(inputValue.GetType());

                if (tryDataType == inputType )
                {
                    result = inputValue;
                    return new ReturnValue<object>(true, result);
                }

                EBasicType tryBasicType = GetBasicType(tryDataType);
                EBasicType inputBasicType = GetBasicType(inputType);

                if(tryBasicType == EBasicType.Numeric && (inputBasicType == EBasicType.Numeric))
                {
                    switch (tryDataType)
                    {
                        case ETypeCode.Byte:
                            result = Convert.ToByte(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Int16:
                            result = Convert.ToInt16(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Int32:
                            result = Convert.ToInt32(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Int64:
                            result = Convert.ToInt64(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Double:
                            result = Convert.ToDouble(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Decimal:
                            result = Convert.ToDecimal(inputValue);
                            return new ReturnValue<object>(true, result);
                        default:
                            string reason = "Cannot convert value " + inputValue + " from numeric to " + tryDataType;
                            return new ReturnValue<object>(false, reason, null);
                    }
                }

                if(tryBasicType == EBasicType.Numeric && inputBasicType == EBasicType.Date)
                {
                    result = ((DateTime)inputValue).Ticks;
                }

                if (tryBasicType == EBasicType.Date && inputBasicType == EBasicType.Numeric)
                {
                    result = new DateTime(Convert.ToInt64(inputValue));
                }

                if (tryBasicType == EBasicType.Date && inputBasicType != EBasicType.String)
                {
                    string reason = "Cannot convert value " + inputValue + " to " + tryDataType;
                    return new ReturnValue<object>(false, reason, null);
                }

                if (inputBasicType == EBasicType.String)
                {
                    string value = (string)inputValue;

                    bool returnValue;
                    switch (tryDataType)
                    {
                        case ETypeCode.Byte:
                            byte byteResult;
                            returnValue = Byte.TryParse(value, out byteResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a byte.", null);
                            result = byteResult;
                            break;
                        case ETypeCode.Int16:
                            Int16 int16Result;
                            returnValue = Int16.TryParse(value, out int16Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int16.", null);
                            result = int16Result;
                            break;
                        case ETypeCode.Int32:
                            Int32 int32Result;
                            returnValue = Int32.TryParse(value, out int32Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int32.", null);
                            result = int32Result;
                            break;
                        case ETypeCode.Int64:
                            byte int64Result;
                            returnValue = Byte.TryParse(value, out int64Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int64.", null);
                            result = int64Result;
                            break;
                        case ETypeCode.Double:
                            Double doubleResult;
                            returnValue = Double.TryParse(value, out doubleResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Double.", null);
                            result = doubleResult;
                            break;
                        case ETypeCode.Decimal:
                            Decimal decimalResult;
                            returnValue = Decimal.TryParse(value, out decimalResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Decimal.", null);
                            result = decimalResult;
                            break;
                        case ETypeCode.String:
                            result = value;
                            break;
                        case ETypeCode.Boolean:
                            Boolean booleanResult;
                            returnValue = Boolean.TryParse(value, out booleanResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a boolean.", null);
                            result = booleanResult;
                            break;
                        case ETypeCode.DateTime:
                            DateTime dateTimeResult;
                            returnValue = DateTime.TryParse(value, out dateTimeResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a DataTime.", null);
                            result = dateTimeResult;
                            break;
                        case ETypeCode.Time:
                            TimeSpan timeResult;
                            returnValue = TimeSpan.TryParse(value, out timeResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a DataTime.", null);
                            result = timeResult;
                            break;
                        default:
                            return new ReturnValue<object>(false, "Cannot convert value " + inputValue + " from to " + tryDataType + ".", null);
                    }
                }

                return new ReturnValue<object>(true, result);
            }
            catch (Exception ex)
            {
                string reason = "Cannot convert value " + (inputValue?.ToString() ?? "null") + " to type: " + tryDataType + ". The following error was returned:" +ex.Message;
                return new ReturnValue<object>(false, reason, ex);
            }

        }
    }
}
