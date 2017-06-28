using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections;
using System.Data;
using System.Linq;

namespace dexih.functions
{
    public static class DataType
    {
        [JsonConverter(typeof(StringEnumConverter))]
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
            Boolean,
            Binary
        }

        [JsonConverter(typeof(StringEnumConverter))]
        /// <summary>
        /// List of supported type codes.  This is a cutdown version of the standard "typecode" enum.
        /// </summary>
        public enum ETypeCode
        {
            Binary,
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
            Guid,
            Unknown
        }

        public static object GetDataTypeMaxValue(ETypeCode typeCode, int length = 0)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return Byte.MaxValue;
                case ETypeCode.SByte:
                    return sbyte.MaxValue;
                case ETypeCode.UInt16:
                    return UInt16.MaxValue;
                case ETypeCode.UInt32:
                    return UInt32.MaxValue;
                case ETypeCode.UInt64:
                    return Int64.MaxValue; //use max value of int64 as some databases don't support uint64 (namely postgreSql)
                case ETypeCode.Int16:
                    return Int16.MaxValue;
				case ETypeCode.Int32:
                    return Int32.MaxValue;
				case ETypeCode.Int64:
                    return Int64.MaxValue;
				case ETypeCode.Decimal:
                    return 999999999999999999; //use arbitrary big number as range varies between databases.  
				case ETypeCode.Double:
                    return Double.MaxValue / 10;
				case ETypeCode.Single:
                    return Single.MaxValue /10;
				case ETypeCode.String:
                    return new string('A', length);
                case ETypeCode.Boolean:
                    return true;
                case ETypeCode.DateTime:
                    return DateTime.Now; //DateTime.MaxValue;
				case ETypeCode.Time:
                    return TimeSpan.FromDays(1) - TimeSpan.FromMilliseconds(1);
                case ETypeCode.Guid:
                    return Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF");
                case ETypeCode.Binary:
                    return new byte[] { Byte.MaxValue, Byte.MaxValue, Byte.MaxValue };
                case ETypeCode.Unknown:
                    return "";
                default:
                    return typeof(object);
            }
        }

        public static object GetDataTypeMinValue(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return Byte.MinValue;
				case ETypeCode.SByte:
                    return sbyte.MinValue;
				case ETypeCode.UInt16:
                    return UInt16.MinValue;
				case ETypeCode.UInt32:
                    return UInt32.MinValue;
				case ETypeCode.UInt64:
                    return UInt64.MinValue;
				case ETypeCode.Int16:
                    return Int16.MinValue;
				case ETypeCode.Int32:
                    return Int32.MinValue;
				case ETypeCode.Int64:
                    return Int64.MinValue;
				case ETypeCode.Decimal:
                    return -999999999999999999;
                case ETypeCode.Double:
                    return Double.MinValue / 10;
                case ETypeCode.Single:
                    return Single.MinValue / 10;
                case ETypeCode.String:
                    return "";
                case ETypeCode.Boolean:
                    return false;
                case ETypeCode.DateTime:
                    return new DateTime(1753,01,01);
                case ETypeCode.Time:
                    return TimeSpan.FromDays(0);
                case ETypeCode.Guid:
                    return Guid.Parse("00000000-0000-0000-0000-000000000000");
                case ETypeCode.Binary:
                    return new byte[] { Byte.MinValue, Byte.MinValue, Byte.MinValue };
                case ETypeCode.Unknown:
                    return "";
                default:
                    return typeof(object);
            }
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
                case ETypeCode.Guid:
                case ETypeCode.String: return EBasicType.String;
                case ETypeCode.Boolean: return EBasicType.Boolean;
                case ETypeCode.DateTime: return EBasicType.Date;
                case ETypeCode.Time: return EBasicType.Time;
                case ETypeCode.Binary: return EBasicType.Binary;
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
            if (dataType == typeof(Guid))
                return ETypeCode.Guid;
            if (dataType == typeof(byte[]))
                return ETypeCode.Binary;

            return ETypeCode.Unknown;
        }

        public static Type GetType(ETypeCode typeCode)
        {
            switch(typeCode)
            {
                case ETypeCode.Byte:
                    return typeof(Byte);
                case ETypeCode.SByte:
                    return typeof(SByte);
                case ETypeCode.UInt16:
                    return typeof(UInt16);
                case ETypeCode.UInt32:
                    return typeof(UInt32);
                case ETypeCode.UInt64:
                    return typeof(UInt64);
                case ETypeCode.Int16:
                    return typeof(Int16);
                case ETypeCode.Int32:
                    return typeof(Int32);
                case ETypeCode.Int64:
                    return typeof(Int64);
                case ETypeCode.Decimal:
                    return typeof(Decimal);
                case ETypeCode.Double:
                    return typeof(Double);
                case ETypeCode.Single: 
                    return typeof(Single);
                case ETypeCode.String: 
                    return typeof(String);
                case ETypeCode.Boolean: 
                    return typeof(Boolean);
                case ETypeCode.DateTime: 
                    return typeof(DateTime);
                case ETypeCode.Time: 
                    return typeof(TimeSpan);
                case ETypeCode.Guid:
                    return typeof(Guid);
                case ETypeCode.Binary:
                    return typeof(byte[]);
                default:
                    return typeof(object);
            }
        }

        public static DbType GetDbType(ETypeCode typeCode)
        {
            switch (typeCode)
            {
                case ETypeCode.Byte:
                    return DbType.Byte;
                case ETypeCode.SByte:
                    return DbType.SByte;
                case ETypeCode.UInt16:
                    return DbType.UInt16;
                case ETypeCode.UInt32:
                    return DbType.UInt32;
                case ETypeCode.UInt64:
                    return DbType.UInt64;
                case ETypeCode.Int16:
                    return DbType.Int16;
                case ETypeCode.Int32:
                    return DbType.Int32;
                case ETypeCode.Int64:
                    return DbType.Int64;
                case ETypeCode.Decimal:
                    return DbType.Decimal;
                case ETypeCode.Double:
                    return DbType.Double;
                case ETypeCode.Single:
                    return DbType.Single;
                case ETypeCode.String:
                    return DbType.String;
                case ETypeCode.Boolean:
                    return DbType.Boolean;
                case ETypeCode.DateTime:
                    return DbType.DateTime;
                case ETypeCode.Time:
                    return DbType.Time;
                case ETypeCode.Guid:
                    return DbType.Guid;
                case ETypeCode.Binary:
                    return DbType.Binary;
                default:
                    return DbType.String;
            }
        }


        /// <summary>
        /// Result of a data comparison
        /// </summary>
        public enum ECompareResult
        {
            Greater,
            Less,
            Equal
        }

        /// <summary>
        /// truncates a string to a fixed length and adds ... where it exceeds the length.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="maxLength"></param>
        /// <returns></returns>
        public static string TruncateString(this string value, int maxLength)
        {
            if (string.IsNullOrEmpty(value)) return value;
            return value.Length <= maxLength ? value : value.Substring(0, maxLength-3) + "...";
        }

        /// <summary>
        /// Compares two values of the specified typecode and returns a result indicating null, greater, less ,equal, not equal.  for example if inputValue is greater than comparevalue the return will be "Greater".
        /// </summary>
        /// <param name="dataType">data type to compare</param>
        /// <param name="inputValue">primary value</param>
        /// <param name="compareValue">value to compare against</param>
        /// <returns>Success = false for compare error</returns>
        public static ReturnValue<ECompareResult> Compare(ETypeCode dataType, object inputValue, object compareValue)
        {
            try
            {
                if ((inputValue == null || inputValue is DBNull) && (compareValue == null || compareValue is DBNull))
                    return new ReturnValue<ECompareResult>(true, ECompareResult.Equal);

                if (inputValue == null || inputValue is DBNull || compareValue == null || compareValue is DBNull)
                    return new ReturnValue<ECompareResult>(true, (inputValue == null || inputValue is DBNull) ? ECompareResult.Less : ECompareResult.Greater);

                Type type = GetType(dataType);

                if (inputValue.GetType() != type)
                {
                    var try1 = TryParse(dataType, inputValue);
                    if (try1.Success == false)
                        return new ReturnValue<ECompareResult>(false, "Could not parse the value " + inputValue + " as a type " + dataType + ". Reason: " + try1.Message, null);
                    inputValue = try1.Value;
                }

                if (compareValue.GetType() != type)
                {

                    var try2 = TryParse(dataType, compareValue);
                    if (try2.Success == false)
                        return new ReturnValue<ECompareResult>(false, "Could not parse the value " + compareValue + " as a type " + dataType + ". Reason: " + try2.Message, null);
                    compareValue = try2.Value;
                }

                switch (dataType)
                {
                    case ETypeCode.Byte:
                        return new ReturnValue<ECompareResult>(true, (Byte)inputValue == (Byte)compareValue ? ECompareResult.Equal : (Byte)inputValue > (Byte)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.SByte:
                        return new ReturnValue<ECompareResult>(true, (SByte)inputValue == (SByte)compareValue ? ECompareResult.Equal : (SByte)inputValue > (SByte)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt16:
                        return new ReturnValue<ECompareResult>(true, (UInt16)inputValue == (UInt16)compareValue ? ECompareResult.Equal : (UInt16)inputValue > (UInt16)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt32:
                        return new ReturnValue<ECompareResult>(true, (UInt32)inputValue == (UInt32)compareValue ? ECompareResult.Equal : (UInt32)inputValue > (UInt32)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt64:
                        return new ReturnValue<ECompareResult>(true, (UInt64)inputValue == (UInt64)compareValue ? ECompareResult.Equal : (UInt64)inputValue > (UInt64)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int16:
                        return new ReturnValue<ECompareResult>(true, (Int16)inputValue == (Int16)compareValue ? ECompareResult.Equal : (Int16)inputValue > (Int16)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int32:
                        return new ReturnValue<ECompareResult>(true, (Int32)inputValue == (Int32)compareValue ? ECompareResult.Equal : (Int32)inputValue > (Int32)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int64:
                        return new ReturnValue<ECompareResult>(true, (Int64)inputValue == (Int64)compareValue ? ECompareResult.Equal : (Int64)inputValue > (Int64)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Decimal:
                        return new ReturnValue<ECompareResult>(true, (Decimal)inputValue == (Decimal)compareValue ? ECompareResult.Equal : (Decimal)inputValue > (Decimal)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Double:
                        return new ReturnValue<ECompareResult>(true, Math.Abs((Double)inputValue - (Double)compareValue) < 0.0001 ? ECompareResult.Equal : (Double)inputValue > (Double)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Single:
                        return new ReturnValue<ECompareResult>(true, Math.Abs((Single)inputValue - (Single)compareValue) < 0.0001 ? ECompareResult.Equal : (Single)inputValue > (Single)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.String:
                        int compareResult = String.Compare((String)inputValue, (String)compareValue);
                        return new ReturnValue<ECompareResult>(true, compareResult == 0 ? ECompareResult.Equal : compareResult < 0 ? ECompareResult.Less : ECompareResult.Greater);
                    case ETypeCode.Guid:
                        compareResult = String.Compare(inputValue.ToString(), compareValue.ToString());
                        return new ReturnValue<ECompareResult>(true, compareResult == 0 ? ECompareResult.Equal : compareResult < 0 ? ECompareResult.Less : ECompareResult.Greater);
                    case ETypeCode.Boolean:
                        return new ReturnValue<ECompareResult>(true, (Boolean)inputValue == (Boolean)compareValue ? ECompareResult.Equal : (Boolean)inputValue ? ECompareResult.Greater : ECompareResult.Less );
                    case ETypeCode.DateTime:
                        return new ReturnValue<ECompareResult>(true, (DateTime)inputValue == (DateTime)compareValue ? ECompareResult.Equal : (DateTime)inputValue > (DateTime)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Time:
                        return new ReturnValue<ECompareResult>(true, (TimeSpan)inputValue == (TimeSpan)compareValue ? ECompareResult.Equal : (TimeSpan)inputValue > (TimeSpan)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Binary:
                        return new ReturnValue<ECompareResult>(true, StructuralComparisons.StructuralEqualityComparer.Equals(inputValue, compareValue) ? ECompareResult.Equal : ECompareResult.Greater);
                    default:
                        return new ReturnValue<ECompareResult>(false, "Unsupported datatype: " + dataType, null);
                }
            }
            catch(Exception ex)
            {
                return new ReturnValue<ECompareResult>(false, "The compare of " + (inputValue??"Null").ToString() + " to " + (compareValue ?? "Null").ToString() + " failed with the following errror: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Attempts to parse and convert the input value to the specified datatype.
        /// </summary>
        /// <param name="tryDataType">DataType to convert to</param>
        /// <param name="inputValue">Input Value to convert</param>
        /// <param name="maxLength">Optional: maximum length for a string value.</param>
        /// <returns>True and the converted value for success, false and a message for conversion fail.</returns>
        public static ReturnValue<object> TryParse(ETypeCode tryDataType, object inputValue, int? maxLength = null)
        {
            Object result = null;
            try
            {
                if (inputValue == null)
                {
                    return new ReturnValue<object>(true, null);
                }

                if (tryDataType == ETypeCode.String)
                {
                    result = inputValue is DBNull ? null : inputValue.ToString();
                    if(maxLength != null && result != null && ((string)result).Length > maxLength)
                        return new ReturnValue<object>(false, "The string " + inputValue + " exceeds the maximum length of " + maxLength.ToString());
                    else
                        return new ReturnValue<object>(true, result);
                }

                if (tryDataType == ETypeCode.Unknown)
                {
                    return new ReturnValue<object>(true, inputValue);
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
                        case ETypeCode.SByte:
                            result = Convert.ToSByte(inputValue);
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
                        case ETypeCode.UInt16:
                            result = Convert.ToUInt16(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.UInt32:
                            result = Convert.ToUInt32(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.UInt64:
                            result = Convert.ToUInt64(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Double:
                            result = Convert.ToDouble(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Decimal:
                            result = Convert.ToDecimal(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Single:
                            result = Convert.ToSingle(inputValue);
                            return new ReturnValue<object>(true, result);
                        case ETypeCode.Binary:
                            return new ReturnValue<object>(false, "Cannot convert a binary data type to another type.", null);
                        default:
                            string reason = "Cannot convert value " + inputValue + " from numeric to " + tryDataType;
                            return new ReturnValue<object>(false, reason, null);
                    }
                }

                if (tryBasicType == EBasicType.Boolean && inputBasicType == EBasicType.Numeric)
                {
                    result = Convert.ToBoolean(inputValue);
                }

                if (tryBasicType == EBasicType.Numeric && inputBasicType == EBasicType.Date)
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

                if (result == null)
                {
                    string value;

                    if (inputType != ETypeCode.String)
                        value = inputValue.ToString();
                    else
                        value = (string)inputValue;

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
                            Int64 int64Result;
                            returnValue = Int64.TryParse(value, out int64Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int64.", null);
                            result = int64Result;
                            break;
                        case ETypeCode.UInt16:
                            UInt16 uint16Result;
                            returnValue = UInt16.TryParse(value, out uint16Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a UInt16.", null);
                            result = uint16Result;
                            break;
                        case ETypeCode.UInt32:
                            UInt32 uint32Result;
                            returnValue = UInt32.TryParse(value, out uint32Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a UInt32.", null);
                            result = uint32Result;
                            break;
                        case ETypeCode.UInt64:
                            UInt64 uint64Result;
                            returnValue = UInt64.TryParse(value, out uint64Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int64.", null);
                            result = uint64Result;
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
                        case ETypeCode.Single:
                            Single singleResult;
                            returnValue = Single.TryParse(value, out singleResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Single.", null);
                            result = singleResult;
                            break;
                        case ETypeCode.SByte:
                            SByte sbyteResult;
                            returnValue = SByte.TryParse(value, out sbyteResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a SByte.", null);
                            result = sbyteResult;
                            break;
                        case ETypeCode.String:
                            result = value;
                            break;
                        case ETypeCode.Guid:
                            Guid guidResult;
                            returnValue = Guid.TryParse(value, out guidResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Guid.", null);
                            result = guidResult;
                            break;
                        case ETypeCode.Boolean:
                            Boolean booleanResult;
                            returnValue = Boolean.TryParse(value, out booleanResult);
                            if (returnValue == false)
                            {
                                returnValue = Int16.TryParse(value, out int16Result);
                                if (returnValue == false)
                                {
                                    return new ReturnValue<object>(false, "The value " + value + " could not be converted to a boolean.", null);
                                }
                                switch(int16Result)
                                {
                                    case 0:
                                        result = false;
                                        break;
                                    case 1:
                                    case -1:
                                        result = true;
                                        break;
                                    default:
                                        return new ReturnValue<object>(false, "The value " + value + " could not be converted to a boolean.", null);
                                }
                            }
                            else
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

        /// <summary>
        /// Removes all non alphanumeric characters from the string
        /// </summary>
        /// <returns></returns>
        public static string CleanString(string value)
        {
            if(string.IsNullOrEmpty(value))
                return value;
                
            var arr = value.Where(c => (char.IsLetterOrDigit(c))).ToArray();
            string newValue = new string(arr);
            return newValue;
        }
    }
}
