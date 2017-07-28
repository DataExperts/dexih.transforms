using System;
using System.Collections;
using System.Data;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

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
                    return byte.MaxValue;
                case ETypeCode.SByte:
                    return sbyte.MaxValue;
                case ETypeCode.UInt16:
                    return ushort.MaxValue;
                case ETypeCode.UInt32:
                    return uint.MaxValue;
                case ETypeCode.UInt64:
                    return long.MaxValue; //use max value of int64 as some databases don't support uint64 (namely postgreSql)
                case ETypeCode.Int16:
                    return short.MaxValue;
				case ETypeCode.Int32:
                    return int.MaxValue;
				case ETypeCode.Int64:
                    return long.MaxValue;
				case ETypeCode.Decimal:
                    return 999999999999999999; //use arbitrary big number as range varies between databases.  
				case ETypeCode.Double:
                    return double.MaxValue / 10;
				case ETypeCode.Single:
                    return float.MaxValue /10;
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
                    return new[] { byte.MaxValue, byte.MaxValue, byte.MaxValue };
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
                    return byte.MinValue;
				case ETypeCode.SByte:
                    return sbyte.MinValue;
				case ETypeCode.UInt16:
                    return ushort.MinValue;
				case ETypeCode.UInt32:
                    return uint.MinValue;
				case ETypeCode.UInt64:
                    return ulong.MinValue;
				case ETypeCode.Int16:
                    return short.MinValue;
				case ETypeCode.Int32:
                    return int.MinValue;
				case ETypeCode.Int64:
                    return long.MinValue;
				case ETypeCode.Decimal:
                    return -999999999999999999;
                case ETypeCode.Double:
                    return double.MinValue / 10;
                case ETypeCode.Single:
                    return float.MinValue / 10;
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
                    return new[] { byte.MinValue, byte.MinValue, byte.MinValue };
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
            if(dataType == typeof(byte))
                return ETypeCode.Byte;
            if (dataType == typeof(sbyte))
                return ETypeCode.SByte;
            if (dataType == typeof(ushort))
                return ETypeCode.UInt16;
            if (dataType == typeof(uint))
                return ETypeCode.UInt32;
            if (dataType == typeof(ulong))
                return ETypeCode.UInt64;
            if (dataType == typeof(short))
                return ETypeCode.Int16;
            if (dataType == typeof(int))
                return ETypeCode.Int32;
            if (dataType == typeof(long))
                return ETypeCode.Int64;
            if (dataType == typeof(decimal))
                return ETypeCode.Decimal;
            if (dataType == typeof(double))
                return ETypeCode.Double;
            if (dataType == typeof(float))
                return ETypeCode.Single;
            if (dataType == typeof(string))
                return ETypeCode.String;
            if (dataType == typeof(bool))
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
                    return typeof(byte);
                case ETypeCode.SByte:
                    return typeof(sbyte);
                case ETypeCode.UInt16:
                    return typeof(ushort);
                case ETypeCode.UInt32:
                    return typeof(uint);
                case ETypeCode.UInt64:
                    return typeof(ulong);
                case ETypeCode.Int16:
                    return typeof(short);
                case ETypeCode.Int32:
                    return typeof(int);
                case ETypeCode.Int64:
                    return typeof(long);
                case ETypeCode.Decimal:
                    return typeof(decimal);
                case ETypeCode.Double:
                    return typeof(double);
                case ETypeCode.Single: 
                    return typeof(float);
                case ETypeCode.String: 
                    return typeof(string);
                case ETypeCode.Boolean: 
                    return typeof(bool);
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

                var type = GetType(dataType);

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
                        return new ReturnValue<ECompareResult>(true, (byte)inputValue == (byte)compareValue ? ECompareResult.Equal : (byte)inputValue > (byte)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.SByte:
                        return new ReturnValue<ECompareResult>(true, (sbyte)inputValue == (sbyte)compareValue ? ECompareResult.Equal : (sbyte)inputValue > (sbyte)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt16:
                        return new ReturnValue<ECompareResult>(true, (ushort)inputValue == (ushort)compareValue ? ECompareResult.Equal : (ushort)inputValue > (ushort)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt32:
                        return new ReturnValue<ECompareResult>(true, (uint)inputValue == (uint)compareValue ? ECompareResult.Equal : (uint)inputValue > (uint)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.UInt64:
                        return new ReturnValue<ECompareResult>(true, (ulong)inputValue == (ulong)compareValue ? ECompareResult.Equal : (ulong)inputValue > (ulong)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int16:
                        return new ReturnValue<ECompareResult>(true, (short)inputValue == (short)compareValue ? ECompareResult.Equal : (short)inputValue > (short)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int32:
                        return new ReturnValue<ECompareResult>(true, (int)inputValue == (int)compareValue ? ECompareResult.Equal : (int)inputValue > (int)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Int64:
                        return new ReturnValue<ECompareResult>(true, (long)inputValue == (long)compareValue ? ECompareResult.Equal : (long)inputValue > (long)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Decimal:
                        return new ReturnValue<ECompareResult>(true, (decimal)inputValue == (decimal)compareValue ? ECompareResult.Equal : (decimal)inputValue > (decimal)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Double:
                        return new ReturnValue<ECompareResult>(true, Math.Abs((double)inputValue - (double)compareValue) < 0.0001 ? ECompareResult.Equal : (double)inputValue > (double)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.Single:
                        return new ReturnValue<ECompareResult>(true, Math.Abs((float)inputValue - (float)compareValue) < 0.0001 ? ECompareResult.Equal : (float)inputValue > (float)compareValue ? ECompareResult.Greater : ECompareResult.Less);
                    case ETypeCode.String:
                        var compareResult = string.Compare((string)inputValue, (string)compareValue);
                        return new ReturnValue<ECompareResult>(true, compareResult == 0 ? ECompareResult.Equal : compareResult < 0 ? ECompareResult.Less : ECompareResult.Greater);
                    case ETypeCode.Guid:
                        compareResult = string.Compare(inputValue.ToString(), compareValue.ToString());
                        return new ReturnValue<ECompareResult>(true, compareResult == 0 ? ECompareResult.Equal : compareResult < 0 ? ECompareResult.Less : ECompareResult.Greater);
                    case ETypeCode.Boolean:
                        return new ReturnValue<ECompareResult>(true, (bool)inputValue == (bool)compareValue ? ECompareResult.Equal : (bool)inputValue ? ECompareResult.Greater : ECompareResult.Less );
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
                return new ReturnValue<ECompareResult>(false, "The compare of " + (inputValue??"Null") + " to " + (compareValue ?? "Null") + " failed with the following errror: " + ex.Message, ex);
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
            object result = null;
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
                        return new ReturnValue<object>(false, "The string " + inputValue + " exceeds the maximum length of " + maxLength);
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

                var inputType = GetTypeCode(inputValue.GetType());

                if (tryDataType == inputType )
                {
                    result = inputValue;
                    return new ReturnValue<object>(true, result);
                }

                var tryBasicType = GetBasicType(tryDataType);
                var inputBasicType = GetBasicType(inputType);

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
                            var reason = "Cannot convert value " + inputValue + " from numeric to " + tryDataType;
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
                    var reason = "Cannot convert value " + inputValue + " to " + tryDataType;
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
                            returnValue = byte.TryParse(value, out byteResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a byte.", null);
                            result = byteResult;
                            break;
                        case ETypeCode.Int16:
                            short int16Result;
                            returnValue = short.TryParse(value, out int16Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int16.", null);
                            result = int16Result;
                            break;
                        case ETypeCode.Int32:
                            int int32Result;
                            returnValue = int.TryParse(value, out int32Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int32.", null);
                            result = int32Result;
                            break;
                        case ETypeCode.Int64:
                            long int64Result;
                            returnValue = long.TryParse(value, out int64Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int64.", null);
                            result = int64Result;
                            break;
                        case ETypeCode.UInt16:
                            ushort uint16Result;
                            returnValue = ushort.TryParse(value, out uint16Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a UInt16.", null);
                            result = uint16Result;
                            break;
                        case ETypeCode.UInt32:
                            uint uint32Result;
                            returnValue = uint.TryParse(value, out uint32Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a UInt32.", null);
                            result = uint32Result;
                            break;
                        case ETypeCode.UInt64:
                            ulong uint64Result;
                            returnValue = ulong.TryParse(value, out uint64Result);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Int64.", null);
                            result = uint64Result;
                            break;
                        case ETypeCode.Double:
                            double doubleResult;
                            returnValue = double.TryParse(value, out doubleResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Double.", null);
                            result = doubleResult;
                            break;
                        case ETypeCode.Decimal:
                            decimal decimalResult;
                            returnValue = decimal.TryParse(value, out decimalResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Decimal.", null);
                            result = decimalResult;
                            break;
                        case ETypeCode.Single:
                            float singleResult;
                            returnValue = float.TryParse(value, out singleResult);
                            if (returnValue == false)
                                return new ReturnValue<object>(false, "The value " + value + " could not be converted to a Single.", null);
                            result = singleResult;
                            break;
                        case ETypeCode.SByte:
                            sbyte sbyteResult;
                            returnValue = sbyte.TryParse(value, out sbyteResult);
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
                            bool booleanResult;
                            returnValue = bool.TryParse(value, out booleanResult);
                            if (returnValue == false)
                            {
                                returnValue = short.TryParse(value, out int16Result);
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
                var reason = "Cannot convert value " + (inputValue?.ToString() ?? "null") + " to type: " + tryDataType + ". The following error was returned:" +ex.Message;
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
            var newValue = new string(arr);
            return newValue;
        }
    }
}
