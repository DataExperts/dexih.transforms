using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;
using static dexih.functions.DataType;

namespace dexih.functions.tests
{
    public class FunctionDataType
    {
        [Theory]
        [InlineData(ETypeCode.Boolean, EBasicType.Boolean)]
        [InlineData(ETypeCode.Byte, EBasicType.Numeric)]
        [InlineData(ETypeCode.DateTime, EBasicType.Date)]
        [InlineData(ETypeCode.Decimal, EBasicType.Numeric)]
        [InlineData(ETypeCode.Double, EBasicType.Numeric)]
        [InlineData(ETypeCode.Guid, EBasicType.String)]
        [InlineData(ETypeCode.Int16, EBasicType.Numeric)]
        [InlineData(ETypeCode.Int32, EBasicType.Numeric)]
        [InlineData(ETypeCode.Int64, EBasicType.Numeric)]
        [InlineData(ETypeCode.SByte, EBasicType.Numeric)]
        [InlineData(ETypeCode.Single, EBasicType.Numeric)]
        [InlineData(ETypeCode.String, EBasicType.String)]
        [InlineData(ETypeCode.Time, EBasicType.Time)]
        [InlineData(ETypeCode.UInt16, EBasicType.Numeric)]
        [InlineData(ETypeCode.UInt32, EBasicType.Numeric)]
        [InlineData(ETypeCode.UInt64, EBasicType.Numeric)]
        [InlineData(ETypeCode.Unknown, EBasicType.Unknown)]
        public void DataType_GetBasicType(ETypeCode inputType, EBasicType expected)
        {
            Assert.Equal(expected, DataType.GetBasicType(inputType));
        }

        [Theory]
        [InlineData(typeof(Byte), ETypeCode.Byte)]
        [InlineData(typeof(SByte), ETypeCode.SByte)]
        [InlineData(typeof(UInt16), ETypeCode.UInt16)]
        [InlineData(typeof(UInt32), ETypeCode.UInt32)]
        [InlineData(typeof(UInt64), ETypeCode.UInt64)]
        [InlineData(typeof(Int16), ETypeCode.Int16)]
        [InlineData(typeof(Int32), ETypeCode.Int32)]
        [InlineData(typeof(Int64), ETypeCode.Int64)]
        [InlineData(typeof(Decimal), ETypeCode.Decimal)]
        [InlineData(typeof(Double), ETypeCode.Double)]
        [InlineData(typeof(Single), ETypeCode.Single)]
        [InlineData(typeof(String), ETypeCode.String)]
        [InlineData(typeof(Boolean), ETypeCode.Boolean)]
        [InlineData(typeof(DateTime), ETypeCode.DateTime)]
        [InlineData(typeof(TimeSpan), ETypeCode.Time)]
        [InlineData(typeof(Guid), ETypeCode.Guid)]
        public void DataType_GetTypeCode(Type dataType, ETypeCode expectedTypeCode)
        {
            Assert.Equal(expectedTypeCode, DataType.GetTypeCode(dataType));
        }

        [Theory]
        [InlineData(typeof(Byte), ETypeCode.Byte)]
        [InlineData(typeof(SByte), ETypeCode.SByte)]
        [InlineData(typeof(UInt16), ETypeCode.UInt16)]
        [InlineData(typeof(UInt32), ETypeCode.UInt32)]
        [InlineData(typeof(UInt64), ETypeCode.UInt64)]
        [InlineData(typeof(Int16), ETypeCode.Int16)]
        [InlineData(typeof(Int32), ETypeCode.Int32)]
        [InlineData(typeof(Int64), ETypeCode.Int64)]
        [InlineData(typeof(Decimal), ETypeCode.Decimal)]
        [InlineData(typeof(Double), ETypeCode.Double)]
        [InlineData(typeof(Single), ETypeCode.Single)]
        [InlineData(typeof(String), ETypeCode.String)]
        [InlineData(typeof(Boolean), ETypeCode.Boolean)]
        [InlineData(typeof(DateTime), ETypeCode.DateTime)]
        [InlineData(typeof(TimeSpan), ETypeCode.Time)]
        [InlineData(typeof(Guid), ETypeCode.Guid)]
        public void DataType_GetType(Type expectedType, ETypeCode typeCode)
        {
            Assert.Equal(expectedType, DataType.GetType(typeCode));
        }

        [Theory]
        [InlineData(ETypeCode.Byte, 2, 1 , ECompareResult.Greater)]
        [InlineData(ETypeCode.Byte, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.Byte, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.SByte, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.SByte, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.SByte, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.UInt16, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.UInt16, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.UInt16, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.UInt32, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.UInt32, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.UInt32, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.UInt64, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.UInt64, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.UInt64, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.Int16, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.Int16, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.Int16, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.Int32, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.Int32, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.Int32, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.Int64, 2, 1, ECompareResult.Greater)]
        [InlineData(ETypeCode.Int64, 1, 1, ECompareResult.Equal)]
        [InlineData(ETypeCode.Int64, 1, 2, ECompareResult.Less)]
        [InlineData(ETypeCode.Decimal, 1.1, 1.09, ECompareResult.Greater)]
        [InlineData(ETypeCode.Decimal, 1.09, 1.09, ECompareResult.Equal)]
        [InlineData(ETypeCode.Decimal, 1.09, 1.1, ECompareResult.Less)]
        [InlineData(ETypeCode.Double, 1.1, 1.09, ECompareResult.Greater)]
        [InlineData(ETypeCode.Double, 1.09, 1.09, ECompareResult.Equal)]
        [InlineData(ETypeCode.Double, 1.09, 1.1, ECompareResult.Less)]
        [InlineData(ETypeCode.Single, 1.1, 1.09, ECompareResult.Greater)]
        [InlineData(ETypeCode.Single, 1.09, 1.09, ECompareResult.Equal)]
        [InlineData(ETypeCode.Single, 1.09, 1.1, ECompareResult.Less)]
        [InlineData(ETypeCode.String, "01", "001", ECompareResult.Greater)]
        [InlineData(ETypeCode.String, "01", "01", ECompareResult.Equal)]
        [InlineData(ETypeCode.String, "001", "01", ECompareResult.Less)]
        [InlineData(ETypeCode.Boolean, true, false, ECompareResult.Greater)]
        [InlineData(ETypeCode.Boolean, true, true, ECompareResult.Equal)]
        [InlineData(ETypeCode.Boolean, false, true, ECompareResult.Less)]
        [InlineData(ETypeCode.DateTime, "2001-01-01", "2000-12-31", ECompareResult.Greater)]
        [InlineData(ETypeCode.DateTime, "2001-01-01", "2001-01-01", ECompareResult.Equal)]
        [InlineData(ETypeCode.DateTime, "2000-01-02", "2000-01-03", ECompareResult.Less)]
        [InlineData(ETypeCode.Time, "00:01:00", "00:00:59", ECompareResult.Greater)]
        [InlineData(ETypeCode.Time, "00:00:59", "00:00:59", ECompareResult.Equal)]
        [InlineData(ETypeCode.Time, "00:01:00", "00:01:01", ECompareResult.Less)]
        [InlineData(ETypeCode.Guid, "6d5bba83-e71b-4ce1-beb8-006085a0a77d", "6d5bba83-e71b-4ce1-beb8-006085a0a77c", ECompareResult.Greater)]
        [InlineData(ETypeCode.Guid, "6d5bba83-e71b-4ce1-beb8-006085a0a77c", "6d5bba83-e71b-4ce1-beb8-006085a0a77c", ECompareResult.Equal)]
        [InlineData(ETypeCode.Guid, "6d5bba83-e71b-4ce1-beb8-006085a0a77c", "6d5bba83-e71b-4ce1-beb8-006085a0a77d", ECompareResult.Less)]
        public void DataType_Compare(ETypeCode dataType, object inputValue, object compareValue, ECompareResult expectedResult)
        {
            var result = DataType.Compare(dataType, inputValue, compareValue);
            Assert.True(result.Success);
            Assert.Equal(expectedResult, result.Value);
        }

        [Theory]
        [InlineData(ETypeCode.Byte, 2, (Byte)2)]
        [InlineData(ETypeCode.Byte, "2", (Byte)2)]
        [InlineData(ETypeCode.SByte, 2, (SByte)2)]
        [InlineData(ETypeCode.SByte, "2", (SByte)2)]
        [InlineData(ETypeCode.UInt16, 2, (UInt16)2)]
        [InlineData(ETypeCode.UInt16, "2", (UInt16)2)]
        [InlineData(ETypeCode.UInt32, 2, (UInt32)2)]
        [InlineData(ETypeCode.UInt32, "2", (UInt32)2)]
        [InlineData(ETypeCode.UInt64, 2, (UInt64)2)]
        [InlineData(ETypeCode.UInt64, "2", (UInt64)2)]
        [InlineData(ETypeCode.Int16, -2, (Int16)(-2))]
        [InlineData(ETypeCode.Int16, "-2", (Int16)(-2))]
        [InlineData(ETypeCode.Int32, -2, (Int32)(-2))]
        [InlineData(ETypeCode.Int32, "-2", (Int32)(-2))]
        [InlineData(ETypeCode.Int64, -2, (Int64)(-2))]
        [InlineData(ETypeCode.Int64, "-2", (Int64)(-2))]
        [InlineData(ETypeCode.Double, -2.123, (Double)(-2.123))]
        [InlineData(ETypeCode.Double, "-2.123 ", (Double)(-2.123))]
        [InlineData(ETypeCode.String, 01, "1")]
        [InlineData(ETypeCode.String, true, "True")]
        [InlineData(ETypeCode.Boolean, "true", true)]
        [InlineData(ETypeCode.Boolean, "1", true)]
        [InlineData(ETypeCode.Boolean, 1, true)]
        [MemberData("OtherDataTypes")]
        public void DataType_TryParse(ETypeCode dataType, object inputValue, object expectedValue)
        {
            var result = DataType.TryParse(dataType, inputValue);
            Assert.True(result.Success);
            Assert.Equal(expectedValue, result.Value);
        }

        public static IEnumerable<object[]> OtherDataTypes
        {
            get
            {
                return new[]
                {
                new object[] { ETypeCode.Decimal, -2.123, (Decimal)(-2.123)},
                new object[] { ETypeCode.Decimal, "-2.123", (Decimal)(-2.123)},
                new object[] { ETypeCode.DateTime, "2001-01-01", new DateTime(2001,01,01)},
                new object[] { ETypeCode.DateTime, "2001-01-01T12:59:59", new DateTime(2001,01,01, 12, 59, 59)},
                new object[] { ETypeCode.Time, "12:59:59", new TimeSpan(12, 59, 59)},
                new object[] { ETypeCode.Guid, "6d5bba83-e71b-4ce1-beb8-006085a0a77d", new Guid("6d5bba83-e71b-4ce1-beb8-006085a0a77d")},
                new object[] { ETypeCode.Guid, "6d5bba83-e71b-4ce1-beb8-006085a0a77d", new Guid("6d5bba83-e71b-4ce1-beb8-006085a0a77d")},
            };

            }

        }

        //values that should return a false result
        [Theory]
        [InlineData(ETypeCode.Byte, -1, 0)]
        [InlineData(ETypeCode.SByte, -200, 0)]
        [InlineData(ETypeCode.UInt16, -1, 0)]
        [InlineData(ETypeCode.UInt32, -1, 0)]
        [InlineData(ETypeCode.UInt64, -1, 0)]
        [InlineData(ETypeCode.Int16, Int16.MaxValue+1, 0)]
        [InlineData(ETypeCode.Int32, "a123", 0)]
        [InlineData(ETypeCode.Int64, "123a", 0)]
        [InlineData(ETypeCode.Double, "123a", 0)]
        [InlineData(ETypeCode.String, "12345", 4)]
        [InlineData(ETypeCode.Decimal, "123a", 0)]
        [InlineData(ETypeCode.DateTime, "2001-01-32", 0)]
        [InlineData(ETypeCode.Time, "12:70:00", 0)]
        [InlineData(ETypeCode.Guid, "asdfadsf", 0)]
        public void DataType_TryParse_False(ETypeCode dataType, object inputValue, int maxLength = 0)
        {
            var result = DataType.TryParse(dataType, inputValue, maxLength);
            Assert.False(result.Success);
        }
    }
}
