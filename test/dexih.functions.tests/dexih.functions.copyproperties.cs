using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;
using static dexih.functions.Reflection;

namespace dexih.functions.tests
{
    public class FunctionCopyProperties
    {
        [Theory]
        [InlineData("hi", true)]
        [InlineData(1, true)]
        [InlineData(1.1, true)]
        [InlineData(functions.DataType.ETypeCode.Boolean, true)]
        [InlineData(true, true)]
        [MemberData("OtherFunctions")] 
        public void Test_IsSimpleType(object value, bool expected)
        {
            Assert.Equal(expected, IsSimpleType(value.GetType()));
        }

        public static IEnumerable<object[]> OtherProperties
        {
            get
            {
                var dateValue = DateTime.Parse("2001-01-01");

                return new[]
                {
                    new object[] { new object[] {1,2,3}, false },
                    new object[] { dateValue, true }
                };
            }
        }

    }
}
