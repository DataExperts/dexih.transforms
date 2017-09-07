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
        [InlineData(DataType.ETypeCode.Boolean, true)]
        [InlineData(true, true)]
        [MemberData(nameof(OtherProperties))] 
        public void Test_IsSimpleType(object value, bool expected)
        {
            Assert.Equal(expected, IsSimpleType(value.GetType()));
        }

	    private static IEnumerable<object[]> OtherProperties
        {
            get
            {
                var dateValue = DateTime.Parse("2001-01-01");
                var timeValue = new TimeSpan(1, 2, 3);

				return new[]
                {
                    new object[] { new object[] {1,2,3}, false },
                    new object[] { new int[] {1,2,3}, false },
                    new object[] { new string[] { "hi", "there" }, false },
                    new object[] { dateValue, true },
					new object[] { timeValue, true }
				};
            }
        }

        [Fact]
        public void Test_CopyProperties_Column()
        {
            var column = new TableColumn()
            {
                BaseDataType = DataType.ETypeCode.String,
                DeltaType = TableColumn.EDeltaType.CreateDate,
                Name = "columnName",
                AllowDbNull = true,
                SecurityFlag = TableColumn.ESecurityFlag.OneWayHash
            };

            var newColumn = new TableColumn();
            column.CopyProperties(newColumn, true);

            Assert.Equal(DataType.ETypeCode.String, newColumn.BaseDataType);
            Assert.Equal(TableColumn.EDeltaType.CreateDate, newColumn.DeltaType);
            Assert.Equal("columnName", newColumn.Name);
            Assert.Equal(TableColumn.ESecurityFlag.OneWayHash, newColumn.SecurityFlag);
        }

		[Theory]
		[InlineData("hi")]
		[InlineData(1)]
		[InlineData(1.1)]
		[InlineData(functions.DataType.ETypeCode.Boolean)]
		[InlineData(true, true)]
		[MemberData(nameof(OtherSimple))] 
		public void Test_CopyProperties_Simple(object value)
		{
            var newValue = Activator.CreateInstance(value.GetType());
            value.CopyProperties(newValue);
            Assert.Equal(value, newValue);
		}

	    private static IEnumerable<object[]> OtherSimple => new[]
	    {
		    new object[] { DateTime.Parse("2001-01-01") },
		    new object[] { new TimeSpan(1, 2, 3) },
	    };
    }
}
