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
        [MemberData("OtherProperties")] 
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
                    new object[] { new int[] {1,2,3}, false },
                    new object[] { new string[] { "hi", "there" }, false },
                    new object[] { dateValue, true }
                };
            }
        }

        [Fact]
        public void Test_CopyProperties()
        {
            TableColumn column = new TableColumn()
            {
                BaseDataType = DataType.ETypeCode.String,
                DeltaType = TableColumn.EDeltaType.CreateDate,
                ColumnName = "columnName",
                AllowDbNull = true,
                SecurityFlag = TableColumn.ESecurityFlag.OneWayHash
            };

            TableColumn newColumn = new TableColumn();
            column.CopyProperties(newColumn, true);

            Assert.Equal(DataType.ETypeCode.String, newColumn.BaseDataType);
            Assert.Equal(TableColumn.EDeltaType.CreateDate, newColumn.DeltaType);
            Assert.Equal("columnName", newColumn.ColumnName);
            Assert.Equal(TableColumn.ESecurityFlag.OneWayHash, newColumn.SecurityFlag);
        }

    }
}
