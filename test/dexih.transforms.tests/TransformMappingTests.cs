using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class TransformMappingTests
    {
        [Fact]
        public void Mappings()
        {
            SourceTable Source = Helpers.CreateSortedTestData();
            TransformMapping transformMapping = new TransformMapping();

            List<Function> Mappings = new List<Function>();

            Mappings.Add(new Function("CustomFunction", false, "test", "return StringColumn + number.ToString();", null, ETypeCode.String,
                new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("number", ETypeCode.Int32, false, 123)
                }, null));

            Function Function = StandardFunctions.GetFunctionReference("Substring");
            Function.TargetColumn = "Substring";
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 1),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 3) };
            Mappings.Add(Function);

            List<ColumnPair> MappingColumn = new List<ColumnPair>();
            MappingColumn.Add(new ColumnPair("DateColumn", "DateColumn"));
            transformMapping.SetMappings(MappingColumn, Mappings);
            transformMapping.SetInTransform(Source);

            Assert.Equal(3, transformMapping.FieldCount);
            Assert.Equal(3, transformMapping.Fields.Count());

            int count = 0;
            while (transformMapping.Read() == true)
            {
                count = count + 1;
                Assert.Equal("value" + count.ToString().PadLeft(2, '0') + "123", transformMapping["CustomFunction"]);
                Assert.Equal("alu", transformMapping["Substring"]);
                Assert.Equal((DateTime)Convert.ToDateTime("2015-01-" + count.ToString()), (DateTime)transformMapping["DateColumn"]);
            }
            Assert.Equal(10, count);

            //test the getschematable table function.
            //DataReaderAdapter SchemaTable = TransformMapping.GetSchemaTable();
            //Assert.Equal("DateColumn", SchemaTable.Rows[0]["ColumnName"]);
            //Assert.Equal("CustomFunction", SchemaTable.Rows[1]["ColumnName"]);
            //Assert.Equal("Substring", SchemaTable.Rows[2]["ColumnName"]);
        }
    }
}
