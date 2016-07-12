using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class TransformMappingTests
    {
        private readonly ITestOutputHelper output;

        public TransformMappingTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void Mappings()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformMapping transformMapping = new TransformMapping();

            List<Function> Mappings = new List<Function>();

            //Mappings.Add(new Function("CustomFunction", false, "test", "return StringColumn + number.ToString();", null, ETypeCode.String,
            //    new dexih.functions.Parameter[] {
            //        new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
            //        new dexih.functions.Parameter("number", ETypeCode.Int32, false, 123)
            //    }, null));

            Function Function = new Function(new Func<string, int, string>((StringColumn, number) => StringColumn + number.ToString()), new string[] { "StringColumn", "number" }, "CustomFunction", null);
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("number", ETypeCode.Int32, false, 123) };
            Mappings.Add(Function);

            Function = StandardFunctions.GetFunctionReference("Substring");
            Function.TargetColumn = "Substring";
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 1),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 3) };
            Mappings.Add(Function);

            List<ColumnPair> MappingColumn = new List<ColumnPair>();
            MappingColumn.Add(new ColumnPair("DateColumn", "DateColumn"));

            transformMapping = new TransformMapping(Source, false, MappingColumn, Mappings);

            Assert.Equal(3, transformMapping.FieldCount);

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

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task MappingPerformancePassthrough(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            TransformMapping transformMapping = new TransformMapping();
            transformMapping.PassThroughColumns = true;
            transformMapping.SetInTransform(data);

            int count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceSummary());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task MappingPerformanceColumnPairs(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            TransformMapping transformMapping = new TransformMapping();
            List<ColumnPair> columnMappings = new List<ColumnPair>();

            for (int i = 0; i < data.FieldCount; i++)
                columnMappings.Add(new ColumnPair(data.GetName(i)));

            transformMapping.PassThroughColumns = false;
            transformMapping.MapFields = columnMappings;
            transformMapping.SetInTransform(data);

            int count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceSummary());

        }

        [Theory]
        [InlineData(100000)] //should run in ~ 900ms
        public async Task MappingPerformanceFunctions(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            TransformMapping transformMapping = new TransformMapping();
            List<Function> columnMappings = new List<Function>();

            for (int i = 0; i < data.FieldCount; i++)
            {
                Function newFunction = new Function(new Func<object, object>((value) => value), new string[] { data.GetName(i) }, data.GetName(i), null);
                columnMappings.Add(newFunction);
            }

            transformMapping.PassThroughColumns = false;
            transformMapping.Functions = columnMappings;
            transformMapping.SetInTransform(data);

            int count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceSummary());

        }

    }
}
