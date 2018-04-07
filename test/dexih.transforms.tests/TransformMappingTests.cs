using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static Dexih.Utils.DataType.DataType;

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
        public async Task Mappings()
        {
            var Source = Helpers.CreateSortedTestData();
            var transformMapping = new TransformMapping();

            var Mappings = new List<TransformFunction>();

            //Mappings.Add(new Function("CustomFunction", false, "test", "return StringColumn + number.ToString();", null, ETypeCode.String,
            //    new dexih.functions.Parameter[] {
            //        new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
            //        new dexih.functions.Parameter("number", ETypeCode.Int32, false, 123)
            //    }, null));

            var transformFunction = new TransformFunction(
                new Func<string, int, string>((StringColumn, number) => StringColumn + number.ToString()), 
                new TableColumn[] { new TableColumn("StringColumn"), new TableColumn("number", ETypeCode.Int32) }, 
                new TableColumn("CustomFunction"), 
                null,
                null);
            transformFunction.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null,  new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("number", ETypeCode.Int32, false, 123) };
            Mappings.Add(transformFunction);

            transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "Substring").GetTransformFunction();
            transformFunction.TargetColumn = new TableColumn("Substring");
            transformFunction.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null,  new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 1),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 3) };
            Mappings.Add(transformFunction);

            var MappingColumn = new List<ColumnPair>();
            MappingColumn.Add(new ColumnPair(new TableColumn("DateColumn", ETypeCode.DateTime), new TableColumn("DateColumn", ETypeCode.DateTime)));

            transformMapping = new TransformMapping(Source, false, MappingColumn, Mappings);

            Assert.Equal(3, transformMapping.FieldCount);

            var count = 0;
            while (await transformMapping.ReadAsync() == true)
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
            var transformMapping = new TransformMapping();
            transformMapping.PassThroughColumns = true;
            transformMapping.SetInTransform(data);

            var count = 0;
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
            var transformMapping = new TransformMapping();
            var columnMappings = new List<ColumnPair>();

            for (var i = 0; i < data.FieldCount; i++)
                columnMappings.Add(new ColumnPair(new TableColumn(data.GetName(i))));

            transformMapping.PassThroughColumns = false;
            transformMapping.MapFields = columnMappings;
            transformMapping.SetInTransform(data);

            var count = 0;
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
            var transformMapping = new TransformMapping();
            var columnMappings = new List<TransformFunction>();

            for (var i = 0; i < data.FieldCount; i++)
            {
                var newTransformFunction = new TransformFunction(
                    new Func<object, object>((value) => value), 
                    new TableColumn[] { new TableColumn(data.GetName(i)) }, 
                    new TableColumn(data.GetName(i)), 
                    null,
                    null);
                columnMappings.Add(newTransformFunction);
            }

            transformMapping.PassThroughColumns = false;
            transformMapping.Functions = columnMappings;
            transformMapping.SetInTransform(data);

            var count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceSummary());

        }

    }
}
