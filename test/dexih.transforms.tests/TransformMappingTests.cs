using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
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
            var source = Helpers.CreateSortedTestData();

            var mappings = new Mappings(false);

            var function = new TransformFunction(new Func<string, int, string>((stringColumn, number) => stringColumn + number.ToString()), null, null);
            var parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterColumn("StringColumn", ETypeCode.String),
                    new ParameterValue("number", ETypeCode.Int32, 123),
                },
                ReturnParameter = new ParameterOutputColumn("CustomFunction", ETypeCode.String)
            };

            
            mappings.Add(new MapFunction(function, parameters));
            
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Substring)).GetTransformFunction();
            parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterColumn("name", new TableColumn("StringColumn")),
                    new ParameterValue("start", ETypeCode.Int32, 1),
                    new ParameterValue("end", ETypeCode.Int32, 3),
                },
                ReturnParameter = new ParameterOutputColumn("return", new TableColumn("Substring"))
            };
            mappings.Add(new MapFunction(function, parameters));
            
            
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Concat)).GetTransformFunction();
            parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterArray("value", ETypeCode.String, new List<Parameter>
                    {
                        new ParameterColumn("value", new TableColumn("StringColumn")),
                    })
                },
                ReturnParameter = new ParameterOutputColumn("return", new TableColumn("Concat"))
            };
            mappings.Add(new MapFunction(function, parameters));

            mappings.Add(new MapColumn(new TableColumn("DateColumn", ETypeCode.DateTime), new TableColumn("DateColumn", ETypeCode.DateTime)));
            
            var transformMapping = new TransformMapping(source, mappings);

            Assert.Equal(4, transformMapping.FieldCount);

            var count = 0;
            while (await transformMapping.ReadAsync())
            {
                count = count + 1;
                Assert.Equal("value" + count.ToString().PadLeft(2, '0') + "123", transformMapping["CustomFunction"]);
                Assert.Equal("alu", transformMapping["Substring"]);
                Assert.Equal(Convert.ToDateTime("2015-01-" + count), (DateTime)transformMapping["DateColumn"]);
            }
            Assert.Equal(10, count);
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task MappingPerformancePassthrough(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var transformMapping = new TransformMapping(data, new Mappings());

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
            var mappings = new Mappings(false);
            for (var i = 0; i < data.FieldCount; i++)
            {
                mappings.Add(new MapColumn(new TableColumn(data.GetName(i))));
            }

            var transformMapping = new TransformMapping(data, mappings);

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
            var mappings = new Mappings(false);
            for (var i = 0; i < data.FieldCount; i++)
            {
                var function = new TransformFunction(new Func<object, object>((value) => value), null, null);
                var parameters = new Parameters()
                {
                    Inputs = new List<Parameter>()
                    {
                        new ParameterColumn(data.GetName(i), ETypeCode.String)
                    },
                    ReturnParameter = new ParameterOutputColumn(data.GetName(i), ETypeCode.String)
                };
            }

            var transformMapping = new TransformMapping(data, mappings);

            var count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceSummary());

        }

    }
}
