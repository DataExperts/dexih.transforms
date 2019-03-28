using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Parameter;
using dexih.transforms.Mapping;
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

            var function = new TransformFunction(new Func<string, int, string>((stringColumn, number) => stringColumn + number.ToString()), typeof(bool), null, null);
            var parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterColumn("StringColumn", ETypeCode.String),
                    new ParameterValue("number", ETypeCode.Int32, 123),
                },
                ReturnParameters =  new List<Parameter> { new ParameterOutputColumn("CustomFunction", ETypeCode.String)}
            };

            
            mappings.Add(new MapFunction(function, parameters, MapFunction.EFunctionCaching.NoCache));
            
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Substring), Helpers.BuiltInAssembly).GetTransformFunction(typeof(string));
            parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterColumn("name", new TableColumn("StringColumn")),
                    new ParameterValue("start", ETypeCode.Int32, 1),
                    new ParameterValue("end", ETypeCode.Int32, 3),
                },
                ReturnParameters = new List<Parameter> { new ParameterOutputColumn("return", new TableColumn("Substring"))}
            };
            mappings.Add(new MapFunction(function, parameters, MapFunction.EFunctionCaching.NoCache));
            
            
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Concat), Helpers.BuiltInAssembly).GetTransformFunction(typeof(string));
            parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterArray("value", ETypeCode.String, 1, new List<Parameter>
                    {
                        new ParameterColumn("value", new TableColumn("StringColumn")),
                    })
                },
                ReturnParameters = new List<Parameter> {  new ParameterOutputColumn("return", new TableColumn("Concat"))}
            };
            mappings.Add(new MapFunction(function, parameters, MapFunction.EFunctionCaching.NoCache));

            mappings.Add(new MapColumn(new TableColumn("DateColumn", ETypeCode.DateTime), new TableColumn("DateColumn", ETypeCode.DateTime)));
            mappings.Add(new MapColumn(new TableColumn("ArrayColumn", ETypeCode.DateTime), new TableColumn("ArrayColumn", ETypeCode.Int32, rank: 1)));
            
            var transformMapping = new TransformMapping(source, mappings);
            await transformMapping.Open();
            
            Assert.Equal(5, transformMapping.FieldCount);

            var count = 0;
            while (await transformMapping.ReadAsync())
            {
                count = count + 1;
                Assert.Equal("value" + count.ToString().PadLeft(2, '0') + "123", transformMapping["CustomFunction"]);
                Assert.Equal("alu", transformMapping["Substring"]);
                Assert.Equal(Convert.ToDateTime("2015-01-" + count), (DateTime)transformMapping["DateColumn"]);
                Assert.Equal(new[] {1,1}, transformMapping["ArrayColumn"]);
            }
            Assert.Equal(10, count);
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task MappingPerformancePassThrough(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var transformMapping = new TransformMapping(data, new Mappings());
            await transformMapping.Open();
            
            var count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceDetails());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 115ms
        public async Task MappingPerformanceColumnPairs(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var mappings = new Mappings(false);
            for (var i = 0; i < data.FieldCount; i++)
            {
                mappings.Add(new MapColumn(new TableColumn(data.GetName(i))));
            }

            var transformMapping = new TransformMapping(data, mappings);
            await transformMapping.Open();

            var count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceDetails());

        }

        [Theory]
        [InlineData(100000)] //should run in ~ 900ms
        public async Task MappingPerformanceFunctions(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var mappings = new Mappings(false);
            for (var i = 0; i < data.FieldCount; i++)
            {
                var function = new TransformFunction(new Func<object, object>((value) => value), typeof(string), null, null);
                var parameters = new Parameters
                {
                    Inputs = new List<Parameter>
                    {
                        new ParameterColumn(data.GetName(i), ETypeCode.String)
                    },
                    ReturnParameters = new List<Parameter> { new ParameterOutputColumn(data.GetName(i), ETypeCode.String)}
                };
                
                mappings.Add(new MapFunction(function, parameters, MapFunction.EFunctionCaching.NoCache));
            }

            var transformMapping = new TransformMapping(data, mappings);
            await transformMapping.Open();
            
            var count = 0;
            while (await transformMapping.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformMapping.PerformanceDetails());

        }

    }
}
