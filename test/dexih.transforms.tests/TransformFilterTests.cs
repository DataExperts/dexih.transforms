using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Xunit;
using Xunit.Abstractions;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformFilterTests
    {
        private readonly ITestOutputHelper _output;

        public TransformFilterTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData("StringColumn", ETypeCode.String, "value01", Filter.ECompare.IsEqual, 1)]
        [InlineData("StringColumn", ETypeCode.String, "value01", Filter.ECompare.NotEqual, 9)]
        [InlineData("StringColumn", ETypeCode.String, "value02", Filter.ECompare.GreaterThan, 8)]
        [InlineData("StringColumn", ETypeCode.String, "value02", Filter.ECompare.GreaterThanEqual, 9)]
        [InlineData("StringColumn", ETypeCode.String, "value02", Filter.ECompare.LessThan, 1)]
        [InlineData("StringColumn", ETypeCode.String, "value02", Filter.ECompare.LessThanEqual, 2)]
        [InlineData("IntColumn", ETypeCode.Int32, 1, Filter.ECompare.IsEqual, 1)]
        [InlineData("IntColumn", ETypeCode.Int32, 1, Filter.ECompare.NotEqual, 9)]
        [InlineData("IntColumn", ETypeCode.Int32, 2, Filter.ECompare.GreaterThan, 8)]
        [InlineData("IntColumn", ETypeCode.Int32, 2, Filter.ECompare.GreaterThanEqual, 9)]
        [InlineData("IntColumn", ETypeCode.Int32, 2, Filter.ECompare.LessThan, 1)]
        [InlineData("IntColumn", ETypeCode.Int32, 2, Filter.ECompare.LessThanEqual, 2)]
        [MemberData(nameof(FilterPairDateTests))]
        public async Task FilterPairs(string columnName, ETypeCode dataType, object filterValue, Filter.ECompare filterCompare, int expctedRows)
        {
            var table = Helpers.CreateSortedTestData();

            var mappings = new Mappings()
            {
                new MapFilter(new TableColumn(columnName, dataType), filterValue, filterCompare)
            };

            // set a junk filter that filters
            var transformFilter = new TransformFilter(table, mappings);
            await transformFilter.Open(0, null, CancellationToken.None);

            Assert.Equal(5, transformFilter.FieldCount);
            
            var count = 0;
            while (await transformFilter.ReadAsync())
            {
                count = count + 1;
            }
            Assert.Equal(expctedRows, count);
        }

        public static IEnumerable<object[]> FilterPairDateTests => new[]
        {
            new object[] { "DateColumn", ETypeCode.DateTime, Convert.ToDateTime("2015/01/01"), Filter.ECompare.IsEqual, 1},
            new object[] { "DateColumn", ETypeCode.DateTime, Convert.ToDateTime("2015/01/01"), Filter.ECompare.NotEqual, 9},
            new object[] { "DateColumn", ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.GreaterThan, 8},
            new object[] { "DateColumn", ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.GreaterThanEqual, 9},
            new object[] { "DateColumn", ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.LessThan, 1},
            new object[] { "DateColumn", ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.LessThanEqual, 2}
        };

        [Fact]
        public async Task Filters()
        {
            var table = Helpers.CreateSortedTestData();

            //set a filter that filters all
            var function = Functions.GetFunction(typeof(ConditionFunctions).FullName, nameof(ConditionFunctions.IsEqual)).GetTransformFunction();
            var parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterArray("Compare", ETypeCode.String, new List<Parameter>
                    {
                        new ParameterColumn("StringColumn", new TableColumn("StringColumn")),
                        new ParameterValue("Compare", ETypeCode.String, "junk")
                    })
                },
            };
            
            var mappings = new Mappings()
            {
                new MapFunction(function, parameters)
            };

            var transformFilter = new TransformFilter(table, mappings);
            await transformFilter.Open(0, null, CancellationToken.None);

            Assert.Equal(5, transformFilter.FieldCount);

            var count = 0;
            while (await transformFilter.ReadAsync())
            {
                count = count + 1;
            }
            Assert.Equal(0, count);

            //set a filter than filters to 1 row.
            parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterArray("Compare", ETypeCode.String, new List<Parameter>
                    {
                        new ParameterColumn("StringColumn", new TableColumn("StringColumn")),
                        new ParameterValue("Compare", ETypeCode.String, "value03")
                    })
                }
            };

            transformFilter.Mappings = new Mappings { new MapFunction(function, parameters) };
            transformFilter.Reset();

            count = 0;
            while (await transformFilter.ReadAsync())
            {
                count = count + 1;
                if (count == 1)
                    Assert.Equal(3, transformFilter["IntColumn"]);
            }
            Assert.Equal(1, count);

            // use the "IN" function to filter 3 rows.
            //set a filter than filters to 1 row.
            function = Functions.GetFunction(typeof(ConditionFunctions).FullName, nameof(ConditionFunctions.IsIn)).GetTransformFunction();
            parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterColumn("StringColumn", new TableColumn("StringColumn")),
                    new ParameterArray("CompareTo", ETypeCode.String, new List<Parameter>
                    {
                        new ParameterValue("CompareTo", ETypeCode.String, "value03"),
                        new ParameterValue("CompareTo", ETypeCode.String, "value05"),
                        new ParameterValue("CompareTo", ETypeCode.String, "value07")
                    })
                }
            };
            transformFilter.Mappings = new Mappings { new MapFunction(function, parameters) };
            table.Reset();
            transformFilter.SetInTransform(table);

            count = 0;
            while (await transformFilter.ReadAsync())
            {
                count = count + 1;
            }
            Assert.Equal(3, count);

            // create a mapping, and use the filter after the calculation.
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Substring)).GetTransformFunction();
            parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterColumn("name", new TableColumn("StringColumn")),
                    new ParameterValue("start", ETypeCode.Int32, 5),
                    new ParameterValue("end", ETypeCode.Int32, 50),
                },
                ReturnParameter = new ParameterOutputColumn("return", new TableColumn("Substring"))
            };
            table.Reset();
            var transformMapping = new TransformMapping(table, new Mappings { new MapFunction(function, parameters) });
            
            function = Functions.GetFunction(typeof(ConditionFunctions).FullName, nameof(ConditionFunctions.LessThan)).GetTransformFunction();
            parameters = new Parameters()
            {
                Inputs = new List<Parameter>()
                {
                    new ParameterColumn("Substring", new TableColumn("Substring", ETypeCode.Int32)),
                    new ParameterValue("Compare", ETypeCode.Int32, 5),
                },
            };
            transformFilter.Mappings = new Mappings { new MapFunction(function, parameters) };
            transformFilter.SetInTransform(transformMapping);

            count = 0;
            while (await transformFilter.ReadAsync())
            {
                count = count + 1;
            }
            Assert.Equal(4, count);

        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task FilterPerformanceEmpty(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var transformFilter = new TransformFilter();
            transformFilter.SetInTransform(data);

            var count = 0;
            while (await transformFilter.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            _output.WriteLine(transformFilter.PerformanceSummary());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task FilterPerformanceFilterAll(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            
            var function = new TransformFunction(new Func<int, bool>(value => value < 0), null, null);

            var mappings = new Mappings()
            {
                new MapFunction(function, new Parameters()
                {
                    Inputs = new List<Parameter>()
                    {
                        new ParameterColumn("value", new TableColumn(data.GetName(0), ETypeCode.Int32))
                    }
                })
            };

            var transformFilter = new TransformFilter(data, mappings);

            var count = 0;
            while (await transformFilter.ReadAsync())
                count++;

            Assert.Equal(0, count);

            _output.WriteLine(transformFilter.PerformanceSummary());
        }

    }
}
