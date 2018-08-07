using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;
using Dexih.Utils.DataType;
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
            this._output = output;
        }

        [Theory]
        [InlineData("StringColumn", DataType.ETypeCode.String, "value01", Filter.ECompare.IsEqual, 1)]
        [InlineData("StringColumn", DataType.ETypeCode.String, "value01", Filter.ECompare.NotEqual, 9)]
        [InlineData("StringColumn", DataType.ETypeCode.String, "value02", Filter.ECompare.GreaterThan, 8)]
        [InlineData("StringColumn", DataType.ETypeCode.String, "value02", Filter.ECompare.GreaterThanEqual, 9)]
        [InlineData("StringColumn", DataType.ETypeCode.String, "value02", Filter.ECompare.LessThan, 1)]
        [InlineData("StringColumn", DataType.ETypeCode.String, "value02", Filter.ECompare.LessThanEqual, 2)]
        [InlineData("IntColumn", DataType.ETypeCode.Int32, 1, Filter.ECompare.IsEqual, 1)]
        [InlineData("IntColumn", DataType.ETypeCode.Int32, 1, Filter.ECompare.NotEqual, 9)]
        [InlineData("IntColumn", DataType.ETypeCode.Int32, 2, Filter.ECompare.GreaterThan, 8)]
        [InlineData("IntColumn", DataType.ETypeCode.Int32, 2, Filter.ECompare.GreaterThanEqual, 9)]
        [InlineData("IntColumn", DataType.ETypeCode.Int32, 2, Filter.ECompare.LessThan, 1)]
        [InlineData("IntColumn", DataType.ETypeCode.Int32, 2, Filter.ECompare.LessThanEqual, 2)]
        [MemberData(nameof(FilterPairDateTests))]
        public async Task FilterPairs(string columnName, DataType.ETypeCode dataType, object filterValue, Filter.ECompare filterCompare, int expctedRows)
        {
            var table = Helpers.CreateSortedTestData();

            var joinPairs = new List<FilterPair>
            {
                new FilterPair(new TableColumn(columnName, dataType), filterValue, filterCompare)
            };

            // set a junk filter that filters
            var transformFilter = new TransformFilter(table, null, joinPairs);
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
            new object[] { "DateColumn", DataType.ETypeCode.DateTime, Convert.ToDateTime("2015/01/01"), Filter.ECompare.IsEqual, 1},
            new object[] { "DateColumn", DataType.ETypeCode.DateTime, Convert.ToDateTime("2015/01/01"), Filter.ECompare.NotEqual, 9},
            new object[] { "DateColumn", DataType.ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.GreaterThan, 8},
            new object[] { "DateColumn", DataType.ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.GreaterThanEqual, 9},
            new object[] { "DateColumn", DataType.ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.LessThan, 1},
            new object[] { "DateColumn", DataType.ETypeCode.DateTime, Convert.ToDateTime("2015/01/02"), Filter.ECompare.LessThanEqual, 2},
        };

        [Fact]
        public async Task Filters()
        {
            var table = Helpers.CreateSortedTestData();

            //set a filter that filters all
            var conditions = new List<TransformFunction>();
            var function = Functions.GetFunction("dexih.functions.BuiltIn.ConditionFunctions", "IsEqual").GetTransformFunction();
            function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null,  new TableColumn("StringColumn"), isArray: true  ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk", isArray: true ) };
            conditions.Add(function);

            var transformFilter = new TransformFilter(table, conditions, null);
            await transformFilter.Open(0, null, CancellationToken.None);

            Assert.Equal(5, transformFilter.FieldCount);

            var count = 0;
            while (await transformFilter.ReadAsync())
            {
                count = count + 1;
            }
            Assert.Equal(0, count);

            //set a filter than filters to 1 row.
            conditions = new List<TransformFunction>();
            function = Functions.GetFunction("dexih.functions.BuiltIn.ConditionFunctions", "IsEqual").GetTransformFunction();
            function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null,  new TableColumn("StringColumn"), isArray: true ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "value03", isArray: true ) };
            conditions.Add(function);

            transformFilter.Conditions = conditions;
            transformFilter.Reset();

            count = 0;
            while (await transformFilter.ReadAsync() == true)
            {
                count = count + 1;
                if (count == 1)
                    Assert.Equal(3, transformFilter["IntColumn"]);
            }
            Assert.Equal(1, count);

            // use the "IN" function to filter 3 rows.
            conditions = new List<TransformFunction>();
            function = Functions.GetFunction("dexih.functions.BuiltIn.ConditionFunctions", "IsIn").GetTransformFunction();
            function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Value", ETypeCode.String, true, null,  new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value03", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value05", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value07", isArray: true) };

            conditions.Add(function);
            transformFilter.Conditions = conditions;
            table.Reset();
            transformFilter.SetInTransform(table);

            count = 0;
            while (await transformFilter.ReadAsync() == true)
            {
                count = count + 1;
            }
            Assert.Equal(3, count);

            // create a mapping, and use the filter after the calculation.
            var mappings = new List<TransformFunction>();
            function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "Substring").GetTransformFunction();
            function.TargetColumn = new TableColumn("Substring");
            function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null,  new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 5),
                    new dexih.functions.Parameter("end", ETypeCode.Int32, false, 50) };
            mappings.Add(function);

            table.Reset();
            var transformMapping = new TransformMapping(table, false, null, mappings);

            conditions = new List<TransformFunction>();
            function = Functions.GetFunction("dexih.functions.BuiltIn.ConditionFunctions", "LessThan").GetTransformFunction();
            function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Substring", ETypeCode.Int32, true, null,  new TableColumn("Substring") ),
                    new dexih.functions.Parameter("Compare", ETypeCode.Int32, false, 5) };
            conditions.Add(function);
            transformFilter.Conditions = conditions;
            transformFilter.SetInTransform(transformMapping);

            count = 0;
            while (await transformFilter.ReadAsync() == true)
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
            var transformFilter = new TransformFilter();

            var filters = new List<TransformFunction>();

            var newFilter = new TransformFunction(
                new Func<int, bool>((value) => value < 0), 
                new TableColumn[] { new TableColumn(data.GetName(0)) }, 
                null, 
                null, new GlobalVariables(null));
            filters.Add(newFilter);
            transformFilter.Functions = filters;
            transformFilter.SetInTransform(data);

            var count = 0;
            while (await transformFilter.ReadAsync())
                count++;

            Assert.Equal(0, count);

            _output.WriteLine(transformFilter.PerformanceSummary());
        }

    }
}
