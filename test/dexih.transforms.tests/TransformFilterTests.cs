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
    public class TransformFilterTests
    {
        private readonly ITestOutputHelper output;

        public TransformFilterTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Filters()
        {
            ReaderMemory Table = Helpers.CreateSortedTestData();

            //set a filter that filters all
            List<Function> Conditions = new List<Function>();
            Function Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null,  new TableColumn("StringColumn"), isArray: true  ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk", isArray: true ) };
            Conditions.Add(Function);

            TransformFilter TransformFilter = new TransformFilter(Table, Conditions);

            Assert.Equal(5, TransformFilter.FieldCount);

            int count = 0;
            while (await TransformFilter.ReadAsync() == true)
            {
                count = count + 1;
            }
            Assert.Equal(0, count);

            //set a filter than filters to 1 row.
            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null,  new TableColumn("StringColumn"), isArray: true ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "value03", isArray: true ) };
            Conditions.Add(Function);

            TransformFilter.Conditions = Conditions;
            TransformFilter.Reset();

            count = 0;
            while (await TransformFilter.ReadAsync() == true)
            {
                count = count + 1;
                if (count == 1)
                    Assert.Equal(3, TransformFilter["IntColumn"]);
            }
            Assert.Equal(1, count);

            // use the "IN" function to filter 3 rows.
            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("IsIn");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Value", ETypeCode.String, true, null,  new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value03", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value05", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value07", isArray: true) };

            Conditions.Add(Function);
            TransformFilter.Conditions = Conditions;
            Table.Reset();
            TransformFilter.SetInTransform(Table);

            count = 0;
            while (await TransformFilter.ReadAsync() == true)
            {
                count = count + 1;
            }
            Assert.Equal(3, count);

            // create a mapping, and use the filter after the calculation.
            List<Function> Mappings = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("Substring");
            Function.TargetColumn = new TableColumn("Substring");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null,  new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 5),
                    new dexih.functions.Parameter("end", ETypeCode.Int32, false, 50) };
            Mappings.Add(Function);

            Table.Reset();
            TransformMapping TransformMapping = new TransformMapping(Table, false, null, Mappings);

            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("LessThan");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Substring", ETypeCode.Int32, true, null,  new TableColumn("Substring") ),
                    new dexih.functions.Parameter("Compare", ETypeCode.Int32, false, 5) };
            Conditions.Add(Function);
            TransformFilter.Conditions = Conditions;
            TransformFilter.SetInTransform(TransformMapping);

            count = 0;
            while (await TransformFilter.ReadAsync() == true)
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
            TransformFilter transformFilter = new TransformFilter();
            transformFilter.SetInTransform(data);

            int count = 0;
            while (await transformFilter.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transformFilter.PerformanceSummary());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task FilterPerformanceFilterAll(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            TransformFilter transformFilter = new TransformFilter();

            List<Function> filters = new List<Function>();

            Function newFilter = new Function(new Func<int, bool>((value) => value < 0), new TableColumn[] { new TableColumn(data.GetName(0)) }, null, null);
            filters.Add(newFilter);
            transformFilter.Functions = filters;
            transformFilter.SetInTransform(data);

            int count = 0;
            while (await transformFilter.ReadAsync())
                count++;

            Assert.Equal(0, count);

            output.WriteLine(transformFilter.PerformanceSummary());
        }

    }
}
