using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Xunit;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformGroupTests
    {
        
        private readonly string _aggregateFunctions = typeof(AggregateFunctions<>).FullName;

        private Mappings AggregateMappings()
        {
            var mappings = new Mappings(false);

            var intParameter = new ParameterColumn("IntColumn", ETypeCode.Int32);
            var stringParameter = new ParameterColumn("StringColumn", ETypeCode.String);

            var sum = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<int>.Sum), Helpers.BuiltInAssembly).GetTransformFunction(typeof(int));
            mappings.Add(new MapFunction(sum,
                new Parameters
                {
                    Inputs = new Parameter[] {intParameter},
                    ResultReturnParameters = new [] {new ParameterOutputColumn("Sum", ETypeCode.Int32)}
                }, EFunctionCaching.NoCache)
            );

            var avg = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<int>.Average), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(double));
            mappings.Add(new MapFunction(avg,
                new Parameters
                {
                    Inputs = new Parameter[] {intParameter},
                    ResultReturnParameters = new [] {new ParameterOutputColumn("Average", ETypeCode.Double)}
                }, EFunctionCaching.NoCache)
            );

            var min = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<int>.Min), Helpers.BuiltInAssembly).GetTransformFunction(typeof(int));
            mappings.Add(new MapFunction(min,
                new Parameters
                {
                    Inputs = new Parameter[] {intParameter},
                    ResultReturnParameters = new [] {new ParameterOutputColumn("Minimum", ETypeCode.Int32)}
                }, EFunctionCaching.NoCache)
            );

            var max = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<int>.Max), Helpers.BuiltInAssembly).GetTransformFunction(typeof(int));
            mappings.Add(new MapFunction(max,
                new Parameters
                {
                    Inputs = new Parameter[] {intParameter},
                    ResultReturnParameters = new [] {new ParameterOutputColumn("Maximum", ETypeCode.Int32)}
                }, EFunctionCaching.NoCache)
            );

            var count = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<int>.Count), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(int));
            mappings.Add(new MapFunction(count,
                new Parameters {ResultReturnParameters = new [] { new ParameterOutputColumn("Count", ETypeCode.Int32)}}, EFunctionCaching.NoCache));

            var countDistinct = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<int>.CountDistinct), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(int));
            mappings.Add(new MapFunction(countDistinct,
                new Parameters
                {
                    Inputs = new Parameter[] {intParameter},
                    ResultReturnParameters = new [] {new ParameterOutputColumn("CountDistinct", ETypeCode.Int32)}
                }, EFunctionCaching.NoCache)
            );
            var concat = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions<string>.ConcatAgg), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(string));
            mappings.Add(new MapFunction(concat,
                new Parameters
                {
                    Inputs = new Parameter[]
                    {
                        new ParameterValue("separator", ETypeCode.String, ","),
                        new ParameterColumn("StringColumn", ETypeCode.String)
                    },
                    ResultReturnParameters = new List<Parameter> { new ParameterOutputColumn("Concat", ETypeCode.String)}
                }, EFunctionCaching.NoCache));

            return mappings;
        }

        [Fact]
        public async Task Group_Transform_Functions_NoGroup()
        {
            var source = Helpers.CreateUnSortedTestData();

            var mappings = AggregateMappings();

            // run the group transform with no group, this should aggregate to one row.
            var transformGroup = new TransformGroup(source, mappings);
            await transformGroup.Open();
            
            Assert.Equal(7, transformGroup.FieldCount);

            var counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                Assert.Equal(55, transformGroup["Sum"]);
                Assert.Equal(5.5, transformGroup["Average"]);
                Assert.Equal(1, transformGroup["Minimum"]);
                Assert.Equal(10, transformGroup["Maximum"]);
                Assert.Equal(10, transformGroup["Count"]);
                Assert.Equal(10, transformGroup["CountDistinct"]);
            }
            Assert.Equal(1, counter);
        }
        
         [Fact]
        public async Task Group_Transform_Functions_OneGroup()
        {
            var source = Helpers.CreateUnSortedTestData();

            //add a row to use for grouping.
            source.DataTable.AddRow(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            var mappings = AggregateMappings();
            mappings.Add(new MapGroup(new TableColumn("StringColumn")));
            var transformGroup = new TransformGroup(source, mappings);
            await transformGroup.Open();
            
            var counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal("value0" + counter, transformGroup["StringColumn"]);
                    Assert.Equal(counter, transformGroup["Sum"]);
                    Assert.Equal((double)counter, transformGroup["Average"]);
                    Assert.Equal(counter, transformGroup["Minimum"]);
                    Assert.Equal(counter, transformGroup["Maximum"]);
                    Assert.Equal(1, transformGroup["Count"]);
                    Assert.Equal(1, transformGroup["CountDistinct"]);
                }
                else
                {
                    Assert.Equal(20, transformGroup["Sum"]);
                    Assert.Equal((double)10, transformGroup["Average"]);
                    Assert.Equal(10, transformGroup["Minimum"]);
                    Assert.Equal(10, transformGroup["Maximum"]);
                    Assert.Equal(2, transformGroup["Count"]);
                    Assert.Equal(1, transformGroup["CountDistinct"]);
                }
            }
            Assert.Equal(10, counter);
        }

        [Fact]
        public async Task Group_Transform_AggregatePairs_NoGroup()
        {
            var source = Helpers.CreateUnSortedTestData();

            var mappings = new Mappings(false);

            var intColumn = new TableColumn("IntColumn", ETypeCode.Int32);

            mappings.Add(new MapAggregate(intColumn, new TableColumn("Sum", ETypeCode.Int32),
                EAggregate.Sum));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Average", ETypeCode.Decimal),
                EAggregate.Average));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Minimum", ETypeCode.Int32),
                EAggregate.Min));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Maximum", ETypeCode.Int32),
                EAggregate.Max));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Count", ETypeCode.Int32),
                EAggregate.Count));

            var transformGroup = new TransformGroup(source, mappings);
            await transformGroup.Open();

            Assert.Equal(5, transformGroup.FieldCount);

            var counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                Assert.Equal(55, transformGroup["Sum"]);
                Assert.Equal(5.5m, transformGroup["Average"]);
                Assert.Equal(1, transformGroup["Minimum"]);
                Assert.Equal(10, transformGroup["Maximum"]);
                Assert.Equal(10, transformGroup["Count"]);
            }

            Assert.Equal(1, counter);
        }

        [Fact]
        public async Task Group_Transform_AggregatePairs_OneGroup()
        {
            var source = Helpers.CreateUnSortedTestData();

            var mappings = new Mappings(false);

            var intColumn = new TableColumn("IntColumn", ETypeCode.Int32);

            mappings.Add(new MapAggregate(intColumn, new TableColumn("Sum", ETypeCode.Int32),
                EAggregate.Sum));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Average", ETypeCode.Int32),
                EAggregate.Average));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Minimum", ETypeCode.Int32),
                EAggregate.Min));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Maximum", ETypeCode.Int32),
                EAggregate.Max));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Count", ETypeCode.Int32),
                EAggregate.Count));

            //add a row to use for grouping.
            source.DataTable.AddRow(new object[] {"value10", 10, 10.1, "2015/01/10", 10, "Even"});

            mappings.Add(new MapGroup(new TableColumn("StringColumn")));
            var transformGroup = new TransformGroup(source, mappings);
            await transformGroup.Open();
            
            var counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal(counter, transformGroup["Sum"]);
                    Assert.Equal(counter, transformGroup["Average"]);
                    Assert.Equal(counter, transformGroup["Minimum"]);
                    Assert.Equal(counter, transformGroup["Maximum"]);
                    Assert.Equal(1, transformGroup["Count"]);
                }
                else
                {
                    Assert.Equal(20, transformGroup["Sum"]);
                    Assert.Equal(10, transformGroup["Average"]);
                    Assert.Equal(10, transformGroup["Minimum"]);
                    Assert.Equal(10, transformGroup["Maximum"]);
                    Assert.Equal(2, transformGroup["Count"]);
                }
            }

            Assert.Equal(10, counter);
        }


      

      
        
     

    }
}
