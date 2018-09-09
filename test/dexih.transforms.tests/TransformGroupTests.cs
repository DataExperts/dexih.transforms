using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Xunit;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformGroupTests
    {
        
        private readonly string _aggregateFunctions = typeof(AggregateFunctions).FullName;

        private Mappings AggregateMappings()
        {
            var mappings = new Mappings(false);

            var intParameter = new ParameterColumn("IntColumn", ETypeCode.Int32);
            var stringParameter = new ParameterColumn("StringColumn", ETypeCode.String);

            var sum = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Sum)).GetTransformFunction();
            mappings.Add(new MapFunction(sum,
                new Parameters()
                {
                    Inputs = new[] {intParameter},
                    ResultReturnParameter = new ParameterOutputColumn("Sum", ETypeCode.Int32)
                }));

            var avg = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Average))
                .GetTransformFunction();
            mappings.Add(new MapFunction(avg,
                new Parameters()
                {
                    Inputs = new[] {intParameter},
                    ResultReturnParameter = new ParameterOutputColumn("Average", ETypeCode.Double)
                }));

            var min = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Min)).GetTransformFunction();
            mappings.Add(new MapFunction(min,
                new Parameters()
                {
                    Inputs = new[] {intParameter},
                    ResultReturnParameter = new ParameterOutputColumn("Minimum", ETypeCode.Int32)
                }));

            var max = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Max)).GetTransformFunction();
            mappings.Add(new MapFunction(max,
                new Parameters()
                {
                    Inputs = new[] {intParameter},
                    ResultReturnParameter = new ParameterOutputColumn("Maximum", ETypeCode.Int32)
                }));

            var count = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Count))
                .GetTransformFunction();
            mappings.Add(new MapFunction(count,
                new Parameters() {ResultReturnParameter = new ParameterOutputColumn("Count", ETypeCode.Int32)}));

            var countdistinct = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.CountDistinct))
                .GetTransformFunction();
            mappings.Add(new MapFunction(countdistinct,
                new Parameters()
                {
                    Inputs = new[] {intParameter},
                    ResultReturnParameter = new ParameterOutputColumn("CountDistinct", ETypeCode.Int32)
                }));

            var concat = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.ConcatAgg))
                .GetTransformFunction();
            mappings.Add(new MapFunction(concat,
                new Parameters()
                {
                    Inputs = new Parameter[]
                    {
                        new ParameterValue("separator", ETypeCode.String, ","),
                        new ParameterColumn("StringColumn", ETypeCode.String)
                    },
                    ResultReturnParameter = new ParameterOutputColumn("Concat", ETypeCode.String)
                }));

            return mappings;
        }

        [Fact]
        public async Task Group_Transform_Functions_NoGroup()
        {
            var source = Helpers.CreateUnSortedTestData();

            var mappings = AggregateMappings();

            // run the group transform with no group, this should aggregate to one row.
            var transformGroup = new TransformGroup(source, mappings);
            
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
            source.Add(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            var mappings = AggregateMappings();
            mappings.Add(new MapGroup(new TableColumn("StringColumn")));
            var transformGroup = new TransformGroup(source, mappings);

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

            mappings.Add(new MapAggregate(intColumn, new TableColumn("Sum", ETypeCode.Double),
                SelectColumn.EAggregate.Sum));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Average", ETypeCode.Double),
                SelectColumn.EAggregate.Average));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Minimum", ETypeCode.Double),
                SelectColumn.EAggregate.Min));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Maximum", ETypeCode.Double),
                SelectColumn.EAggregate.Max));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Count", ETypeCode.Double),
                SelectColumn.EAggregate.Count));

            var transformGroup = new TransformGroup(source, mappings);


            Assert.Equal(5, transformGroup.FieldCount);

            var counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                Assert.Equal(55, transformGroup["Sum"]);
                Assert.Equal(5.5, transformGroup["Average"]);
                Assert.Equal(1, transformGroup["Minimum"]);
                Assert.Equal(10, transformGroup["Maximum"]);
                Assert.Equal(10L, transformGroup["Count"]);
            }

            Assert.Equal(1, counter);
        }

        [Fact]
        public async Task Group_Transform_AggregatePairs_OneGroup()
        {
            var source = Helpers.CreateUnSortedTestData();

            var mappings = new Mappings(false);

            var intColumn = new TableColumn("IntColumn", ETypeCode.Int32);

            mappings.Add(new MapAggregate(intColumn, new TableColumn("Sum", ETypeCode.Double),
                SelectColumn.EAggregate.Sum));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Average", ETypeCode.Double),
                SelectColumn.EAggregate.Average));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Minimum", ETypeCode.Double),
                SelectColumn.EAggregate.Min));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Maximum", ETypeCode.Double),
                SelectColumn.EAggregate.Max));
            mappings.Add(new MapAggregate(intColumn, new TableColumn("Count", ETypeCode.Double),
                SelectColumn.EAggregate.Count));

            //add a row to use for grouping.
            source.Add(new object[] {"value10", 10, 10.1, "2015/01/10", 10, "Even"});

            mappings.Add(new MapGroup(new TableColumn("StringColumn")));
            var transformGroup = new TransformGroup(source, mappings);

            var counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal(counter, transformGroup["Sum"]);
                    Assert.Equal((double) counter, transformGroup["Average"]);
                    Assert.Equal(counter, transformGroup["Minimum"]);
                    Assert.Equal(counter, transformGroup["Maximum"]);
                    Assert.Equal(1L, transformGroup["Count"]);
                }
                else
                {
                    Assert.Equal(20, transformGroup["Sum"]);
                    Assert.Equal(10d, transformGroup["Average"]);
                    Assert.Equal(10, transformGroup["Minimum"]);
                    Assert.Equal(10, transformGroup["Maximum"]);
                    Assert.Equal(2L, transformGroup["Count"]);
                }
            }

            Assert.Equal(10, counter);
        }


      

      
        
     

    }
}
