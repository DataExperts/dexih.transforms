using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Xunit;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformGroupTests
    {
        
        private readonly string _aggregateFunctions = typeof(AggregateFunctions).FullName;
        private readonly string _seriesFunctions = typeof(SeriesFunctions).FullName;

        [Fact]
        public async Task Group_Aggregates_Functions()
        {
            var source = Helpers.CreateUnSortedTestData();

//            var aggregates = new List<TransformFunction>();
            
            var mappings = new Mappings(false, true);
            
            var intParameter = new ParameterColumn("IntColumn", ETypeCode.Int32);
            var stringParameter = new ParameterColumn("StringColumn", ETypeCode.String);

            var sum = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Sum)).GetTransformFunction();
            mappings.Add(new MapFunction(sum, new Parameters() {Inputs = new [] {intParameter}, ResultReturnParameter = new ParameterOutputColumn("Sum", ETypeCode.Int32)}) );

            var avg = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Average)).GetTransformFunction();
            mappings.Add(new MapFunction(avg, new Parameters() {Inputs = new [] {intParameter}, ResultReturnParameter = new ParameterOutputColumn("Average", ETypeCode.Double)}) );

            var min = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Min)).GetTransformFunction();
            mappings.Add(new MapFunction(min, new Parameters() {Inputs = new [] {intParameter}, ResultReturnParameter = new ParameterOutputColumn("Minimum", ETypeCode.Int32)}) );

            var max = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Max)).GetTransformFunction();
            mappings.Add(new MapFunction(max, new Parameters() {Inputs = new [] {intParameter}, ResultReturnParameter = new ParameterOutputColumn("Maximum", ETypeCode.Int32)}) );

            var count = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.Count)).GetTransformFunction();
            mappings.Add(new MapFunction(count, new Parameters() { ResultReturnParameter = new ParameterOutputColumn("Count", ETypeCode.Int32)}) );

            var countdistinct = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.CountDistinct)).GetTransformFunction();
            mappings.Add(new MapFunction(countdistinct, new Parameters() {Inputs = new [] {intParameter}, ResultReturnParameter = new ParameterOutputColumn("CountDistinct", ETypeCode.Int32)}) );

            var concat = Functions.GetFunction(_aggregateFunctions, nameof(AggregateFunctions.ConcatAgg)).GetTransformFunction();
            mappings.Add(new MapFunction(concat, new Parameters() {Inputs = new Parameter[] {new ParameterValue("seperator", ETypeCode.String, ","), new ParameterColumn("StringColumn", ETypeCode.String)}, ResultReturnParameter = new ParameterOutputColumn("Concat", ETypeCode.String)}) );

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

            //add a row to use for grouping.
            source.Add(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            // var groupColumns = new List<ColumnPair> { new ColumnPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) };
            
            mappings.Add(new MapGroup(new TableColumn("StringColumn")));
            transformGroup = new TransformGroup(source, mappings);

            //transformGroup = new TransformGroup(source, groupColumns, aggregates, null, false);

            counter = 0;
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
        public async Task Group_Aggregates_AggregatePairs()
        {
            var source = Helpers.CreateUnSortedTestData();

            var mappings = new Mappings(false, true);

            var intColumn = new TableColumn("IntColumn", ETypeCode.Int32);
            
            mappings.Add(new MapAggregate(intColumn,new TableColumn("Sum", ETypeCode.Double), SelectColumn.EAggregate.Sum));
            mappings.Add(new MapAggregate(intColumn,new TableColumn("Average", ETypeCode.Double), SelectColumn.EAggregate.Average));
            mappings.Add(new MapAggregate(intColumn,new TableColumn("Minimum", ETypeCode.Double), SelectColumn.EAggregate.Min));
            mappings.Add(new MapAggregate(intColumn,new TableColumn("Maximum", ETypeCode.Double), SelectColumn.EAggregate.Max));
            mappings.Add(new MapAggregate(intColumn,new TableColumn("Count", ETypeCode.Double), SelectColumn.EAggregate.Count));
            
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

            //add a row to use for grouping.
            source.Add(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            mappings.Add(new MapGroup(new TableColumn("StringColumn")));
            transformGroup = new TransformGroup(source, mappings);

            counter = 0;
            while (await transformGroup.ReadAsync())
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal(counter, transformGroup["Sum"]);
                    Assert.Equal((double)counter, transformGroup["Average"]);
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
        
         [Fact]
        public async Task Group_SeriesTests1()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32, TableColumn.EDeltaType.TrackingField)
            );

            // data with gaps in the date sequence.
            table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10 );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 );
            table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 );
            table.AddRow("value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5 );
            table.AddRow("value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4 );
            table.AddRow("value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2 );
            table.AddRow("value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1);

            var source = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") } );
            source.Reset();

            var mappings = new Mappings(false, true);

            var mavg = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.MovingAverage)).GetTransformFunction();
            
            var parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("DateColumn", ETypeCode.DateTime),
                    new ParameterColumn("IntColumn", ETypeCode.Double),
                    new ParameterValue("PreCount", ETypeCode.Int32, 3),
                    new ParameterValue("PostCount", ETypeCode.Int32, 3)
                },
                ResultReturnParameter = new ParameterOutputColumn("MAvg", ETypeCode.Double)
            };
            
            mappings.Add(new MapFunction(mavg, parameters));

            mappings.Add(new MapSeries(new TableColumn("DateColumn"), ESeriesGrain.Day));
            
            var transformGroup = new TransformGroup(source, mappings);
            
            Assert.Equal(2, transformGroup.FieldCount);

            var counter = 0;
            double[] mAvgExpectedValues = { 0.75, 1.6, 2.33, 3, 2.86, 3.86, 5.29, 6.17, 6.4, 6.5 };
            string[] expectedDates = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10" };
            while (await transformGroup.ReadAsync())
            {
                Assert.Equal(mAvgExpectedValues[counter], Math.Round((double)transformGroup["MAvg"], 2));
                Assert.Equal(Convert.ToDateTime(expectedDates[counter]), transformGroup["DateColumn"]);
                counter = counter + 1;
            }
            Assert.Equal(10, counter);
        }

        [Fact]
        public async Task Group_SeriesTests()
        {
            var source = Helpers.CreateSortedTestData();

            //add a row to test highest since.
            source.Add(new object[] { "value11", 5, 10.1, Convert.ToDateTime("2015/01/11"), 1 });
            source.Reset();

            var mappings = new Mappings(true);

            var mavg = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.MovingAverage)).GetTransformFunction();
            
            var parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("DateColumn", ETypeCode.DateTime),
                    new ParameterColumn("IntColumn", ETypeCode.Double),
                    new ParameterValue("PreCount", ETypeCode.Int32, 3),
                    new ParameterValue("PostCount", ETypeCode.Int32, 3)
                },
                ResultReturnParameter = new ParameterOutputColumn("MAvg", ETypeCode.Double)
            };
            
            mappings.Add(new MapFunction(mavg, parameters));
            
            var highest = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.HighestSince)).GetTransformFunction();
            parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("DateColumn", ETypeCode.DateTime),
                    new ParameterColumn("IntColumn", ETypeCode.Double),
                },
                ResultOutputs = new Parameter[]
                {
                    new ParameterOutputColumn("HighestValue", ETypeCode.Double), 
                },
                ResultReturnParameter = new ParameterOutputColumn("Highest", ETypeCode.DateTime)
            };
            mappings.Add(new MapFunction(highest, parameters));

            var transformGroup = new TransformGroup(source, mappings);
            
            Assert.Equal(8, transformGroup.FieldCount);

            var counter = 0;
            double[] mAvgExpectedValues = { 2.5, 3, 3.5, 4, 5, 6, 7, 7.14, 7.5, 7.8, 8 };
            string[] highestExpectedValues = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10", "2015/01/10" };
            while (await transformGroup.ReadAsync())
            {
                Assert.Equal(mAvgExpectedValues[counter], Math.Round((double)transformGroup["MAvg"], 2));
                Assert.Equal(highestExpectedValues[counter], ((DateTime)transformGroup["Highest"]).ToString("yyyy/MM/dd"));
                counter = counter + 1;
            }
            Assert.Equal(11, counter);
        }

    }
}
