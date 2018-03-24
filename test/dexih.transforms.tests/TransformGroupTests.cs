using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformGroupTests
    {

        [Fact]
        public async Task Group_Aggregates()
        {
            var source = Helpers.CreateUnSortedTestData();

            var aggregates = new List<TransformFunction>();

            var intParam = new Parameter[] { new Parameter("IntColumn", ETypeCode.Double, true, null, new TableColumn("IntColumn", ETypeCode.Int32)) };
            var stringParam = new Parameter[] { new Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn")) };
            var concatParam = new Parameter[] { new Parameter("Seperator", ETypeCode.String, false, ","), new Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn")) };

            var sum = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Sum").GetTransformFunction();
            sum.Inputs = intParam;
            sum.TargetColumn = new TableColumn("Sum", ETypeCode.Double);
            var average = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Average").GetTransformFunction();
            average.Inputs = intParam;
            average.TargetColumn = new TableColumn("Average", ETypeCode.Double);
            var min = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Min").GetTransformFunction();
            min.Inputs = intParam;
            min.TargetColumn = new TableColumn("Minimum", ETypeCode.Double);
            var max = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Max").GetTransformFunction();
            max.Inputs = intParam;
            max.TargetColumn = new TableColumn("Maximum", ETypeCode.Double);
            var count = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Count").GetTransformFunction();
            count.TargetColumn = new TableColumn("Count", ETypeCode.Double);
            var countdistinct = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "CountDistinct").GetTransformFunction();
            countdistinct.Inputs = stringParam;
            countdistinct.TargetColumn = new TableColumn("CountDistinct", ETypeCode.Double);
            var concat = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "ConcatAgg").GetTransformFunction();
            concat.Inputs = concatParam;
            concat.TargetColumn = new TableColumn("Concat", ETypeCode.String);

            aggregates.Add(sum);
            aggregates.Add(average);
            aggregates.Add(min);
            aggregates.Add(max);
            aggregates.Add(count);
            aggregates.Add(countdistinct);
            aggregates.Add(concat);

            var transformGroup = new TransformGroup(source, null, aggregates, false);

            Assert.Equal(7, transformGroup.FieldCount);

            var counter = 0;
            while (await transformGroup.ReadAsync() == true)
            {
                counter = counter + 1;
                Assert.Equal((double)55, transformGroup["Sum"]);
                Assert.Equal((double)5.5, transformGroup["Average"]);
                Assert.Equal((double)1, transformGroup["Minimum"]);
                Assert.Equal((double)10, transformGroup["Maximum"]);
                Assert.Equal(10, transformGroup["Count"]);
                Assert.Equal(10, transformGroup["CountDistinct"]);
            }
            Assert.Equal(1, counter);

            //add a row to use for grouping.
            source.Add(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            var groupColumns = new List<ColumnPair>() { new ColumnPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) };

            transformGroup = new TransformGroup(source, groupColumns, aggregates, false);

            counter = 0;
            while (await transformGroup.ReadAsync() == true)
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal("value0" + counter.ToString(), transformGroup["StringColumn"]);
                    Assert.Equal((double)counter, transformGroup["Sum"]);
                    Assert.Equal((double)counter, transformGroup["Average"]);
                    Assert.Equal((double)counter, transformGroup["Minimum"]);
                    Assert.Equal((double)counter, transformGroup["Maximum"]);
                    Assert.Equal(1, transformGroup["Count"]);
                    Assert.Equal(1, transformGroup["CountDistinct"]);
                }
                else
                {
                    Assert.Equal((double)20, transformGroup["Sum"]);
                    Assert.Equal((double)10, transformGroup["Average"]);
                    Assert.Equal((double)10, transformGroup["Minimum"]);
                    Assert.Equal((double)10, transformGroup["Maximum"]);
                    Assert.Equal(2, transformGroup["Count"]);
                    Assert.Equal(1, transformGroup["CountDistinct"]);
                }
            }
            Assert.Equal(10, counter);
        }

        [Fact]
        public async Task Group_SeriesTests()
        {
            var source = Helpers.CreateSortedTestData();

            //add a row to test highest since.
            source.Add(new object[] { "value11", 5, 10.1, "2015/01/11", 1 });
            source.Reset();

            var aggregates = new List<TransformFunction>();

            var mavg = Functions.GetFunction("dexih.functions.BuiltIn.SeriesFunctions", "MovingAverage").GetTransformFunction();
            mavg.Inputs = new Parameter[] {
                new Parameter("Series", ETypeCode.DateTime, true, null, new TableColumn("DateColumn", ETypeCode.DateTime)),
                new Parameter("Value", ETypeCode.Double, true, null, new TableColumn("IntColumn", ETypeCode.Double)),
                new Parameter("PreCount", ETypeCode.Int32, value: 3),
                new Parameter("PostCount", ETypeCode.Int32, value: 3)
            };
            mavg.TargetColumn = new TableColumn("MAvg", ETypeCode.Double);
            aggregates.Add(mavg);

            var highest = Functions.GetFunction("dexih.functions.BuiltIn.SeriesFunctions", "HighestSince").GetTransformFunction();
            highest.Inputs = new Parameter[] {
                new Parameter("Series", ETypeCode.DateTime, true, null, new TableColumn("DateColumn", ETypeCode.DateTime)),
                new Parameter("Value", ETypeCode.Double, true, null, new TableColumn("IntColumn", ETypeCode.Int32))
            };
            highest.Outputs = new Parameter[] {
                new Parameter("Value", ETypeCode.Double, true, null, new TableColumn("HighestValue", ETypeCode.Double))
            };
            highest.TargetColumn = new TableColumn("Highest", ETypeCode.Double);
            aggregates.Add(highest);

            var groupColumns = new List<ColumnPair>() { new ColumnPair(new TableColumn("DateColumn", ETypeCode.DateTime), new TableColumn("DateColumn", ETypeCode.DateTime)) };

            var transformGroup = new TransformGroup(source, null, aggregates, true);

            Assert.Equal(8, transformGroup.FieldCount);

            var counter = 0;
            double[] mAvgExpectedValues = { 2.5, 3, 3.5, 4, 5, 6, 7, 7.14, 7.5, 7.8, 8 };
            string[] highestExpectedValues = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10", "2015/01/10" };
            while (await transformGroup.ReadAsync() == true)
            {
                Assert.Equal((double)mAvgExpectedValues[counter], Math.Round((double)transformGroup["MAvg"], 2));
                Assert.Equal(highestExpectedValues[counter], ((DateTime)transformGroup["Highest"]).ToString("yyyy/MM/dd"));
                counter = counter + 1;
            }
            Assert.Equal(11, counter);
        }

    }
}
