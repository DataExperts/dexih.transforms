using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class TransformGroupTests
    {

        [Fact]
        public void Group_Aggregates()
        {
            ReaderMemory Source = Helpers.CreateUnSortedTestData();

            List<Function> Aggregates = new List<Function>();

            dexih.functions.Parameter[] IntParam = new dexih.functions.Parameter[] { new dexih.functions.Parameter("IntColumn", ETypeCode.Double, true, null, new TableColumn("IntColumn", ETypeCode.Int32)) };
            dexih.functions.Parameter[] StringParam = new dexih.functions.Parameter[] { new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn")) };
            dexih.functions.Parameter[] ConcatParam = new dexih.functions.Parameter[] { new dexih.functions.Parameter("Seperator", ETypeCode.String, false, ","), new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn")) };

            Function sum = StandardFunctions.GetFunctionReference("Sum");
            sum.Inputs = IntParam;
            sum.TargetColumn = new TableColumn("Sum", ETypeCode.Double);
            Function average = StandardFunctions.GetFunctionReference("Average");
            average.Inputs = IntParam;
            average.TargetColumn = new TableColumn("Average", ETypeCode.Double);
            Function min = StandardFunctions.GetFunctionReference("Min");
            min.Inputs = IntParam;
            min.TargetColumn = new TableColumn("Minimum", ETypeCode.Double);
            Function max = StandardFunctions.GetFunctionReference("Max");
            max.Inputs = IntParam;
            max.TargetColumn = new TableColumn("Maximum", ETypeCode.Double);
            Function count = StandardFunctions.GetFunctionReference("Count");
            count.TargetColumn = new TableColumn("Count", ETypeCode.Double);
            Function countdistinct = StandardFunctions.GetFunctionReference("CountDistinct");
            countdistinct.Inputs = StringParam;
            countdistinct.TargetColumn = new TableColumn("CountDistinct", ETypeCode.Double);
            Function concat = StandardFunctions.GetFunctionReference("ConcatAgg");
            concat.Inputs = ConcatParam;
            concat.TargetColumn = new TableColumn("Concat", ETypeCode.String);

            Aggregates.Add(sum);
            Aggregates.Add(average);
            Aggregates.Add(min);
            Aggregates.Add(max);
            Aggregates.Add(count);
            Aggregates.Add(countdistinct);
            Aggregates.Add(concat);

            TransformGroup transformGroup = new TransformGroup(Source, null, Aggregates, false);

            Assert.Equal(7, transformGroup.FieldCount);

            int counter = 0;
            while (transformGroup.Read() == true)
            {
                counter = counter + 1;
                Assert.Equal((Double)55, transformGroup["Sum"]);
                Assert.Equal((Double)5.5, transformGroup["Average"]);
                Assert.Equal((Double)1, transformGroup["Minimum"]);
                Assert.Equal((Double)10, transformGroup["Maximum"]);
                Assert.Equal(10, transformGroup["Count"]);
                Assert.Equal(10, transformGroup["CountDistinct"]);
            }
            Assert.Equal(1, counter);

            //add a row to use for grouping.
            Source.Add(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            List<ColumnPair> GroupColumns = new List<ColumnPair>() { new ColumnPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) };

            transformGroup = new TransformGroup(Source, GroupColumns, Aggregates, false);

            counter = 0;
            while (transformGroup.Read() == true)
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal("value0" + counter.ToString(), transformGroup["StringColumn"]);
                    Assert.Equal((Double)counter, transformGroup["Sum"]);
                    Assert.Equal((Double)counter, transformGroup["Average"]);
                    Assert.Equal((Double)counter, transformGroup["Minimum"]);
                    Assert.Equal((Double)counter, transformGroup["Maximum"]);
                    Assert.Equal(1, transformGroup["Count"]);
                    Assert.Equal(1, transformGroup["CountDistinct"]);
                }
                else
                {
                    Assert.Equal((Double)20, transformGroup["Sum"]);
                    Assert.Equal((Double)10, transformGroup["Average"]);
                    Assert.Equal((Double)10, transformGroup["Minimum"]);
                    Assert.Equal((Double)10, transformGroup["Maximum"]);
                    Assert.Equal(2, transformGroup["Count"]);
                    Assert.Equal(1, transformGroup["CountDistinct"]);
                }
            }
            Assert.Equal(10, counter);
        }

        [Fact]
        public void Group_SeriesTests()
        {
            dexih.transforms.ReaderMemory Source = Helpers.CreateSortedTestData();

            //add a row to test highest since.
            Source.Add(new object[] { "value11", 5, 10.1, "2015/01/11", 1 });
            Source.Reset();

            List<Function> Aggregates = new List<Function>();

            Function mavg = StandardFunctions.GetFunctionReference("MovingAverage");
            mavg.Inputs = new dexih.functions.Parameter[] {
                new dexih.functions.Parameter("Series", ETypeCode.DateTime, true, null, new TableColumn("DateColumn", ETypeCode.DateTime)),
                new dexih.functions.Parameter("Value", ETypeCode.Double, true, null, new TableColumn("IntColumn", ETypeCode.Double)),
                new dexih.functions.Parameter("PreCount", ETypeCode.Int32, value: 3),
                new dexih.functions.Parameter("PostCount", ETypeCode.Int32, value: 3)
            };
            mavg.TargetColumn = new TableColumn("MAvg", ETypeCode.Double);
            Aggregates.Add(mavg);

            Function highest = StandardFunctions.GetFunctionReference("HighestSince");
            highest.Inputs = new dexih.functions.Parameter[] {
                new dexih.functions.Parameter("Series", ETypeCode.DateTime, true, null, new TableColumn("DateColumn", ETypeCode.DateTime)),
                new dexih.functions.Parameter("Value", ETypeCode.Double, true, null, new TableColumn("IntColumn", ETypeCode.Int32))
            };
            highest.Outputs = new dexih.functions.Parameter[] {
                new dexih.functions.Parameter("Value", ETypeCode.Double, true, null, new TableColumn("HighestValue", ETypeCode.Double))
            };
            highest.TargetColumn = new TableColumn("Highest", ETypeCode.Double);
            Aggregates.Add(highest);

            List<ColumnPair> GroupColumns = new List<ColumnPair>() { new ColumnPair(new TableColumn("DateColumn", ETypeCode.DateTime), new TableColumn("DateColumn", ETypeCode.DateTime)) };

            TransformGroup transformGroup = new TransformGroup(Source, null, Aggregates, true);

            Assert.Equal(8, transformGroup.FieldCount);

            int counter = 0;
            Double[] MAvgExpectedValues = { 2.5, 3, 3.5, 4, 5, 6, 7, 7.14, 7.5, 7.8, 8 };
            String[] HighestExpectedValues = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10", "2015/01/10" };
            while (transformGroup.Read() == true)
            {
                Assert.Equal((Double)MAvgExpectedValues[counter], Math.Round((Double)transformGroup["MAvg"], 2));
                Assert.Equal(HighestExpectedValues[counter], ((DateTime)transformGroup["Highest"]).ToString("yyyy/MM/dd"));
                counter = counter + 1;
            }
            Assert.Equal(11, counter);
        }

    }
}
