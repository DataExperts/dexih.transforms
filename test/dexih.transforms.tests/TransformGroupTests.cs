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
            SourceTable Source = Helpers.CreateUnSortedTestData();
            TransformGroup TransformGroup = new TransformGroup();

            List<Function> Aggregates = new List<Function>();

            dexih.functions.Parameter[] IntParam = new dexih.functions.Parameter[] { new dexih.functions.Parameter("IntColumn", ETypeCode.Double, true, null, "IntColumn") };
            dexih.functions.Parameter[] StringParam = new dexih.functions.Parameter[] { new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn") };
            dexih.functions.Parameter[] ConcatParam = new dexih.functions.Parameter[] { new dexih.functions.Parameter("Seperator", ETypeCode.String, false, ","), new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn") };

            Function sum = StandardFunctions.GetFunctionReference("Sum");
            sum.Inputs = IntParam;
            sum.TargetColumn = "Sum";
            Function average = StandardFunctions.GetFunctionReference("Average");
            average.Inputs = IntParam;
            average.TargetColumn = "Average";
            Function min = StandardFunctions.GetFunctionReference("Min");
            min.Inputs = IntParam;
            min.TargetColumn = "Minimum";
            Function max = StandardFunctions.GetFunctionReference("Max");
            max.Inputs = IntParam;
            max.TargetColumn = "Maximum";
            Function count = StandardFunctions.GetFunctionReference("Count");
            count.TargetColumn = "Count";
            Function countdistinct = StandardFunctions.GetFunctionReference("CountDistinct");
            countdistinct.Inputs = StringParam;
            countdistinct.TargetColumn = "CountDistinct";
            Function concat = StandardFunctions.GetFunctionReference("ConcatAgg");
            concat.Inputs = ConcatParam;
            concat.TargetColumn = "Concat";

            Aggregates.Add(sum);
            Aggregates.Add(average);
            Aggregates.Add(min);
            Aggregates.Add(max);
            Aggregates.Add(count);
            Aggregates.Add(countdistinct);
            Aggregates.Add(concat);

            TransformGroup.SetMappings(null, Aggregates);
            TransformGroup.SetInTransform(Source);

            Assert.Equal(7, TransformGroup.FieldCount);

            int counter = 0;
            while (TransformGroup.Read() == true)
            {
                counter = counter + 1;
                Assert.Equal((Double)55, TransformGroup["Sum"]);
                Assert.Equal((Double)5.5, TransformGroup["Average"]);
                Assert.Equal((Double)1, TransformGroup["Minimum"]);
                Assert.Equal((Double)10, TransformGroup["Maximum"]);
                Assert.Equal(10, TransformGroup["Count"]);
                Assert.Equal(10, TransformGroup["CountDistinct"]);
            }
            Assert.Equal(1, counter);

            //add a row to use for grouping.
            Source.Add(new object[] { "value10", 10, 10.1, "2015/01/10", 10, "Even" });

            List<ColumnPair> GroupColumns = new List<ColumnPair>() { new ColumnPair("StringColumn", "StringColumn") };
            TransformGroup.SetMappings(GroupColumns, Aggregates);
            TransformGroup.Reset();
            Source.Reset();
            TransformGroup.SetInTransform(Source);

            counter = 0;
            while (TransformGroup.Read() == true)
            {
                counter = counter + 1;
                if (counter < 10)
                {
                    Assert.Equal("value0" + counter.ToString(), TransformGroup["StringColumn"]);
                    Assert.Equal((Double)counter, TransformGroup["Sum"]);
                    Assert.Equal((Double)counter, TransformGroup["Average"]);
                    Assert.Equal((Double)counter, TransformGroup["Minimum"]);
                    Assert.Equal((Double)counter, TransformGroup["Maximum"]);
                    Assert.Equal(1, TransformGroup["Count"]);
                    Assert.Equal(1, TransformGroup["CountDistinct"]);
                }
                else
                {
                    Assert.Equal((Double)20, TransformGroup["Sum"]);
                    Assert.Equal((Double)10, TransformGroup["Average"]);
                    Assert.Equal((Double)10, TransformGroup["Minimum"]);
                    Assert.Equal((Double)10, TransformGroup["Maximum"]);
                    Assert.Equal(2, TransformGroup["Count"]);
                    Assert.Equal(1, TransformGroup["CountDistinct"]);
                }
            }
            Assert.Equal(10, counter);
        }

        [Fact]
        public void Group_SeriesTests()
        {
            dexih.transforms.SourceTable Source = Helpers.CreateSortedTestData();
            TransformGroup TransformGroup = new TransformGroup();

            //add a row to test highest since.
            Source.Add(new object[] { "value11", 5, 10.1, "2015/01/11", 1 });
            Source.Reset();

            List<Function> Aggregates = new List<Function>();

            Function mavg = StandardFunctions.GetFunctionReference("MovingAverage");
            mavg.Inputs = new dexih.functions.Parameter[] {
                new dexih.functions.Parameter("Series", ETypeCode.DateTime, true, null, "DateColumn"),
                new dexih.functions.Parameter("Value", ETypeCode.Double, true, null, "IntColumn"),
                new dexih.functions.Parameter("PreCount", ETypeCode.Int32, value: 3),
                new dexih.functions.Parameter("PostCount", ETypeCode.Int32, value: 3)
            };
            mavg.TargetColumn = "MAvg";
            Aggregates.Add(mavg);

            Function highest = StandardFunctions.GetFunctionReference("HighestSince");
            highest.Inputs = new dexih.functions.Parameter[] {
                new dexih.functions.Parameter("Series", ETypeCode.DateTime, true, null, "DateColumn"),
                new dexih.functions.Parameter("Value", ETypeCode.Double, true, null, "IntColumn")
            };
            highest.Outputs = new dexih.functions.Parameter[] {
                new dexih.functions.Parameter("Value", ETypeCode.Double, true, null, "HighestValue")
            };
            highest.TargetColumn = "Highest";
            Aggregates.Add(highest);

            List<ColumnPair> GroupColumns = new List<ColumnPair>() { new ColumnPair("DateColumn", "DateColumn") };

            TransformGroup.SetMappings(null, Aggregates);
            TransformGroup.PassThroughColumns = true;
            TransformGroup.SetInTransform(Source);

            Assert.Equal(8, TransformGroup.FieldCount);

            int counter = 0;
            Double[] MAvgExpectedValues = { 2.5, 3, 3.5, 4, 5, 6, 7, 7.14, 7.5, 7.8, 8 };
            String[] HighestExpectedValues = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10", "2015/01/10" };
            while (TransformGroup.Read() == true)
            {
                Assert.Equal((Double)MAvgExpectedValues[counter], Math.Round((Double)TransformGroup["MAvg"], 2));
                Assert.Equal(HighestExpectedValues[counter], ((DateTime)TransformGroup["Highest"]).ToString("yyyy/MM/dd"));
                counter = counter + 1;
            }
            Assert.Equal(11, counter);
        }

    }
}
