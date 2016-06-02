using dexih.transforms;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih_unit_tests
{
    public class test_transforms
    {
        public DataTableAdapter CreateTestData()
        {
            DataTableSimple Table = new DataTableSimple("test", new DataTableColumns() { "StringColumn",
                "IntColumn",
                "DecimalColumn",
                "DateColumn",
                "SortColumn"
                });

            Table.Data.Add(new object[] { "value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10 });
            Table.Data.Add(new object[] { "value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 });
            Table.Data.Add(new object[] { "value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8 });
            Table.Data.Add(new object[] { "value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7 });
            Table.Data.Add(new object[] { "value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 });
            Table.Data.Add(new object[] { "value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5 });
            Table.Data.Add(new object[] { "value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4 });
            Table.Data.Add(new object[] { "value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3 });
            Table.Data.Add(new object[] { "value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2 });
            Table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1 });

            DataTableAdapter Adapter = new DataTableAdapter(Table);
            Adapter.ResetValues();
            return Adapter;
        }

        public DataTableAdapter CreateJoinData()
        {
            DataTableSimple Table = new DataTableSimple("test", new DataTableColumns() {
                "StringColumn",
                "IntColumn",
                "LookupValue"
                });

            Table.Data.Add(new object[] { "value01", 1, "lookup1" });
            Table.Data.Add(new object[] { "value02", 2, "lookup2" });
            Table.Data.Add(new object[] { "value03", 3, "lookup3" });
            Table.Data.Add(new object[] { "value04", 4, "lookup4" });
            Table.Data.Add(new object[] { "value05", 5, "lookup5" });
            Table.Data.Add(new object[] { "value06", 6, "lookup6" });
            Table.Data.Add(new object[] { "value07", 7, "lookup7" });
            Table.Data.Add(new object[] { "value08", 8, "lookup8" });
            Table.Data.Add(new object[] { "value09", 9, "lookup9" });

            DataTableAdapter Adapter = new DataTableAdapter(Table);
            Adapter.ResetValues();
            return Adapter;
        }

        [Fact]
        public void DataReaderAdapterAdapter_Tests()
        {
            DataTableAdapter Table = CreateTestData();

            Assert.Equal(Table.FieldCount, 5);

            int count = 0;
            while (Table.Read() == true)
            {
                count = count + 1;
                Assert.Equal(Table[1], count);
                Assert.Equal(Table["IntColumn"], count);
            }

            Assert.Equal(10, count);
        }

        [Fact]
        public void DataPart_Mapping_Tests()
        {
            DataTableAdapter Source = CreateTestData();
            TransformMapping TransformMapping = new TransformMapping();

            List<Function> Mappings = new List<Function>();

            Mappings.Add(new Function("CustomFunction", false, "test", "return StringColumn + number.ToString();", null, ETypeCode.String,
                new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("number", ETypeCode.Int32, false, 123)
                }, null));

            Function Function = StandardFunctions.GetFunctionReference("Substring");
            Function.TargetColumn = "Substring";
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 1),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 3) };
            Mappings.Add(Function);

            List<ColumnPair> MappingColumn = new List<ColumnPair>();
            MappingColumn.Add(new ColumnPair("DateColumn", "DateColumn"));
            TransformMapping.SetMappings(MappingColumn, Mappings);
            TransformMapping.SetInTransform(Source);

            Assert.Equal(3, TransformMapping.FieldCount);
            Assert.Equal(3, TransformMapping.Fields.Count());

            int count = 0;
            while (TransformMapping.Read() == true)
            {
                count = count + 1;
                Assert.Equal("value" + count.ToString() + "123", TransformMapping["CustomFunction"]);
                Assert.Equal("alu", TransformMapping["Substring"]);
                Assert.Equal((DateTime)Convert.ToDateTime("2015-01-" + count.ToString()), (DateTime)TransformMapping["DateColumn"]);
            }
            Assert.Equal(10, count);

            //test the getschematable table function.
            //DataReaderAdapter SchemaTable = TransformMapping.GetSchemaTable();
            //Assert.Equal("DateColumn", SchemaTable.Rows[0]["ColumnName"]);
            //Assert.Equal("CustomFunction", SchemaTable.Rows[1]["ColumnName"]);
            //Assert.Equal("Substring", SchemaTable.Rows[2]["ColumnName"]);
        }

        [Fact]
        public void DataPart_Condition_Tests()
        {
            DataTableAdapter Table = CreateTestData();
            TransformFilter TransformFilter = new TransformFilter();
            TransformMapping TransformMapping = new TransformMapping();

            //set a filter that filters all
            List<Function> Conditions = new List<Function>();
            Function Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk") };
            Conditions.Add(Function);

            TransformFilter.SetConditions(Conditions);
            TransformFilter.SetInTransform(Table);

            Assert.Equal(5, TransformFilter.FieldCount);
            Assert.Equal(5, TransformFilter.Fields.Count());

            int count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
            }
            Assert.Equal(0, count);

            //set a filter than filters to 1 row.
            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "value03") };
            Conditions.Add(Function);
            TransformFilter.SetConditions(Conditions);
            Table.ResetValues();
            TransformFilter.SetInTransform(Table);

            count = 0;
            while (TransformFilter.Read() == true)
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
                    new dexih.functions.Parameter("Value", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value03", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value05", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value07", isArray: true) };

            Conditions.Add(Function);
            TransformFilter.SetConditions(Conditions);
            Table.ResetValues();
            TransformFilter.SetInTransform(Table);

            count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
            }
            Assert.Equal(3, count);

            // create a mapping, and use the filter after the calculation.
            List<Function> Mappings = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("Substring");
            Function.TargetColumn = "Substring";
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 5),
                    new dexih.functions.Parameter("end", ETypeCode.Int32, false, 50) };
            Mappings.Add(Function);
            TransformMapping.SetMappings(null, Mappings);
            Table.ResetValues();
            TransformMapping.SetInTransform(Table);

            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("LessThan");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Substring", ETypeCode.Int32, true, null, "Substring" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.Int32, false, 5) };
            Conditions.Add(Function);
            TransformFilter.SetConditions(Conditions);
            TransformFilter.SetInTransform(TransformMapping);

            count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
            }
            Assert.Equal(4, count);

        }

        [Fact]
        public void DataPart_Aggregate_Tests()
        {
            DataTableAdapter Source = CreateTestData();
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
            Assert.Equal(7, TransformGroup.Fields.Count());

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
            Source.Add(new object[] { "value10", 10, 10.1, "2015/01/10" });

            List<ColumnPair> GroupColumns = new List<ColumnPair>() { new ColumnPair ("StringColumn", "StringColumn" ) };
            TransformGroup.SetMappings(GroupColumns, Aggregates);
            Source.ResetValues();
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

            //test the getschematable table function.
            //DataReaderAdapter SchemaTable = TransformGroup.GetSchemaTable();
            //Assert.Equal("StringColumn", SchemaTable.Rows[0]["ColumnName"]);
            //Assert.Equal("Sum", SchemaTable.Rows[1]["ColumnName"]);
            //Assert.Equal("Average", SchemaTable.Rows[2]["ColumnName"]);
            //Assert.Equal("Minimum", SchemaTable.Rows[3]["ColumnName"]);
            //Assert.Equal("Maximum", SchemaTable.Rows[4]["ColumnName"]);
            //Assert.Equal("Count", SchemaTable.Rows[5]["ColumnName"]);
            //Assert.Equal("CountDistinct", SchemaTable.Rows[6]["ColumnName"]);
        }

        [Fact]
        public void DataPart_Aggregate_SeriesTests()
        {
            dexih.transforms.DataTableAdapter Source = CreateTestData(); 
            TransformGroup TransformGroup = new TransformGroup();

            //add a row to test highest since.
            Source.Add(new object[] { "value11", 5, 10.1, "2015/01/11", 1 });
            Source.ResetValues();

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

            List<ColumnPair> GroupColumns = new List<ColumnPair>() { new ColumnPair("DateColumn", "DateColumn" ) };

            TransformGroup.SetMappings(null, Aggregates);
            TransformGroup.PassThroughColumns = true;
            TransformGroup.SetInTransform(Source);

            Assert.Equal(8, TransformGroup.FieldCount);
            Assert.Equal(8, TransformGroup.Fields.Count());

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

        [Fact]
        public void DataPart_Sort_Tests()
        {
            dexih.transforms.DataTableAdapter Source = CreateTestData();
            TransformSort TransformSort = new TransformSort();

            TransformSort.SetSortFields(new List<Sort> { new Sort() { Column = "SortColumn", Direction = Sort.EDirection.Ascending } });
            TransformSort.SetInTransform(Source);
            int SortCount = 1;

            Assert.Equal(5, TransformSort.FieldCount);
            Assert.Equal(5, TransformSort.Fields.Count());

            while (TransformSort.Read() == true)
            {
                Assert.Equal(SortCount, TransformSort["SortColumn"]);
                SortCount++;
            }
        }

        [Fact]
        public void DataPart_SortedJoin_Tests()
        {
            dexih.transforms.DataTableAdapter Source = CreateTestData();
            TransformJoin TransformJoin = new TransformJoin();
            TransformJoin.InputIsSorted = true;
            TransformJoin.SetJoins("JoinTable", new List<JoinPair>() { new JoinPair { SourceColumn = "StringColumn", JoinColumn = "StringColumn" } });
            TransformJoin.SetInTransform(Source, CreateJoinData());

            Assert.Equal(8, TransformJoin.FieldCount);
            Assert.Equal(8, TransformJoin.Fields.Count());

            int pos = 0;
            while (TransformJoin.Read() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), TransformJoin["LookupValue"]);
                else
                    Assert.Equal(DBNull.Value, TransformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public void DataPart_Join_Tests()
        {
            dexih.transforms.DataTableAdapter Source = CreateTestData();
            TransformJoin TransformJoin = new TransformJoin();
            TransformJoin.SetJoins("JoinTable", new List<JoinPair>() { new JoinPair { SourceColumn = "StringColumn", JoinColumn = "StringColumn" } });
            TransformJoin.SetInTransform(Source, CreateJoinData());

            Assert.Equal(8, TransformJoin.FieldCount);
            Assert.Equal(8, TransformJoin.Fields.Count());

            int pos = 0;
            while (TransformJoin.Read() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), TransformJoin["LookupValue"]);
                else
                    Assert.Equal(null, TransformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }
    }
}
