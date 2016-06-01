using dexih.core;
using dexih.functions;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;
using System.Reflection;

namespace dexih_unit_tests
{
    // This project can output the Class library as a NuGet Package.
    // To enable this option, right-click on the project and select the Properties menu item. In the Build tab select "Produce outputs on build".
    public class test_functions
    {
        [Fact]
        public void Parameter_Tests()
        {
            //test boolean
            dexih.functions.Parameter parameter = new dexih.functions.Parameter("test", ETypeCode.Boolean);
            ReturnValue result;

            result = parameter.SetValue("abc");
            Assert.True(result.Success == false, "Should return error when setting bool on string type");
            result = parameter.SetValue("true");
            Assert.True(result.Success == true, "Should not return error message setting true to a bool type");


            //test number
            parameter.DataType = ETypeCode.Int32;
            result = parameter.SetValue("abc");
            Assert.True(result.Success == false, "Should return error when setting numeric on string type");
            result = parameter.SetValue("123");
            Assert.True(result.Success == true, "Should not return error message setting number to a string type");

            //test date
            parameter.DataType = ETypeCode.DateTime;
            result = parameter.SetValue("32 january 2015");
            Assert.True(result.Success == false, "Should return error when setting invalid date");
            result = parameter.SetValue("31 january 2015");
            Assert.True(result.Success == true, "Should not return error message setting a valid date");

            //test string
            parameter.DataType = ETypeCode.String;
            result = parameter.SetValue("abc");
            Assert.True(result.Success == true, "Should not return error message setting string");

            //test parameter converts type
            parameter.DataType = ETypeCode.Int32;
            result = parameter.SetValue("123");
            Assert.Equal("System.Int32", parameter.Value.GetType().ToString());
        }

        [Fact]
        public void Aggregate_Tests()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            Function sum = StandardFunctions.GetFromName("Sum").GetFunction();
            Function average = StandardFunctions.GetFromName("Average").GetFunction();
            Function median = StandardFunctions.GetFromName("Median").GetFunction();
            Function stddev = StandardFunctions.GetFromName("Standard Deviation").GetFunction();
            Function var = StandardFunctions.GetFromName("Variance").GetFunction();
            Function min = StandardFunctions.GetFromName("Minimum").GetFunction();
            Function max = StandardFunctions.GetFromName("Maximum").GetFunction();
            Function count = StandardFunctions.GetFromName("Count").GetFunction();
            Function countdistinct = StandardFunctions.GetFromName("Count Distinct").GetFunction();
            Function concat = StandardFunctions.GetFromName("Concatenate Aggregate").GetFunction();

            for (int a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (int i = 1; i <= 10; i++)
                {
                    Assert.True(sum.RunFunction(new object[] { i }).Success, "sum Failed");
                    Assert.True(average.RunFunction(new object[] { i }).Success, "average Failed");
                    Assert.True(median.RunFunction(new object[] { i }).Success, "median Failed");
                    Assert.True(stddev.RunFunction(new object[] { i }).Success, "stddev Failed");
                    Assert.True(var.RunFunction(new object[] { i }).Success, "var Failed");
                    Assert.True(min.RunFunction(new object[] { i }).Success, "min Failed");
                    Assert.True(max.RunFunction(new object[] { i }).Success, "max Failed");
                    Assert.True(count.RunFunction(new object[] { }).Success, "count Failed");
                    Assert.True(countdistinct.RunFunction(new object[] { i }).Success, "countdistinct Failed");
                    Assert.True(concat.RunFunction(new object[] { i, "," }).Success, "Concat Failed");
                }

                Assert.True((double)55 == (double)sum.ReturnValue().Value, "Sum function failed");
                Assert.True((Double)5.5 == (double)average.ReturnValue().Value, "Average function failed");
                Assert.True((Double)5.5 == (double)median.ReturnValue().Value, "Median function failed");
                Assert.True((Double)2.8723 == (double)Math.Round((Double)stddev.ReturnValue().Value, 4), "Median function failed");
                Assert.True((Double)8.25 == (double)Math.Round((Double)var.ReturnValue().Value, 2), "Variance function failed");
                Assert.True((Double)1 == (double)min.ReturnValue().Value, "Minimum function failed");
                Assert.True((Double)10 == (double)max.ReturnValue().Value, "Maximum function failed");
                Assert.True((Int32)10 == (Int32)count.ReturnValue().Value, "Count function failed");
                Assert.True("1,2,3,4,5,6,7,8,9,10" == (string)concat.ReturnValue().Value, "ContactAgg function failed");

                countdistinct.RunFunction(new object[] { 5 }); //add one more value to prove countdistinct works
                Assert.True(10 == (int)countdistinct.ReturnValue().Value, "Count distinct function failed");

                sum.Reset();
                average.Reset();
                min.Reset();
                max.Reset();
                count.Reset();
                countdistinct.Reset();
                concat.Reset();
            }
        }

        [Fact]
        public void Condition_Tests()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            Assert.True((bool)StandardFunctions.GetFromName("Less Than").GetFunction().RunFunction(new object[] { 5, 10 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFromName("Less Than/Equal").GetFunction().RunFunction(new object[] { 5, 5 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFromName("Greater Than").GetFunction().RunFunction(new object[] { 10, 5 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFromName("Greater Than/Equal").GetFunction().RunFunction(new object[] { 5, 5 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFromName("Equals").GetFunction().RunFunction(new object[] { 5, 5 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFromName("Less Than").GetFunction().RunFunction(new object[] { 10, 5 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFromName("Less Than/Equal").GetFunction().RunFunction(new object[] { 10, 9 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFromName("Greater Than").GetFunction().RunFunction(new object[] { 5, 10 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFromName("Greater Than/Equal").GetFunction().RunFunction(new object[] { 5, 6 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFromName("Equals").GetFunction().RunFunction(new object[] { 5, 5.1 }).Value, "Less Than failed");
        }

        [Fact]
        public void CustomFunction()
        {
            Function custom = new Function("CustomColumn", false, "Test", "return value1 + value2.ToString();", null, ETypeCode.String,
                new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("value1", ETypeCode.String, false),
                    new dexih.functions.Parameter("value2", ETypeCode.Int32, false),
                }, null);
            Assert.True(custom.CreateFunctionMethod().Success == true, "Compile errors not exected");
            Assert.True("abc123" == (string)custom.RunFunction(new object[] { "abc", "123" }).Value, "Run should pass");
            Assert.True(false == (bool)custom.RunFunction(new object[] { "123", "abc" }).Success, "Run should fail due to non-int parameter");

            for (int i = 0; i < 1000; i++)
                Assert.True("abc123" == (string)custom.RunFunction(new object[] { "abc", "123" }).Value, "Run many times didn't work");
        }

        [Fact]
        public void StandardFunctions()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            Assert.True(StandardFunctions.Count > 0, "Functions didn't load correctly");
            Function Function = StandardFunctions.GetFromName("Substring").GetFunction();
            Assert.Equal("b", Function.RunFunction(new object[] { "abc", 1, 1 }).Value);
            Assert.Equal("", Function.RunFunction(new object[] { "abc", 10, 10 }).Value);
            Assert.Equal("bc", Function.RunFunction(new object[] { "abc", 1, 10 }).Value);

        }

        [Fact]
        public void SimpleFunctionTest()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            Assert.Equal("test1test2test3", (string)StandardFunctions.GetFromName("Concatenate").GetFunction().RunFunction(new object[] { "test1", "test2", "test3" }).Value);
            Assert.Equal(2, (Int32)StandardFunctions.GetFromName("Index Of").GetFunction().RunFunction(new object[] { "test string", "s" }).Value);
            Assert.Equal("test new string", (String)StandardFunctions.GetFromName("Insert").GetFunction().RunFunction(new object[] { "test string", 5, "new " }).Value);
            Assert.Equal("test1,test2,test3", (String)StandardFunctions.GetFromName("Join").GetFunction().RunFunction(new object[] { ",", "test1", "test2", "test3" }).Value);
            Assert.Equal("---test", (String)StandardFunctions.GetFromName("Pad Left").GetFunction().RunFunction(new object[] { "test", 7, "-" }).Value);
            Assert.Equal("test---", (String)StandardFunctions.GetFromName("Pad Right").GetFunction().RunFunction(new object[] { "test", 7, "-" }).Value);
            Assert.Equal("tng", (String)StandardFunctions.GetFromName("Remove").GetFunction().RunFunction(new object[] { "testing", 1, 4 }).Value);
            Assert.Equal("straas taat", (String)StandardFunctions.GetFromName("Replace").GetFunction().RunFunction(new object[] { "stress test", "es", "aa" }).Value);
            Assert.Equal(3, (Int32)StandardFunctions.GetFromName("Split").GetFunction().RunFunction(new object[] { "test1,test2,test3", ",", 3 }).Value);
            Assert.Equal("esti", (String)StandardFunctions.GetFromName("Substring").GetFunction().RunFunction(new object[] { "testing", 1, 4 }).Value);
            Assert.Equal("  test1 ", (String)StandardFunctions.GetFromName("To Lowercase").GetFunction().RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("  TEST1 ", (String)StandardFunctions.GetFromName("To Uppercase").GetFunction().RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("tEsT1", (String)StandardFunctions.GetFromName("Trim").GetFunction().RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("  tEsT1", (String)StandardFunctions.GetFromName("Trim End").GetFunction().RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("tEsT1 ", (String)StandardFunctions.GetFromName("Trim Start").GetFunction().RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal(4, (Int32)StandardFunctions.GetFromName("Length").GetFunction().RunFunction(new object[] { "test" }).Value);
            Assert.Equal(3, (Int32)StandardFunctions.GetFromName("WordCount").GetFunction().RunFunction(new object[] { "word1  word2 word3" }).Value);
            Assert.Equal("word3", (String)StandardFunctions.GetFromName("WordExtract").GetFunction().RunFunction(new object[] { "word1  word2 word3", 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Less Than").GetFunction().RunFunction(new object[] { 1, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Less Than/Equal").GetFunction().RunFunction(new object[] { 1, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Greater Than").GetFunction().RunFunction(new object[] { 2, 1 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Greater Than/Equal").GetFunction().RunFunction(new object[] { 2, 1 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Equals").GetFunction().RunFunction(new object[] { 2, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Number").GetFunction().RunFunction(new object[] { 123 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Date").GetFunction().RunFunction(new object[] { "1 Jan 2000" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Null").GetFunction().RunFunction(new object[] { null }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Between").GetFunction().RunFunction(new object[] { 2, 1, 3 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Between Inclusive").GetFunction().RunFunction(new object[] { 2, 1, 3 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Regular Expression").GetFunction().RunFunction(new object[] { "abbbb", "ab*" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Contains").GetFunction().RunFunction(new object[] { "testing", "est" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Ends With").GetFunction().RunFunction(new object[] { "testing", "ing" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Starts With").GetFunction().RunFunction(new object[] { "testing", "tes" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Uppercase").GetFunction().RunFunction(new object[] { "TEST", true }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Lowercase").GetFunction().RunFunction(new object[] { "test", true }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Letters").GetFunction().RunFunction(new object[] { "test" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("IsLetter/Digit").GetFunction().RunFunction(new object[] { "test123" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Pattern").GetFunction().RunFunction(new object[] { "Hello12", "Aaaaa99" }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFromName("Add Days").GetFunction().RunFunction(new object[] { "2015-09-24", 1 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFromName("Add Hours").GetFunction().RunFunction(new object[] { "2015-09-24", 24 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFromName("Add Milliseconds").GetFunction().RunFunction(new object[] { "2015-09-24", 86400000 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFromName("Add Minutes").GetFunction().RunFunction(new object[] { "2015-09-24", 1440 }).Value);
            Assert.Equal(DateTime.Parse("24 Oct 2015"), (DateTime)StandardFunctions.GetFromName("Add Months").GetFunction().RunFunction(new object[] { "2015-09-24", 1 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFromName("Add Seconds").GetFunction().RunFunction(new object[] { "2015-09-24", 86400 }).Value);
            Assert.Equal(DateTime.Parse("24 Sep 2016"), (DateTime)StandardFunctions.GetFromName("AddYears").GetFunction().RunFunction(new object[] { "2015-09-24", 1 }).Value);
            Assert.Equal(30, (Int32)StandardFunctions.GetFromName("DaysInMonth").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(false, (Boolean)StandardFunctions.GetFromName("Is Daylight Saving").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(false, (Boolean)StandardFunctions.GetFromName("Is Leap Year").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(false, (Boolean)StandardFunctions.GetFromName("Is Weekend").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("Is Weekday").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(24, (Int32)StandardFunctions.GetFromName("Day of the Month").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("Thursday", (String)StandardFunctions.GetFromName("Day of the Week Name").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(4, (Int32)StandardFunctions.GetFromName("Day of the Week Number").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(39, (Int32)StandardFunctions.GetFromName("Week of the Year").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(267, (Int32)StandardFunctions.GetFromName("Day of the Year").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(9, (Int32)StandardFunctions.GetFromName("Month ").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("Sep", (String)StandardFunctions.GetFromName("ShortMonth").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("September", (String)StandardFunctions.GetFromName("LongMonth").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(2015, (Int32)StandardFunctions.GetFromName("Year").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("Thursday, 24 September 2015", (String)StandardFunctions.GetFromName("To Long Date String").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("12:00:00 AM", (String)StandardFunctions.GetFromName("To Long Time String").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("24/09/2015", (String)StandardFunctions.GetFromName("To Short Date String").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("12:00 AM", (String)StandardFunctions.GetFromName("To Short Time String").GetFunction().RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("24 Sep 2015", (String)StandardFunctions.GetFromName("Date To String").GetFunction().RunFunction(new object[] { "2015-09-24", "dd MMM yyyy" }).Value);
            Assert.Equal(3, (Double)StandardFunctions.GetFromName("Abs").GetFunction().RunFunction(new object[] { -3 }).Value);
            Assert.Equal(1, (Int32)StandardFunctions.GetFromName("DivRem").GetFunction().RunFunction(new object[] { 6, 4 }).Value);
            Assert.Equal(36, (Double)StandardFunctions.GetFromName("Pow").GetFunction().RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(6, (Double)StandardFunctions.GetFromName("Round").GetFunction().RunFunction(new object[] { 6.5 }).Value);
            Assert.Equal(-1, (Double)StandardFunctions.GetFromName("Sign").GetFunction().RunFunction(new object[] { -4 }).Value);
            Assert.Equal(3, (Double)StandardFunctions.GetFromName("Sqrt").GetFunction().RunFunction(new object[] { 9 }).Value);
            Assert.Equal(6, (Double)StandardFunctions.GetFromName("Truncate").GetFunction().RunFunction(new object[] { 6.4 }).Value);
            Assert.Equal(3, (Decimal)StandardFunctions.GetFromName("Add").GetFunction().RunFunction(new object[] { 1, 2 }).Value);
            Assert.Equal(7, (Decimal)StandardFunctions.GetFromName("Ceiling").GetFunction().RunFunction(new object[] { 6.4 }).Value);
            Assert.Equal(3, (Decimal)StandardFunctions.GetFromName("Divide").GetFunction().RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(6, (Decimal)StandardFunctions.GetFromName("Floor").GetFunction().RunFunction(new object[] { 6.4 }).Value);
            Assert.Equal(12, (Decimal)StandardFunctions.GetFromName("Multiply").GetFunction().RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(-6, (Decimal)StandardFunctions.GetFromName("Negate").GetFunction().RunFunction(new object[] { 6 }).Value);
            Assert.Equal(2, (Decimal)StandardFunctions.GetFromName("Remainder").GetFunction().RunFunction(new object[] { 6, 4 }).Value);
            Assert.Equal(4, (Decimal)StandardFunctions.GetFromName("Subtract").GetFunction().RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFromName("IsIn").GetFunction().RunFunction(new object[] { "test2", "test1", "test2", "test3" }).Value);
        }

        [Fact]
        public void Function_XPathValue()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Get a rows that exists.
            Function XPathValue = StandardFunctions.GetFromName("XPathValues").GetFunction();
            object[] Param = new object[] { "<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>", "//row[1]", "//row[2]", "//row[3]" };
            Assert.Equal(true, XPathValue.RunFunction(Param, new string[] { "value1", "value2", "value3" }).Value);
            Assert.Equal("0", (string)XPathValue.Outputs[0].Value);
            Assert.Equal("1", (string)XPathValue.Outputs[1].Value);
            Assert.Equal("2", (string)XPathValue.Outputs[2].Value);
        }

        [Fact]
        public void Function_JSONValue()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Get a rows that exists.
            Function JSONValues = StandardFunctions.GetFromName("JsonValues").GetFunction();
            object[] Param = new object[] { "{ 'value1': '1', 'value2' : '2', 'value3': '3', 'array' : {'v1' : '1', 'v2' : '2'} }", "value1", "value2", "value3", "array", "badvalue" };
            Assert.Equal(false, JSONValues.RunFunction(Param, new string[] { "value1", "value2", "value3", "array", "badvalue" }).Value);
            Assert.Equal("1", (string)JSONValues.Outputs[0].Value);
            Assert.Equal("2", (string)JSONValues.Outputs[1].Value);
            Assert.Equal("3", (string)JSONValues.Outputs[2].Value);
            Assert.Equal(null, (string)JSONValues.Outputs[4].Value);

            //get the sub Json string, and run another parse over this.
            string MoreValues = (string)JSONValues.Outputs[3].Value;
            Param = new object[] { MoreValues, "v1", "v2" };
            JSONValues = StandardFunctions.GetFromName("JsonValues").GetFunction();
            Assert.Equal(true, JSONValues.RunFunction(Param, new string[] { "v1", "v2" }).Value);
            Assert.Equal("1", (string)JSONValues.Outputs[0].Value);
            Assert.Equal("2", (string)JSONValues.Outputs[1].Value);

        }


        [Fact]
        public void RowFunctions_GenerateSequence()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Use a for loop to similate gen sequence.
            Function Sequence = StandardFunctions.GetFromName("Generate Sequence").GetFunction();
            object[] Param = new object[] { 0, 10, 2 };
            for (int i = 0; i <= 10; i += 2)
            {
                Assert.Equal(true, Sequence.RunFunction(Param).Value);
                Assert.Equal(i, (int)Sequence.Outputs[0].Value);
            }
            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)Sequence.RunFunction(Param).Value);
        }

        [Fact]
        public void RowFunctions_SplitColumnToRows()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Use a for loop to similate gen sequence.
            Function SplitColumnToRows = StandardFunctions.GetFromName("SplitColumnToRows").GetFunction();
            object[] Param = new object[] { "|", "|value2|value3||value5||", 6 };
            string[] Compare = new string[] { "", "value2", "value3", "", "value5", "", "" };
            for (int i = 0; i <= 6; i++)
            {
                Assert.Equal(true, SplitColumnToRows.RunFunction(Param).Value);
                Assert.Equal(Compare[i], (string)SplitColumnToRows.Outputs[0].Value);
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)SplitColumnToRows.RunFunction(Param).Value);
        }

        [Fact]
        public void RowFunctions_XPathNodesToRows()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Use a for loop to similate gen sequence.
            Function XPathNodesToRows = StandardFunctions.GetFromName("XPathNodesToRows").GetFunction();
            object[] Param = new object[] { "<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>", "//row", 5 };
            for (int i = 0; i <= 5; i++)
            {
                Assert.Equal(true, XPathNodesToRows.RunFunction(Param).Value);
                Assert.Equal(i.ToString(), (string)XPathNodesToRows.Outputs[0].Value);
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)XPathNodesToRows.RunFunction(Param).Value);
        }

        [Fact]
        public void RowFunctions_JsonElementsToRows()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Use a for loop to similate gen sequence.
            Function JSONElementsToRows = StandardFunctions.GetFromName("JsonElementsToRows").GetFunction();
            object[] Param = new object[] { "{'results' : [{'value1' : 'r1v1', 'value2' : 'r1v2'}, {'value1' : 'r2v1', 'value2' : 'r2v2'}]} ", "results[*]", 2 };
            for (int i = 1; i <= 2; i++)
            {
                Assert.Equal(true, JSONElementsToRows.RunFunction(Param).Value);
                string JsonResult = (string)JSONElementsToRows.Outputs[0].Value;
                var results = JObject.Parse(JsonResult);
                Assert.Equal("r" + i.ToString() + "v1", results.SelectToken("value1").ToString());
                Assert.Equal("r" + i.ToString() + "v2", results.SelectToken("value2").ToString());
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)JSONElementsToRows.RunFunction(Param).Value);
        }

        [Fact]
        public void EncryptFunctions()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Use a for loop to similate gen sequence.
            Function Encrypt = StandardFunctions.GetFromName("Encrypt").GetFunction();
            Function Decrypt = StandardFunctions.GetFromName("Decrypt").GetFunction();

            string TestValue = "encrypt this!!!";
            string EncryptString = (string)Encrypt.RunFunction(new object[] { TestValue, "key123" }).Value;
            //Assert.Equal("aPOUdzPqb0rOgoxKJuU74Q==", EncryptString);
            string DecryptString = (string)Decrypt.RunFunction(new object[] { EncryptString, "key123" }).Value;
            Assert.Equal(TestValue, DecryptString);
        }

        [Fact]
        public void HashFunctions()
        {
            StandardFunctionReferences StandardFunctions = new StandardFunctionReferences();
            StandardFunctions.Load(Helpers.RepositoryConnection()).Wait();

            //Use a for loop to similate gen sequence.
            Function CreateHash = StandardFunctions.GetFromName("CreateHash").GetFunction();
            Function ValidateHash = StandardFunctions.GetFromName("ValidateHash").GetFunction();

            string TestValue = "hash this!!!";
            string HashString1 = (string)CreateHash.RunFunction(new object[] { TestValue }).Value;
            string HashString2 = (string)CreateHash.RunFunction(new object[] { TestValue }).Value;
            Assert.NotEqual(HashString1, HashString2); //two hashes in a row should not be equal as they are salted;

            string HashString3 = (string)CreateHash.RunFunction(new object[] { "hash this!!! 2" }).Value;

            Assert.True((bool)ValidateHash.RunFunction(new object[] { TestValue, HashString1 }).Value);
            Assert.True((bool)ValidateHash.RunFunction(new object[] { TestValue, HashString2 }).Value);

            Assert.False((bool)ValidateHash.RunFunction(new object[] { TestValue, HashString3 }).Value);
            Assert.False((bool)ValidateHash.RunFunction(new object[] { TestValue + "1", HashString1 }).Value);
        }

        [Fact]
        public void FunctionMethod()
        {
            //create a custom function
            Function function1 = new Function(new Func<int, int, int>((i, j) => i + j), new string[] { "value1", "value2" }, "Add", null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }).Value == 8);

            Function function2 = new Function(typeof(StandardFunctions), "Add", new string[] { "value1", "value2" }, "Add", null);
            Assert.True((decimal)function2.RunFunction(new object[] { 6, 2 }).Value == 8);

            Function function3 = new Function(
                "CustomColumn", false, "Test", "return value1 + value2;", null, ETypeCode.Int32,
                new dexih.functions.Parameter[] { new dexih.functions.Parameter("value1", ETypeCode.Int32, false), new dexih.functions.Parameter("value2", ETypeCode.Int32, false)}, 
                null);
            Assert.True((Int32)function3.RunFunction(new object[] { 6, 2 }).Value == 8);

        }

    }
}
