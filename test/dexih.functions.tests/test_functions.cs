using dexih.functions;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;
using System.Reflection;

namespace dexih.unittests
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
            
            Function sum = StandardFunctions.GetFunctionReference("Sum");
            Function average = StandardFunctions.GetFunctionReference("Average");
            Function median = StandardFunctions.GetFunctionReference("Median");
            Function stddev = StandardFunctions.GetFunctionReference("StdDev");
            Function var = StandardFunctions.GetFunctionReference("Variance");
            Function min = StandardFunctions.GetFunctionReference("Min");
            Function max = StandardFunctions.GetFunctionReference("Max");
            Function count = StandardFunctions.GetFunctionReference("Count");
            Function countdistinct = StandardFunctions.GetFunctionReference("CountDistinct");
            Function concat = StandardFunctions.GetFunctionReference("ConcatAgg");

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
            Assert.True((bool)StandardFunctions.GetFunctionReference("LessThan").RunFunction(new object[] { 5, 10 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFunctionReference("LessThanEqual").RunFunction(new object[] { 5, 5 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFunctionReference("GreaterThan").RunFunction(new object[] { 10, 5 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFunctionReference("GreaterThanEqual").RunFunction(new object[] { 5, 5 }).Value, "Less Than failed");
            Assert.True((bool)StandardFunctions.GetFunctionReference("IsEqual").RunFunction(new object[] { 5, 5 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFunctionReference("LessThan").RunFunction(new object[] { 10, 5 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFunctionReference("LessThanEqual").RunFunction(new object[] { 10, 9 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFunctionReference("GreaterThan").RunFunction(new object[] { 5, 10 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFunctionReference("GreaterThanEqual").RunFunction(new object[] { 5, 6 }).Value, "Less Than failed");
            Assert.False((bool)StandardFunctions.GetFunctionReference("IsEqual").RunFunction(new object[] { 5, 5.1 }).Value, "Less Than failed");
        }

        //[Fact]
        //public void CustomFunction()
        //{
        //    Function custom = new Function("CustomColumn", false, "Test", "return value1 + value2.ToString();", null, ETypeCode.String,
        //        new dexih.functions.Parameter[] {
        //            new dexih.functions.Parameter("value1", ETypeCode.String, false),
        //            new dexih.functions.Parameter("value2", ETypeCode.Int32, false),
        //        }, null);
        //    Assert.True(custom.CreateFunctionMethod().Success == true, "Compile errors not exected");
        //    Assert.True("abc123" == (string)custom.RunFunction(new object[] { "abc", "123" }).Value, "Run should pass");
        //    Assert.True(false == (bool)custom.RunFunction(new object[] { "123", "abc" }).Success, "Run should fail due to non-int parameter");

        //    for (int i = 0; i < 1000; i++)
        //        Assert.True("abc123" == (string)custom.RunFunction(new object[] { "abc", "123" }).Value, "Run many times didn't work");
        //}

        [Fact]
        public void SimpleFunctionTest()
        {
            
            Assert.Equal("test1test2test3", (string)StandardFunctions.GetFunctionReference("Concat").RunFunction(new object[] { "test1", "test2", "test3" }).Value);
            Assert.Equal(2, (Int32)StandardFunctions.GetFunctionReference("IndexOf").RunFunction(new object[] { "test string", "s" }).Value);
            Assert.Equal("test new string", (String)StandardFunctions.GetFunctionReference("Insert").RunFunction(new object[] { "test string", 5, "new " }).Value);
            Assert.Equal("test1,test2,test3", (String)StandardFunctions.GetFunctionReference("Join").RunFunction(new object[] { ",", "test1", "test2", "test3" }).Value);
            Assert.Equal("---test", (String)StandardFunctions.GetFunctionReference("PadLeft").RunFunction(new object[] { "test", 7, "-" }).Value);
            Assert.Equal("test---", (String)StandardFunctions.GetFunctionReference("PadRight").RunFunction(new object[] { "test", 7, "-" }).Value);
            Assert.Equal("tng", (String)StandardFunctions.GetFunctionReference("Remove").RunFunction(new object[] { "testing", 1, 4 }).Value);
            Assert.Equal("straas taat", (String)StandardFunctions.GetFunctionReference("Replace").RunFunction(new object[] { "stress test", "es", "aa" }).Value);
            Assert.Equal(3, (Int32)StandardFunctions.GetFunctionReference("Split").RunFunction(new object[] { "test1,test2,test3", ",", 3 }).Value);
            Assert.Equal("esti", (String)StandardFunctions.GetFunctionReference("Substring").RunFunction(new object[] { "testing", 1, 4 }).Value);
            Assert.Equal("  test1 ", (String)StandardFunctions.GetFunctionReference("ToLower").RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("  TEST1 ", (String)StandardFunctions.GetFunctionReference("ToUpper").RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("tEsT1", (String)StandardFunctions.GetFunctionReference("Trim").RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("  tEsT1", (String)StandardFunctions.GetFunctionReference("TrimEnd").RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal("tEsT1 ", (String)StandardFunctions.GetFunctionReference("TrimStart").RunFunction(new object[] { "  tEsT1 " }).Value);
            Assert.Equal(4, (Int32)StandardFunctions.GetFunctionReference("Length").RunFunction(new object[] { "test" }).Value);
            Assert.Equal(3, (Int32)StandardFunctions.GetFunctionReference("WordCount").RunFunction(new object[] { "word1  word2 word3" }).Value);
            Assert.Equal("word3", (String)StandardFunctions.GetFunctionReference("WordExtract").RunFunction(new object[] { "word1  word2 word3", 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("LessThan").RunFunction(new object[] { 1, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("LessThanEqual").RunFunction(new object[] { 1, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("GreaterThan").RunFunction(new object[] { 2, 1 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("GreaterThanEqual").RunFunction(new object[] { 2, 1 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsEqual").RunFunction(new object[] { 2, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsNumber").RunFunction(new object[] { 123 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsDate").RunFunction(new object[] { "1 Jan 2000" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsNull").RunFunction(new object[] { null }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsBetween").RunFunction(new object[] { 2, 1, 3 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsBetweenInclusive").RunFunction(new object[] { 2, 1, 3 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("RegexMatch").RunFunction(new object[] { "abbbb", "ab*" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("Contains").RunFunction(new object[] { "testing", "est" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("EndsWith").RunFunction(new object[] { "testing", "ing" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("StartsWith").RunFunction(new object[] { "testing", "tes" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsUpper").RunFunction(new object[] { "TEST", true }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsLower").RunFunction(new object[] { "test", true }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsAlpha").RunFunction(new object[] { "test" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsAlphaNumeric").RunFunction(new object[] { "test123" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsPattern").RunFunction(new object[] { "Hello12", "Aaaaa99" }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFunctionReference("AddDays").RunFunction(new object[] { "2015-09-24", 1 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFunctionReference("AddHours").RunFunction(new object[] { "2015-09-24", 24 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFunctionReference("AddMilliseconds").RunFunction(new object[] { "2015-09-24", 86400000 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFunctionReference("AddMinutes").RunFunction(new object[] { "2015-09-24", 1440 }).Value);
            Assert.Equal(DateTime.Parse("24 Oct 2015"), (DateTime)StandardFunctions.GetFunctionReference("AddMonths").RunFunction(new object[] { "2015-09-24", 1 }).Value);
            Assert.Equal(DateTime.Parse("25 Sep 2015"), (DateTime)StandardFunctions.GetFunctionReference("AddSeconds").RunFunction(new object[] { "2015-09-24", 86400 }).Value);
            Assert.Equal(DateTime.Parse("24 Sep 2016"), (DateTime)StandardFunctions.GetFunctionReference("AddYears").RunFunction(new object[] { "2015-09-24", 1 }).Value);
            Assert.Equal(30, (Int32)StandardFunctions.GetFunctionReference("DaysInMonth").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(false, (Boolean)StandardFunctions.GetFunctionReference("IsDaylightSavingTime").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(false, (Boolean)StandardFunctions.GetFunctionReference("IsLeapYear").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(false, (Boolean)StandardFunctions.GetFunctionReference("IsWeekend").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsWeekDay").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(24, (Int32)StandardFunctions.GetFunctionReference("DayOfMonth").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("Thursday", (String)StandardFunctions.GetFunctionReference("DayOfWeekName").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(4, (Int32)StandardFunctions.GetFunctionReference("DayOfWeekNumber").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(39, (Int32)StandardFunctions.GetFunctionReference("WeekOfYear").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(267, (Int32)StandardFunctions.GetFunctionReference("DayOfYear").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(9, (Int32)StandardFunctions.GetFunctionReference("Month").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("Sep", (String)StandardFunctions.GetFunctionReference("ShortMonth").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("September", (String)StandardFunctions.GetFunctionReference("LongMonth").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal(2015, (Int32)StandardFunctions.GetFunctionReference("Year").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("Thursday, 24 September 2015", (String)StandardFunctions.GetFunctionReference("ToLongDateString").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("12:00:00 AM", (String)StandardFunctions.GetFunctionReference("ToLongTimeString").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("24/09/2015", (String)StandardFunctions.GetFunctionReference("ToShortDateString").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("12:00 AM", (String)StandardFunctions.GetFunctionReference("ToShortTimeString").RunFunction(new object[] { "2015-09-24" }).Value);
            Assert.Equal("24 Sep 2015", (String)StandardFunctions.GetFunctionReference("DateToString").RunFunction(new object[] { "2015-09-24", "dd MMM yyyy" }).Value);
            Assert.Equal(3, (Double)StandardFunctions.GetFunctionReference("Abs").RunFunction(new object[] { -3 }).Value);
            Assert.Equal(1, (Int32)StandardFunctions.GetFunctionReference("DivRem").RunFunction(new object[] { 6, 4 }).Value);
            Assert.Equal(36, (Double)StandardFunctions.GetFunctionReference("Pow").RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(6, (Double)StandardFunctions.GetFunctionReference("Round").RunFunction(new object[] { 6.5 }).Value);
            Assert.Equal(-1, (Double)StandardFunctions.GetFunctionReference("Sign").RunFunction(new object[] { -4 }).Value);
            Assert.Equal(3, (Double)StandardFunctions.GetFunctionReference("Sqrt").RunFunction(new object[] { 9 }).Value);
            Assert.Equal(6, (Double)StandardFunctions.GetFunctionReference("Truncate").RunFunction(new object[] { 6.4 }).Value);
            Assert.Equal(3, (Decimal)StandardFunctions.GetFunctionReference("Add").RunFunction(new object[] { 1, 2 }).Value);
            Assert.Equal(7, (Decimal)StandardFunctions.GetFunctionReference("Ceiling").RunFunction(new object[] { 6.4 }).Value);
            Assert.Equal(3, (Decimal)StandardFunctions.GetFunctionReference("Divide").RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(6, (Decimal)StandardFunctions.GetFunctionReference("Floor").RunFunction(new object[] { 6.4 }).Value);
            Assert.Equal(12, (Decimal)StandardFunctions.GetFunctionReference("Multiply").RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(-6, (Decimal)StandardFunctions.GetFunctionReference("Negate").RunFunction(new object[] { 6 }).Value);
            Assert.Equal(2, (Decimal)StandardFunctions.GetFunctionReference("Remainder").RunFunction(new object[] { 6, 4 }).Value);
            Assert.Equal(4, (Decimal)StandardFunctions.GetFunctionReference("Subtract").RunFunction(new object[] { 6, 2 }).Value);
            Assert.Equal(true, (Boolean)StandardFunctions.GetFunctionReference("IsIn").RunFunction(new object[] { "test2", "test1", "test2", "test3" }).Value);
        }

        [Fact]
        public void Function_XPathValue()
        {
            //Get a rows that exists.
            Function XPathValue = StandardFunctions.GetFunctionReference("XPathValues");
            object[] Param = new object[] { "<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>", "//row[1]", "//row[2]", "//row[3]" };
            Assert.Equal(true, XPathValue.RunFunction(Param, new string[] { "value1", "value2", "value3" }).Value);
            Assert.Equal("0", (string)XPathValue.Outputs[0].Value);
            Assert.Equal("1", (string)XPathValue.Outputs[1].Value);
            Assert.Equal("2", (string)XPathValue.Outputs[2].Value);
        }

        [Fact]
        public void Function_JSONValue()
        {
            //Get a rows that exists.
            Function JSONValues = StandardFunctions.GetFunctionReference("JsonValues");
            object[] Param = new object[] { "{ 'value1': '1', 'value2' : '2', 'value3': '3', 'array' : {'v1' : '1', 'v2' : '2'} }", "value1", "value2", "value3", "array", "badvalue" };
            Assert.Equal(false, JSONValues.RunFunction(Param, new string[] { "value1", "value2", "value3", "array", "badvalue" }).Value);
            Assert.Equal("1", (string)JSONValues.Outputs[0].Value);
            Assert.Equal("2", (string)JSONValues.Outputs[1].Value);
            Assert.Equal("3", (string)JSONValues.Outputs[2].Value);
            Assert.Equal(null, (string)JSONValues.Outputs[4].Value);

            //get the sub Json string, and run another parse over this.
            string MoreValues = (string)JSONValues.Outputs[3].Value;
            Param = new object[] { MoreValues, "v1", "v2" };
            JSONValues = StandardFunctions.GetFunctionReference("JsonValues");
            Assert.Equal(true, JSONValues.RunFunction(Param, new string[] { "v1", "v2" }).Value);
            Assert.Equal("1", (string)JSONValues.Outputs[0].Value);
            Assert.Equal("2", (string)JSONValues.Outputs[1].Value);

        }


        [Fact]
        public void RowFunctions_GenerateSequence()
        {
            //Use a for loop to similate gen sequence.
            Function Sequence = StandardFunctions.GetFunctionReference("GenerateSequence");
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
            //Use a for loop to similate gen sequence.
            Function SplitColumnToRows = StandardFunctions.GetFunctionReference("SplitColumnToRows");
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
            //Use a for loop to similate gen sequence.
            Function XPathNodesToRows = StandardFunctions.GetFunctionReference("XPathNodesToRows");
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
            //Use a for loop to similate gen sequence.
            Function JSONElementsToRows = StandardFunctions.GetFunctionReference("JsonElementsToRows");
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
            //Use a for loop to similate gen sequence.
            Function Encrypt = StandardFunctions.GetFunctionReference("Encrypt");
            Function Decrypt = StandardFunctions.GetFunctionReference("Decrypt");

            string TestValue = "encrypt this!!!";
            string EncryptString = (string)Encrypt.RunFunction(new object[] { TestValue, "key123" }).Value;
            //Assert.Equal("aPOUdzPqb0rOgoxKJuU74Q==", EncryptString);
            string DecryptString = (string)Decrypt.RunFunction(new object[] { EncryptString, "key123" }).Value;
            Assert.Equal(TestValue, DecryptString);
        }

        [Fact]
        public void HashFunctions()
        {
            //Use a for loop to similate gen sequence.
            Function CreateHash = StandardFunctions.GetFunctionReference("CreateSaltedHash");
            Function ValidateHash = StandardFunctions.GetFunctionReference("ValidateSaltedHash");

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

            Function function2 = StandardFunctions.GetFunctionReference("Add", new string[] { "value1", "value2" }, "Add", null);
            Assert.True((decimal)function2.RunFunction(new object[] { 6, 2 }).Value == 8);

            //Function function3 = new Function(
            //    "CustomColumn", false, "Test", "return value1 + value2;", null, ETypeCode.Int32,
            //    new dexih.functions.Parameter[] { new Parameter("value1", ETypeCode.Int32, false), new dexih.functions.Parameter("value2", ETypeCode.Int32, false)}, 
            //    null);
            //Assert.True((int)function3.RunFunction(new object[] { 6, 2 }).Value == 8);

        }

    }
}
