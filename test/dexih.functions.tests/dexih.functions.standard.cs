using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions.tests
{
    public class FunctionStandardFunctions
    {
        private readonly ITestOutputHelper _output;

        public FunctionStandardFunctions(ITestOutputHelper output)
        {
            _output = output;
        }
        
        [Theory]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Concat", new object[] { new [] {"test1", "test2", "test3" }}, "test1test2test3")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "IndexOf", new object[] { "test string", "s" }, 2)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Insert", new object[] { "test string", 5, "new " }, "test new string")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Join", new object[] { ",", new [] {"test1", "test2", "test3" }}, "test1,test2,test3")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "PadLeft", new object[] { "test", 7, "-" }, "---test")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "PadRight", new object[] { "test", 7, "-" }, "test---")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Remove", new object[] { "testing", 1, 4 }, "tng")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Replace", new object[] { "stress test", "es", "aa" }, "straas taat")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Split", new object[] { "test1,test2,test3", ",", 3 }, 3)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Substring", new object[] { "testing", 1, 4 }, "esti")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "ToLower", new object[] { "  tEsT1 " }, "  test1 ")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "ToUpper", new object[] { "  tEsT1 " }, "  TEST1 ")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Trim", new object[] { "  tEsT1 " }, "tEsT1")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "TrimEnd", new object[] { "  tEsT1 " }, "  tEsT1")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "TrimStart", new object[] { "  tEsT1 " }, "tEsT1 ")]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Length", new object[] { "test" }, 4)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "WordCount", new object[] { "word1  word2 word3" }, 3)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "WordExtract", new object[] { "word1  word2 word3", 2 }, "word3")]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "LessThan", new object[] { 1, 2 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "LessThan", new object[] { 2, 1 }, false)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "LessThanEqual", new object[] { 1, 2 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "LessThanEqual", new object[] { 2, 2 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "LessThanEqual", new object[] { 2, 1 }, false)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "GreaterThan", new object[] { 2, 1 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "GreaterThanEqual", new object[] { 2, 1 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsEqual", new object[] { new object[] {2, 2} }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsBooleanEqual", new object[] { new [] { true, true} }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsEqual", new object[] { new object[] {3, 2} }, false)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsNumber", new object[] { "123" }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsNumber", new object[] { "123a" }, false)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsNull", new object[] { null }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsBetween", new object[] { 2, 1, 3 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsBetweenInclusive", new object[] { 2, 1, 3 }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "RegexMatch", new object[] { "abbbb", "ab*" }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "Contains", new object[] { "testing", "est", false }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "EndsWith", new object[] { "testing", "ing", false }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "StartsWith", new object[] { "testing", "tes", false }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsUpper", new object[] { "TEST", true }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsLower", new object[] { "test", true }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsAlpha", new object[] { "test" }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsAlphaNumeric", new object[] { "test123" }, true)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsPattern", new object[] { "Hello12", "Aaaaa99" }, true)]
//        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsDaylightSavingTime", new object[] { "2015-09-24T21:22:48.2698750Z" }, false)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Abs", new object[] { -3 }, (double)3)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "DivRem", new object[] { 6, 4 }, 1)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Pow", new object[] { 6, 2 }, (double)36)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Round", new object[] { 6.5 }, (double)6)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Sign", new object[] { -4 }, (double)-1)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Sqrt", new object[] { 9 }, (double)3)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "Truncate", new object[] { 6.4 }, (double)6)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "IsIn", new object[] { "test2", new object[] {"test1", "test2", "test3"} }, true)]
        [InlineData("dexih.functions.BuiltIn.MapFunctions", "GetDistanceTo", new object[] { -38, -145, -34 ,- 151 }, 699082.1288)] //melbourne to sydney distance
        [InlineData("dexih.functions.BuiltIn.ValidationFunctions", "MaxLength", new object[] { "abcdef", 5 }, false)]
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "RangeIntersect", new object[] { 1, 2, 3, 4 }, false)] //(1,2)(3,4) not intersect
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "RangeIntersect", new object[] { 1, 3, 3, 4 }, false)] //(1,3)(3,4) do intersect
        [InlineData("dexih.functions.BuiltIn.ConditionFunctions", "RangeIntersect", new object[] { 1, 4, 3, 4 }, true)] //(1,4)(3,4) do intersect
        [InlineData("dexih.functions.BuiltIn.CategorizeFunctions", "RangeCategorize", new object[] { 1D, new [] {1D, 5D, 10D}}, true)]
        [InlineData("dexih.functions.BuiltIn.CategorizeFunctions", "RangeCategorize", new object[] { 11D, new [] {1D, 5D, 10D} }, false)]
        [InlineData("dexih.functions.BuiltIn.CategorizeFunctions", "DiscreteRangeCategorize", new object[] { 1L, new [] {1L, 5L, 10L} }, true)]
        [InlineData("dexih.functions.BuiltIn.CategorizeFunctions", "DiscreteRangeCategorize", new object[] { 11L, new [] {1L, 5L, 10L} }, false)]
        [MemberData(nameof(OtherFunctions))]
        public void StandardFunctionTest(string typeName, string methodName, object[] parameters, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();
            transformFunction.OnNull = EErrorAction.Execute;
            var returnValue = transformFunction.Invoke(parameters);

            if (returnValue is double d)
            {
                Assert.Equal(expectedResult, Math.Round(d, 4));
            }
            else
                Assert.Equal(expectedResult, returnValue);
        }

        public static IEnumerable<object[]> OtherFunctions
        {
            get
            {
                var date1 =  DateTime.Parse("2015-09-24");

                return new[]
                {
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "DayOfMonth", new object[] {date1}, 24},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "DaysInMonth", new object[] {date1}, 30},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "DayOfWeekName", new object[] {date1}, "Thursday"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "DayOfWeekNumber", new object[] {date1}, 4},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "WeekOfYear", new object[] {date1}, 39},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "DayOfYear", new object[] {date1}, 267},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Month", new object[] {date1}, 9},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "ShortMonth", new object[] {date1}, "Sep"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "LongMonth", new object[] {date1}, "September"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Year", new object[] {date1}, 2015},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "ToLongDateString", new object[] {date1}, "Thursday, 24 September 2015"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "ToLongTimeString", new object[] {date1}, "12:00:00 AM"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "ToShortDateString", new object[] {date1}, "24/09/2015"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "ToShortTimeString", new object[] {date1}, "12:00 AM"},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "DateToString", new object[] {date1, "dd MMM yyyy"}, "24 Sep 2015"},
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "IsLeapYear", new object[] {date1}, false},
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "IsWeekend", new object[] { date1 }, false},
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "IsWeekDay", new object[] { date1 }, true},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddDays", new object[] { date1, 1 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddHours", new object[] { date1, 24 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddMilliseconds", new object[] { date1, 86400000 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddMinutes", new object[] { date1, 1440 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddMonths", new object[] { date1, 1 }, DateTime.Parse("24 Oct 2015")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddSeconds", new object[] { date1, 86400 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AddYears", new object[] { date1, 1 }, DateTime.Parse("24 Sep 2016")},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AgeInYearsAtDate", new object[] { date1,  DateTime.Parse("2016-09-24") }, 1},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AgeInYearsAtDate", new object[] { date1,  date1 }, 0},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "AgeInYearsAtDate", new object[] { date1,  DateTime.Parse("2017-09-25") }, 2},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Add", new object[] { 1m, new [] {2m} }, 3m},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Ceiling", new object[] { 6.4m }, (Decimal)7 },
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Divide", new object[] { 6m, 2m }, (Decimal)3 },
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Floor", new object[] { 6.4m }, (Decimal)6 },
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Multiply", new object[] { 6m, new [] {2m} }, (Decimal)12 },
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Negate", new object[] { 6m }, (Decimal) (-6)},
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Remainder", new object[] { 6m, 4m }, (Decimal)2 },
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "Subtract", new object[] { 6m, new [] { 2m} }, (Decimal)4 },
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "IsDateTimeEqual", new object[] { new [] { DateTime.Parse("25 Sep 2015"), DateTime.Parse("25 Sep 2015")} }, true },
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "IsBetween", new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("25 Sep 2015"), DateTime.Parse("27 Sep 2015") }, true },
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "IsBetweenInclusive", new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("26 Sep 2015"), DateTime.Parse("27 Sep 2015") }, true },
                    new object[] { "dexih.functions.BuiltIn.MapFunctions", "UnixTimeStampToDate", new object[] { 1518739200 }, new DateTime(2018, 2, 16, 0, 0, 0, 0, System.DateTimeKind.Utc).ToLocalTime() },
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "RangeIntersect", new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("27 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, false },
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "RangeIntersect", new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, false },
                    new object[] { "dexih.functions.BuiltIn.ConditionFunctions", "RangeIntersect", new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("29 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, true },
                };
            }
        }

        [Theory]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Sum", (double)55)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Average", 5.5)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Median", 5.5)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "StdDev", 2.8723)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Variance", 8.25)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Min", 1)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Max", 10)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "First", 1)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "Last", 10)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "CountDistinct", 10)]
        public void AggregateFunctionTest(string typeName, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    transformFunction.Invoke(new object[] { i });
                }

                var aggregateResult = transformFunction.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);

                if(aggregateResult is double)
                {
                    Assert.Equal(expectedResult, Math.Round((double)aggregateResult, 4));
                }
                else
                    Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }

        [Theory]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "CountTrue", 3)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "CountFalse", 1)]
        public void CountTests(string typeName, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();

            var data = new object[] {true, false, true, true};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    transformFunction.Invoke(new object[] { data[i] });
                }

                var aggregateResult = transformFunction.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);

                Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "CountEqual", 3)]
        public void CountEqualTests(string typeName, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();

            // 3/4 match
            var data1 = new object[] {1, 2, 3, 4};
            var data2 = new object[] {1, 2, 0, 4};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data1.Length; i++)
                {
                    transformFunction.Invoke(new[] { new[] { data1[i], data2[i]} });
                }

                var aggregateResult = transformFunction.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);

                Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "CountDistinct", 3)]
        public void CountDistinctTests(string typeName, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();

            // 3 distinct values
            var data = new object[] {1, 2, 2, 1, 3};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    transformFunction.Invoke(new object[] { data[i] });
                }

                var aggregateResult = transformFunction.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);

                Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", nameof(BuiltIn.AggregateFunctions.PivotToColumns), 3)]
        public void PivotColumnsTests(string typeName, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();

            var data1 = new object[] {"val1", "val2", "val3"};
            var data2 = new object[] {1, 2, 3};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data1.Length; i++)
                {
                    transformFunction.Invoke(new object[] { data1[i], data2[i], data1 });
                }

                object[] outputs;
                var aggregateResult = transformFunction.ReturnValue(0, out outputs);
                Assert.True((bool)aggregateResult);

                var result = (object[]) outputs[0];
                
                for (var i = 0; i < data1.Length; i++)
                {
                    Assert.Equal(data2[i], result[i]);
                }

                transformFunction.Reset();
            }
        }

        [Theory]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "FirstWhen", 1, 1)]
        [InlineData("dexih.functions.BuiltIn.AggregateFunctions", "LastWhen", 1, 9)]
        public void AggregateWhenFunctionTest(string typeName, string methodName, object test, object expectedResult)
        {
            var function = Functions.GetFunction(typeName, methodName);
            var transformFunction = function.GetTransformFunction();

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    var value = i % 2;

                    var functionResult = transformFunction.Invoke(new object[] { test, value, i });
                }

                var aggregateResult = transformFunction.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);

                if (aggregateResult is double aggregateResultDouble)
                {
                    Assert.Equal(expectedResult, Math.Round(aggregateResultDouble, 4));
                }
                else
                    Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }

        [Fact]
        public void MinMaxDateTest()
        {
            var minFunction = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "MinDate").GetTransformFunction(); 
            var maxFunction = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "MaxDate").GetTransformFunction(); 

            var baseDate = DateTime.Now;

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i <= 10; i++)
                {
                    var minFunctionResult = minFunction.Invoke(new object[] { baseDate.AddDays(i) });
                    var maxFunctionResult = maxFunction.Invoke(new object[] { baseDate.AddDays(i) });
                }

                var minResult = minFunction.ReturnValue(0, out _);
                var maxResult = maxFunction.ReturnValue(0, out _);
                Assert.NotNull(minResult);
                Assert.NotNull(maxResult);
                Assert.Equal(baseDate, (DateTime)minResult);
                Assert.Equal(baseDate.AddDays(10), (DateTime)maxResult);

                minFunction.Reset();
                maxFunction.Reset();
            }
        }
        
        [Fact]
        public void FastEncryptTest()
        {
            var globalVariables = new GlobalVariables("abc");
            const string value = "encrypt this";
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "FastEncrypt").GetTransformFunction(globalVariables);
            var encrypted = function.Invoke(new object[] {value});

            function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "FastDecrypt").GetTransformFunction(globalVariables);
            var decrypted = function.Invoke(new[] {encrypted});

            Assert.Equal(value, decrypted);
        }

        [Fact]
        public void StrongEncryptTest()
        {
            var globalVariables = new GlobalVariables("abc");
            const string value = "encrypt this";
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "StrongEncrypt").GetTransformFunction(globalVariables);
            var encrypted = function.Invoke(new object[] {value});

            function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "StrongDecrypt").GetTransformFunction(globalVariables);
            var decrypted = function.Invoke(new[] {encrypted});

            Assert.Equal(value, decrypted);
        }
        
        [Fact]
        public void EncryptTest()
        {
            const string value = "encrypt this";
            const string key = "abc";
            const int iterations = 10;
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "Encrypt").GetTransformFunction();
            var encrypted = function.Invoke(new object[] {value, key, iterations});

            function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "Decrypt").GetTransformFunction();
            var decrypted = function.Invoke(new [] {encrypted, key, iterations});

            Assert.Equal(value, decrypted);
        }
        
        [Fact]
        public void HashTest()
        {
            const string value = "hash this";
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "SecureHash").GetTransformFunction();
            var hashed = function.Invoke(new object[] {value});

            function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "ValidateSecureHash").GetTransformFunction();
            var passed = function.Invoke(new [] {value, hashed});

            Assert.Equal(true, passed);
            
            //check hash fails with different value.
            passed = function.Invoke(new [] {"hash thiS", hashed});
            Assert.Equal(false, passed);
        }
        
        [Fact]
        public void SHA1Test()
        {
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "CreateSHA1").GetTransformFunction();
            var sha1 = function.Invoke(new object[] {"sha this"});
            var sha1a = function.Invoke(new [] {"sha this"});

            // check same value hashed the same.
            Assert.Equal(sha1, sha1a);

            var sha1b = function.Invoke(new [] {"sha thiS"});

            // check different value hashed the differently.
            Assert.NotEqual(sha1, sha1b);


            // uncomment below for tests on much larger string.
            
//            // sha a very large string
//            var random = new Random();
//            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
//            
//            Span<char> largeString = Enumerable.Repeat(chars, 1_000_000_000).Select(s => s[random.Next(s.Length)]).ToArray();
//            
//            var timer = Stopwatch.StartNew();
//            var bigsha = function.RunFunction(new object[] {largeString.ToString()});
//            timer.Stop();
//            _output.WriteLine($"SHA of large string value {bigsha}, and ran in {timer.ElapsedMilliseconds} ms.");
//
//            // largeString[999999999] = ' ';
//            largeString[0] = 'a';
//            
//            timer = Stopwatch.StartNew();
//            var bigsha2 = function.RunFunction(new object[] {largeString.ToString()});
//            timer.Stop();
//            _output.WriteLine($"SHA of large string value {bigsha2}, and ran in {timer.ElapsedMilliseconds} ms.");
//            
//            Assert.NotEqual(bigsha, bigsha2);

        }
        
        [Fact]
        public void CountTest()
        {
            var function = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Count").GetTransformFunction(); 

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    var functionResult = function.Invoke(new object[] { });
                }

                var aggregateResult = function.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);
                Assert.Equal(10, aggregateResult);

                function.Reset();
            }
        }

        [Fact]
        public void ConcatAggTest()
        {
            var function = Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "ConcatAgg").GetTransformFunction(); 

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    var functionResult = function.Invoke(new object[] { i.ToString(), "," });
                }

                var aggregateResult = function.ReturnValue(0, out _);
                Assert.NotNull(aggregateResult);
                Assert.Equal("1,2,3,4,5,6,7,8,9,10", aggregateResult);

                function.Reset();
            }

        }

        [Fact]
        public void Function_XPathValue()
        {
            //Get a rows that exists.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "XPathValues").GetTransformFunction(); 
            var param = new object[] { "<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>", new [] { "//row[1]", "//row[2]", "//row[3]"} };
            Assert.True((bool)function.Invoke(param, out var outputs));
            var result = (object[]) outputs[0];;
            Assert.Equal("0", result[0]);
            Assert.Equal("1", result[1]);
            Assert.Equal("2", result[2]);
        }

        [Fact]
        public void Function_JSONValue()
        {
            //Get a rows that exists.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "JsonValues").GetTransformFunction(); 
            var param = new object[] { "{ 'value1': '1', 'value2' : '2', 'value3': '3', 'array' : {'v1' : '1', 'v2' : '2'} }", new [] { "value1", "value2", "value3", "array", "badvalue" }};
            
            Assert.False((bool)function.Invoke(param, out var outputs));
            var result = (object[]) outputs[0];;
            Assert.Equal("1", result[0]);
            Assert.Equal("2", result[1]);
            Assert.Equal("3", result[2]);
            Assert.Null(result[4]);

            //get the sub Json string, and run another parse over this.
            var moreValues = (string)result[3];
            param = new object[] { moreValues, new [] { "v1", "v2"} };
            function = Functions.GetFunction("dexih.functions.BuiltIn.MapFunctions", "JsonValues").GetTransformFunction();
            Assert.True((bool)function.Invoke(param, out outputs));
            result = (object[]) outputs[0];;
            Assert.Equal("1", result[0]);
            Assert.Equal("2", result[1]);
        }


        [Fact]
        public void RowFunctions_GenerateSequence()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.RowFunctions", "GenerateSequence").GetTransformFunction();
            var param = new object[] { 0, 10, 2 };
            for (var i = 0; i <= 10; i += 2)
            {
                Assert.True((bool)function.Invoke(param, out var outputs));
                Assert.Equal(i, (int)outputs[0]);
            }
            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.Invoke(param));
        }

        [Fact]
        public void RowFunctions_SplitColumnToRows()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.RowFunctions", "SplitColumnToRows").GetTransformFunction();
            var param = new object[] { "|", "|value2|value3||value5||", 6 };
            var compare = new string[] { "", "value2", "value3", "", "value5", "", "" };
            for (var i = 0; i < 6; i++)
            {
                Assert.True((bool)function.Invoke(param, out var outputs));
                Assert.Equal(compare[i], (string)outputs[0]);
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.Invoke(param));
        }

        [Fact]
        public void RowFunctions_XPathNodesToRows()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.RowFunctions", "XPathNodesToRows").GetTransformFunction();
            var param = new object[] { "<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>", "//row", 5 };
            for (var i = 0; i < 5; i++)
            {
                Assert.True((bool)function.Invoke(param, out var outputs));
                Assert.Equal(i.ToString(), (string)outputs[0]);
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.Invoke(param));
        }

        [Fact]
        public void RowFunctions_JsonElementsToRows()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.RowFunctions", "JsonElementsToRows").GetTransformFunction();
            var param = new object[] { "{'results' : [{'value1' : 'r1v1', 'value2' : 'r1v2'}, {'value1' : 'r2v1', 'value2' : 'r2v2'}]} ", "results[*]", 2 };
            for (var i = 1; i <= 2; i++)
            {
                Assert.True((bool)function.Invoke(param, out var outputs));
                var jsonResult = (string)outputs[0];
                var results = JObject.Parse(jsonResult);
                Assert.Equal("r" + i.ToString() + "v1", results.SelectToken("value1").ToString());
                Assert.Equal("r" + i.ToString() + "v2", results.SelectToken("value2").ToString());
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.Invoke(param));
        }
        
        [Fact]
        public void RowFunctions_JsonPivotElementToRows()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction("dexih.functions.BuiltIn.RowFunctions", "JsonPivotElementToRows").GetTransformFunction();
            var param = new object[] { "{'results' : {'name1' : 'value1', 'name2' : 'value2', 'name3' : 'value3'}} ", "results", 3 };
            for (var i = 1; i <= 3; i++)
            {
                Assert.True((bool)function.Invoke(param, out var outputs));
                var name = (string)outputs[0];
                var value = (string)outputs[1];
                Assert.Equal(name, "name" + i.ToString());
                Assert.Equal(value, "value" + i.ToString());
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.Invoke(param));
        }
    }
}
