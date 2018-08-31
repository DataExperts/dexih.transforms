using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using dexih.functions.BuiltIn;
using Xunit;
using Xunit.Abstractions;

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
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Concat), new object[] { new [] {"test1", "test2", "test3" }}, "test1test2test3")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.IndexOf), new object[] { "test string", "s" }, 2)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Insert), new object[] { "test string", 5, "new " }, "test new string")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Join), new object[] { ",", new [] {"test1", "test2", "test3" }}, "test1,test2,test3")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.PadLeft), new object[] { "test", 7, "-" }, "---test")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.PadRight), new object[] { "test", 7, "-" }, "test---")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Remove), new object[] { "testing", 1, 4 }, "tng")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Replace), new object[] { "stress test", "es", "aa" }, "straas taat")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Split), new object[] { "test1,test2,test3", ",", 3 }, 3)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Substring), new object[] { "testing", 1, 4 }, "esti")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.ToLower), new object[] { "  tEsT1 " }, "  test1 ")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.ToUpper), new object[] { "  tEsT1 " }, "  TEST1 ")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Trim), new object[] { "  tEsT1 " }, "tEsT1")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.TrimEnd), new object[] { "  tEsT1 " }, "  tEsT1")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.TrimStart), new object[] { "  tEsT1 " }, "tEsT1 ")]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Length), new object[] { "test" }, 4)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.WordCount), new object[] { "word1  word2 word3" }, 3)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.WordExtract), new object[] { "word1  word2 word3", 2 }, "word3")]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.LessThan), new object[] { 1, 2 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.LessThan), new object[] { 2, 1 }, false)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.LessThanEqual), new object[] { 1, 2 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.LessThanEqual), new object[] { 2, 2 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.LessThanEqual), new object[] { 2, 1 }, false)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.GreaterThan), new object[] { 2, 1 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.GreaterThanEqual), new object[] { 2, 1 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsEqual), new object[] { new object[] {2, 2} }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsBooleanEqual), new object[] { new [] { true, true} }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsEqual), new object[] { new object[] {3, 2} }, false)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsNumber), new object[] { "123" }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsNumber), new object[] { "123a" }, false)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsNull), new object[] { null }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsBetween), new object[] { 2, 1, 3 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsBetweenInclusive), new object[] { 2, 1, 3 }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.RegexMatch), new object[] { "abbbb", "ab*" }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.Contains), new object[] { "testing", "est", false }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.EndsWith), new object[] { "testing", "ing", false }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.StartsWith), new object[] { "testing", "tes", false }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsUpper), new object[] { "TEST", true }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsLower), new object[] { "test", true }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsAlpha), new object[] { "test" }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsAlphaNumeric), new object[] { "test123" }, true)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsPattern), new object[] { "Hello12", "Aaaaa99" }, true)]
//        [InlineData(typeof(BuiltIn.ConditionFunctions), nameof(ConditionFunctions.IsDaylightSavingTime), new object[] { "2015-09-24T21:22:48.2698750Z" }, false)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Abs), new object[] { -3 }, (double)3)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.DivRem), new object[] { 6, 4 }, 1)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Pow), new object[] { 6, 2 }, (double)36)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Round), new object[] { 6.5 }, (double)6)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Sign), new object[] { -4 }, (double)-1)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Sqrt), new object[] { 9 }, (double)3)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.Truncate), new object[] { 6.4 }, (double)6)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.IsIn), new object[] { "test2", new object[] {"test1", "test2", "test3"} }, true)]
        [InlineData(typeof(MapFunctions), nameof(MapFunctions.GetDistanceTo), new object[] { -38, -145, -34 ,- 151 }, 699082.1288)] //melbourne to sydney distance
        [InlineData(typeof(ValidationFunctions), nameof(ValidationFunctions.MaxLength), new object[] { "abcdef", 5 }, false)]
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.RangeIntersect), new object[] { 1, 2, 3, 4 }, false)] //(1,2)(3,4) not intersect
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.RangeIntersect), new object[] { 1, 3, 3, 4 }, false)] //(1,3)(3,4) do intersect
        [InlineData(typeof(ConditionFunctions), nameof(ConditionFunctions.RangeIntersect), new object[] { 1, 4, 3, 4 }, true)] //(1,4)(3,4) do intersect
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.RangeCategorize), new object[] { 1D, new [] {1D, 5D, 10D}}, true)]
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.RangeCategorize), new object[] { 11D, new [] {1D, 5D, 10D} }, false)]
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.DiscreteRangeCategorize), new object[] { 1L, new [] {1L, 5L, 10L} }, true)]
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.DiscreteRangeCategorize), new object[] { 11L, new [] {1L, 5L, 10L} }, false)]
        [MemberData(nameof(OtherFunctions))]
        public void StandardFunctionTest(Type type, string methodName, object[] parameters, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
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
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.DayOfMonth), new object[] {date1}, 24},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.DaysInMonth), new object[] {date1}, 30},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.DayOfWeekName), new object[] {date1}, "Thursday"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.DayOfWeekNumber), new object[] {date1}, 4},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.WeekOfYear), new object[] {date1}, 39},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.DayOfYear), new object[] {date1}, 267},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Month), new object[] {date1}, 9},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.ShortMonth), new object[] {date1}, "Sep"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.LongMonth), new object[] {date1}, "September"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Year), new object[] {date1}, 2015},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.ToLongDateString), new object[] {date1}, "Thursday, 24 September 2015"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.ToLongTimeString), new object[] {date1}, "12:00:00 AM"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.ToShortDateString), new object[] {date1}, "24/09/2015"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.ToShortTimeString), new object[] {date1}, "12:00 AM"},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.DateToString), new object[] {date1, "dd MMM yyyy"}, "24 Sep 2015"},
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.IsLeapYear), new object[] {date1}, false},
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.IsWeekend), new object[] { date1 }, false},
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.IsWeekDay), new object[] { date1 }, true},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddDays), new object[] { date1, 1 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddHours), new object[] { date1, 24 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddMilliseconds), new object[] { date1, 86400000 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddMinutes), new object[] { date1, 1440 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddMonths), new object[] { date1, 1 }, DateTime.Parse("24 Oct 2015")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddSeconds), new object[] { date1, 86400 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AddYears), new object[] { date1, 1 }, DateTime.Parse("24 Sep 2016")},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AgeInYearsAtDate), new object[] { date1,  DateTime.Parse("2016-09-24") }, 1},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AgeInYearsAtDate), new object[] { date1,  date1 }, 0},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.AgeInYearsAtDate), new object[] { date1,  DateTime.Parse("2017-09-25") }, 2},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Add), new object[] { 1m, new [] {2m} }, 3m},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Ceiling), new object[] { 6.4m }, (Decimal)7 },
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Divide), new object[] { 6m, 2m }, (Decimal)3 },
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Floor), new object[] { 6.4m }, (Decimal)6 },
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Multiply), new object[] { 6m, new [] {2m} }, (Decimal)12 },
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Negate), new object[] { 6m }, (Decimal) (-6)},
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Remainder), new object[] { 6m, 4m }, (Decimal)2 },
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.Subtract), new object[] { 6m, new [] { 2m} }, (Decimal)4 },
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.IsDateTimeEqual), new object[] { new [] { DateTime.Parse("25 Sep 2015"), DateTime.Parse("25 Sep 2015")} }, true },
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.IsBetween), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("25 Sep 2015"), DateTime.Parse("27 Sep 2015") }, true },
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.IsBetweenInclusive), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("26 Sep 2015"), DateTime.Parse("27 Sep 2015") }, true },
                    new object[] { typeof(MapFunctions), nameof(MapFunctions.UnixTimeStampToDate), new object[] { 1518739200 }, new DateTime(2018, 2, 16, 0, 0, 0, 0, DateTimeKind.Utc).ToLocalTime() },
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.RangeIntersect), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("27 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, false },
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.RangeIntersect), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, false },
                    new object[] { typeof(ConditionFunctions), nameof(ConditionFunctions.RangeIntersect), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("29 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, true },
                };
            }
        }

        [Theory]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Sum), (double)55)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Average), 5.5)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Median), 5.5)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.StdDev), 2.8723)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Variance), 8.25)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Min), 1)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Max), 10)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.First), 1)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.Last), 10)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.CountDistinct), 10)]
        public void AggregateFunctionTest(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
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
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.CountTrue), 3)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.CountFalse), 1)]
        public void CountTests(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
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
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.CountEqual), 3)]
        public void CountEqualTests(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
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
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.CountDistinct), 3)]
        public void CountDistinctTests(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
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
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.PivotToColumns))]
        public void PivotColumnsTests(Type type, string methodName)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
            var transformFunction = function.GetTransformFunction();

            var data1 = new object[] {"val1", "val2", "val3"};
            var data2 = new object[] {1, 2, 3};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data1.Length; i++)
                {
                    transformFunction.Invoke(new object[] { data1[i], data2[i], data1 });
                }

                var aggregateResult = transformFunction.ReturnValue(0, out var outputs);
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
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.FirstWhen), 1, 1)]
        [InlineData(typeof(AggregateFunctions), nameof(AggregateFunctions.LastWhen), 1, 9)]
        public void AggregateWhenFunctionTest(Type type, string methodName, object test, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName);
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
            var minFunction = Functions.GetFunction(typeof(AggregateFunctions).FullName, nameof(AggregateFunctions.MinDate)).GetTransformFunction(); 
            var maxFunction = Functions.GetFunction(typeof(AggregateFunctions).FullName, nameof(AggregateFunctions.MaxDate)).GetTransformFunction(); 

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
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.FastEncrypt)).GetTransformFunction(null, globalVariables);
            var encrypted = function.Invoke(new object[] {value});

            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.FastDecrypt)).GetTransformFunction(null, globalVariables);
            var decrypted = function.Invoke(new[] {encrypted});

            Assert.Equal(value, decrypted);
        }

        [Fact]
        public void StrongEncryptTest()
        {
            var globalVariables = new GlobalVariables("abc");
            const string value = "encrypt this";
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.StrongEncrypt)).GetTransformFunction(null, globalVariables);
            var encrypted = function.Invoke(new object[] {value});

            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.StrongDecrypt)).GetTransformFunction(null, globalVariables);
            var decrypted = function.Invoke(new[] {encrypted});

            Assert.Equal(value, decrypted);
        }
        
        [Fact]
        public void EncryptTest()
        {
            const string value = "encrypt this";
            const string key = "abc";
            const int iterations = 10;
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Encrypt)).GetTransformFunction();
            var encrypted = function.Invoke(new object[] {value, key, iterations});

            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Decrypt)).GetTransformFunction();
            var decrypted = function.Invoke(new [] {encrypted, key, iterations});

            Assert.Equal(value, decrypted);
        }
        
        [Fact]
        public void HashTest()
        {
            const string value = "hash this";
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.SecureHash)).GetTransformFunction();
            var hashed = function.Invoke(new object[] {value});

            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.ValidateSecureHash)).GetTransformFunction();
            var passed = (bool)function.Invoke(new [] {value, hashed});

            Assert.True(passed);
            
            //check hash fails with different value.
            passed = (bool) function.Invoke(new [] {"hash thiS", hashed});
            Assert.False(passed);
        }
        
        [Fact]
        public void SHA1Test()
        {
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.CreateSHA1)).GetTransformFunction();
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
            var function = Functions.GetFunction(typeof(AggregateFunctions).FullName, nameof(AggregateFunctions.Count)).GetTransformFunction(); 

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
            var function = Functions.GetFunction(typeof(AggregateFunctions).FullName, nameof(AggregateFunctions.ConcatAgg)).GetTransformFunction(); 

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
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.XPathValues)).GetTransformFunction(); 
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
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.JsonValues)).GetTransformFunction(); 
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
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.JsonValues)).GetTransformFunction();
            Assert.True((bool)function.Invoke(param, out outputs));
            result = (object[]) outputs[0];;
            Assert.Equal("1", result[0]);
            Assert.Equal("2", result[1]);
        }


        [Fact]
        public void RowFunctions_GenerateSequence()
        {
            //Use a for loop to simulate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.GenerateSequence)).GetTransformFunction();
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
            //Use a for loop to simulate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.SplitColumnToRows)).GetTransformFunction();
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
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.XPathNodesToRows)).GetTransformFunction();
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
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.JsonElementsToRows)).GetTransformFunction();
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
            //Use a for loop to simulate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.JsonPivotElementToRows)).GetTransformFunction();
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
