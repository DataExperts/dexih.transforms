using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Xml;
using dexih.functions.BuiltIn;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace dexih.functions.builtIn.tests
{
    public class BuiltIn
    {
        const string BuiltInAssembly = "dexih.functions.builtIn.dll";
        
   private readonly ITestOutputHelper _output;

        public BuiltIn(ITestOutputHelper output)
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
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.LessThan), new object[] { 1, 2 }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.LessThan), new object[] { 2, 1 }, false)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.LessThanOrEqual), new object[] { 1, 2 }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.LessThanOrEqual), new object[] { 2, 2 }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.LessThanOrEqual), new object[] { 2, 1 }, false)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.GreaterThan), new object[] { 2, 1 }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.GreaterThanOrEqual), new object[] { 2, 1 }, true)]
        [InlineData(typeof(ConditionFunctions<int>), nameof(ConditionFunctions<int>.IsEqual), new object[] { new [] {2, 2} }, true)]
        [InlineData(typeof(ConditionFunctions<bool>), nameof(ConditionFunctions<int>.IsEqual), new object[] { new [] { true, true} }, true)]
        [InlineData(typeof(ConditionFunctions<int>), nameof(ConditionFunctions<int>.IsEqual), new object[] { new [] {3, 2} }, false)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsNumber), new object[] { "123" }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsNumber), new object[] { "123a" }, false)]
        [InlineData(typeof(ConditionFunctions<string>), nameof(ConditionFunctions<int>.IsNull), new object[] { null }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsBetween), new object[] { 2, 1, 3 }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsBetweenInclusive), new object[] { 2, 1, 3 }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RegexMatch), new object[] { "abbbb", "ab*" }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.Contains), new object[] { "testing", "est", false }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.EndsWith), new object[] { "testing", "ing", false }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.StartsWith), new object[] { "testing", "tes", false }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsUpper), new object[] { "TEST", true }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsLower), new object[] { "test", true }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsAlpha), new object[] { "test" }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsAlphaNumeric), new object[] { "test123" }, true)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsPattern), new object[] { "Hello12", "Aaaaa99" }, true)]
//        [InlineData(typeof(BuiltIn.ConditionFunctions), nameof(ConditionFunctions.IsDaylightSavingTime), new object[] { "2015-09-24T21:22:48.2698750Z" }, false)]
        [InlineData(typeof(MathFunctions), nameof(MathFunctions.Abs), new object[] { -3 }, (double)3)]
        [InlineData(typeof(MathFunctions), nameof(MathFunctions.DivRem), new object[] { 6, 4 }, 1)]
        [InlineData(typeof(MathFunctions), nameof(MathFunctions.Pow), new object[] { 6, 2 }, (double)36)]
        [InlineData(typeof(MathFunctions), nameof(MathFunctions.Round), new object[] { 6.5 }, (double)6)]
        [InlineData(typeof(ArithmeticFunctions<>), nameof(ArithmeticFunctions<int>.Sign), new object[] { -4 }, -1)]
        [InlineData(typeof(MathFunctions), nameof(MathFunctions.Sqrt), new object[] { 9 }, (double)3)]
        [InlineData(typeof(MathFunctions), nameof(MathFunctions.Truncate), new object[] { 6.4 }, (double)6)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.ArrayContains), new object[] { "test2", new string[] {"test1", "test2", "test3"} }, true)]
        [InlineData(typeof(GeometryFunctions), nameof(GeometryFunctions.GeographicDistance), new object[] { -38, -145, -34 ,- 151 }, 699082.1288)] //melbourne to sydney distance
        [InlineData(typeof(ValidationFunctions), nameof(ValidationFunctions.MaxLength), new object[] { "abcdef", 5 }, false)]
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RangeIntersect), new object[] { 1, 2, 3, 4 }, false)] //(1,2)(3,4) not intersect
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RangeIntersect), new object[] { 1, 3, 3, 4 }, false)] //(1,3)(3,4) do intersect
        [InlineData(typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RangeIntersect), new object[] { 1, 4, 3, 4 }, true)] //(1,4)(3,4) do intersect
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.RangeCategorize), new object[] { 1D, new [] {1D, 5D, 10D}}, true)]
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.RangeCategorize), new object[] { 11D, new [] {1D, 5D, 10D} }, false)]
        [InlineData(typeof(CategorizeFunctions), nameof(CategorizeFunctions.DiscreteRangeCategorize), new object[] { 1L, new [] {1L, 5L, 10L} }, true)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertTemperature), new object[] { 100, ConversionFunctions.ETemperatureScale.Kelvin, ConversionFunctions.ETemperatureScale.Celsius }, -173.15)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertTemperature), new object[] { 40, ConversionFunctions.ETemperatureScale.Celsius, ConversionFunctions.ETemperatureScale.Fahrenheit }, 104d)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertMass), new object[] { 1, ConversionFunctions.EMassScale.Kilogram, ConversionFunctions.EMassScale.Pound }, 2.2046)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertMassString), new object[] { 1, "kg", ConversionFunctions.EMassScale.Pound }, 2.2046)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertLength), new object[] { 1, ConversionFunctions.ELengthScale.Kilometer, ConversionFunctions.ELengthScale.Mile }, 0.6214)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertLengthString), new object[] { 1, "km", ConversionFunctions.ELengthScale.Mile }, 0.6214)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertTime), new object[] { 1, ConversionFunctions.ETimeScale.Hour, ConversionFunctions.ETimeScale.Millisecond }, 3600000d)]
        [InlineData(typeof(ConversionFunctions), nameof(ConversionFunctions.ConvertTimeString), new object[] { 1, "h", ConversionFunctions.ETimeScale.Millisecond }, 3600000d)]
        [MemberData(nameof(OtherFunctions))]
        public void StandardFunctionTest(Type type, string methodName, object[] parameters, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(parameters[0]?.GetType());
            transformFunction.OnNull = EErrorAction.Execute;
            var returnValue = transformFunction.RunFunction(parameters, CancellationToken.None).returnValue;

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
                var date2 = DateTime.Parse("2015-09-25");

                return new[]
                {
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DayOfMonth), new object[] {date1}, 24},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DaysInMonth), new object[] {date1}, 30},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DayOfWeekName), new object[] {date1}, "Thursday"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DayOfWeekNumber), new object[] {date1}, 4},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.WeekOfYear), new object[] {date1}, 39},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DayOfYear), new object[] {date1}, 267},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.Month), new object[] {date1}, 9},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.ShortMonth), new object[] {date1}, "Sep"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.LongMonth), new object[] {date1}, "September"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.Year), new object[] {date1}, 2015},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.ToLongDateString), new object[] {date1}, "Thursday, 24 September 2015"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.ToLongTimeString), new object[] {date1}, "12:00:00 AM"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.ToShortDateString), new object[] {date1}, "24/09/2015"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.ToShortTimeString), new object[] {date1}, "12:00 AM"},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DateToString), new object[] {date1, "dd MMM yyyy"}, "24 Sep 2015"},
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsLeapYear), new object[] {date1}, false},
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsWeekend), new object[] { date1 }, false},
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsWeekDay), new object[] { date1 }, true},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddDays), new object[] { date1, 1 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddHours), new object[] { date1, 24 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddMilliseconds), new object[] { date1, 86400000 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddMinutes), new object[] { date1, 1440 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddMonths), new object[] { date1, 1 }, DateTime.Parse("24 Oct 2015")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddSeconds), new object[] { date1, 86400 }, DateTime.Parse("25 Sep 2015")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AddYears), new object[] { date1, 1 }, DateTime.Parse("24 Sep 2016")},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.DaysBetween), new object[] { date1, date2 }, 1d},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.HoursBetween), new object[] { date1, date2 }, 24d},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.MinutesBetween), new object[] { date1, date2 }, 1440d},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.SecondsBetween), new object[] { date1, date2 }, 86400d},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.MillisecondsBetween), new object[] { date1, date2 }, 86400000d},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AgeInYearsAtDate), new object[] { date1,  DateTime.Parse("2016-09-24") }, 1},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AgeInYearsAtDate), new object[] { date1,  date1 }, 0},
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.AgeInYearsAtDate), new object[] { date1,  DateTime.Parse("2017-09-25") }, 2},
                    new object[] { typeof(ArithmeticFunctions<>), nameof(ArithmeticFunctions<int>.Add), new object[] { 1m, new [] {2m} }, 3m},
                    new object[] { typeof(MathFunctions), nameof(MathFunctions.Ceiling), new object[] { 6.4m }, (decimal)7 },
                    new object[] { typeof(ArithmeticFunctions<>), nameof(ArithmeticFunctions<int>.Divide), new object[] { 6m, 2m }, (decimal)3 },
                    new object[] { typeof(MathFunctions), nameof(MathFunctions.Floor), new object[] { 6.4m }, (decimal)6 },
                    new object[] { typeof(ArithmeticFunctions<>), nameof(ArithmeticFunctions<int>.Multiply), new object[] { 6m, new [] {2m} }, (decimal)12 },
                    new object[] { typeof(ArithmeticFunctions<>), nameof(ArithmeticFunctions<int>.Negate), new object[] { 6m }, (decimal) (-6)},
                    new object[] { typeof(MathFunctions), nameof(MathFunctions.Remainder), new object[] { 6m, 4m }, (decimal)2 },
                    new object[] { typeof(ArithmeticFunctions<>), nameof(ArithmeticFunctions<int>.Subtract), new object[] { 6m, new [] { 2m} }, (decimal)4 },
                    new object[] { typeof(ConditionFunctions<DateTime>), nameof(ConditionFunctions<int>.IsEqual), new object[] { new [] { DateTime.Parse("25 Sep 2015"), DateTime.Parse("25 Sep 2015")} }, true },
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsBetween), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("25 Sep 2015"), DateTime.Parse("27 Sep 2015") }, true },
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.IsBetweenInclusive), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("26 Sep 2015"), DateTime.Parse("27 Sep 2015") }, true },
                    new object[] { typeof(DateFunctions), nameof(DateFunctions.UnixTimeStampToDate), new object[] { 1518739200 }, new DateTime(2018, 2, 16, 0, 0, 0, 0, DateTimeKind.Utc).ToLocalTime() },
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RangeIntersect), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("27 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, false },
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RangeIntersect), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, false },
                    new object[] { typeof(ConditionFunctions<>), nameof(ConditionFunctions<int>.RangeIntersect), new object[] { DateTime.Parse("26 Sep 2015"), DateTime.Parse("29 Sep 2015"), DateTime.Parse("28 Sep 2015"), DateTime.Parse("29 Sep 2015") }, true },
                };
            }
        }

        [Fact]
        public void FiscalDateTest()
        {
            var dateFunctions = new DateFunctions();

            var startDate = new DateTime(2018, 02, 04);

            // first day of the fiscal year
            var result1 = dateFunctions.FiscalDateComponents(new DateTime(2020, 02, 02), DateFunctions.eCalendarType.FourFiveFour, startDate);
            Assert.Equal(1, result1.Day);
            Assert.Equal(1, result1.DayOfWeek);
            Assert.Equal(1, result1.WeekOfYear);
            Assert.Equal(1, result1.WeekOfQuarter);
            Assert.Equal(1, result1.Quarter);
            Assert.Equal(1, result1.Month);
            Assert.Equal(2020, result1.Year);

            // last day of the second month
            result1 = dateFunctions.FiscalDateComponents(new DateTime(2020, 04, 04), DateFunctions.eCalendarType.FourFiveFour, startDate);
            Assert.Equal(35, result1.Day);
            Assert.Equal(7, result1.DayOfWeek);
            Assert.Equal(9, result1.WeekOfYear);
            Assert.Equal(9, result1.WeekOfQuarter);
            Assert.Equal(1, result1.Quarter);
            Assert.Equal(2, result1.Month);
            Assert.Equal(2020, result1.Year);

            // last day of the third month
            result1 = dateFunctions.FiscalDateComponents(new DateTime(2020, 05, 02), DateFunctions.eCalendarType.FourFiveFour, startDate);
            Assert.Equal(28, result1.Day);
            Assert.Equal(7, result1.DayOfWeek);
            Assert.Equal(13, result1.WeekOfYear);
            Assert.Equal(13, result1.WeekOfQuarter);
            Assert.Equal(1, result1.Quarter);
            Assert.Equal(3, result1.Month);
            Assert.Equal(2020, result1.Year);

            // last day of the year
            result1 = dateFunctions.FiscalDateComponents(new DateTime(2021, 01, 30), DateFunctions.eCalendarType.FourFiveFour, startDate);
            Assert.Equal(28, result1.Day);
            Assert.Equal(7, result1.DayOfWeek);
            Assert.Equal(52, result1.WeekOfYear);
            Assert.Equal(13, result1.WeekOfQuarter);
            Assert.Equal(4, result1.Quarter);
            Assert.Equal(12, result1.Month);
            Assert.Equal(2020, result1.Year);
        }

        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Sum), 55)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Average), 5.5)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Median), 5.5)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.StdDev), 2.8723)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Variance), 8.25)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Min), 1)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Max), 10)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.First), 1)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.Last), 10)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.CountDistinct), 10)]
        public void AggregateFunctionTest(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(expectedResult.GetType());

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    transformFunction.RunFunction(new object[] { i }, CancellationToken.None);
                }

                var aggregateResult = transformFunction.RunResult(null, out _, CancellationToken.None).returnValue;
                Assert.NotNull(aggregateResult);

                if(aggregateResult is double d)
                {
                    Assert.Equal(expectedResult, Math.Round(d, 4));
                }
                else
                    Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }

        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.CountTrue), 3)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.CountFalse), 1)]
        public void CountTests(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(int));

            var data = new object[] {true, false, true, true};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    transformFunction.RunFunction(new[] { data[i] }, CancellationToken.None);
                }

                var aggregateResult = transformFunction.RunResult(null, out _, CancellationToken.None).returnValue;
                Assert.NotNull(aggregateResult);

                Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }
        
        [Fact]
        public void MovingSumTest()
        {
            var function = Functions.GetFunction(typeof(AggregateFunctions<>).FullName, nameof(AggregateFunctions<int>.MovingSum), BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(int));

            var data = new object[] {1, 2, 3, 4, 5};
            var expectedResult = new object[] {6, 10, 15, 14, 12};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    transformFunction.RunFunction(new[] { data[i] }, CancellationToken.None);
                }

                for(var i = 0; i < expectedResult.Length; i++)
                {
                    var functionVariables = new FunctionVariables()
                    {
                        Index = i,
                    };
                    var aggregateResult = transformFunction
                        .RunResult(functionVariables, new object[] {2, 2}, out _, CancellationToken.None).returnValue;
                    Assert.NotNull(aggregateResult);
                    Assert.Equal(expectedResult[i], aggregateResult);
                }

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.RunningAverage), new object[] {1,1.5,2,2.5,3})]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.RunningCount), new object[] {1,2,3,4,5})]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.RunningSum), new object[] {1,3,6,10,15})]
        public void RunningTests(Type type, string methodName, object[] expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(double));

            var data = new object[] {1, 2, 3, 4, 5};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    var result = transformFunction.RunFunction(new[] { data[i] }, CancellationToken.None);
                    Assert.NotNull(result);
                    Assert.Equal(Convert.ToDouble(expectedResult[i]), Convert.ToDouble(result.returnValue));
                }
                
                transformFunction.Reset();
            }
        }
        
        [Fact]
        
        public void MovingAverageTest()
        {
            var function = Functions.GetFunction(typeof(AggregateFunctions<>).FullName, nameof(AggregateFunctions<double>.MovingAverage), BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(double));

            var data = new object[] {1, 2, 3, 4, 5};
            var expectedResult = new object[] {1.5, 2d, 3d, 4d, 4.5};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    transformFunction.RunFunction(new[] { data[i] }, CancellationToken.None);
                }

                for(var i = 0; i < expectedResult.Length; i++)
                {
                    var functionVariables = new FunctionVariables()
                    {
                        Index = i,
                    };
                    var aggregateResult = transformFunction
                        .RunResult(functionVariables, new object[] {1, 1}, out _, CancellationToken.None).returnValue;
                    Assert.NotNull(aggregateResult);
                    Assert.Equal(expectedResult[i], aggregateResult);
                }

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.CountEqual), 3)]
        public void CountEqualTests(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(int));

            // 3/4 match
            var data1 = new object[] {1, 2, 3, 4};
            var data2 = new object[] {1, 2, 0, 4};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data1.Length; i++)
                {
                    transformFunction.RunFunction(new[] { new[] { data1[i], data2[i]} }, CancellationToken.None);
                }

                var aggregateResult = transformFunction.RunResult(null, out _, CancellationToken.None).returnValue;
                Assert.NotNull(aggregateResult);

                Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.CountDistinct), 3)]
        public  void CountDistinctTests(Type type, string methodName, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(int));

            // 3 distinct values
            var data = new object[] {1, 2, 2, 1, 3};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data.Length; i++)
                {
                    transformFunction.RunFunction(new[] { data[i] }, CancellationToken.None);
                }

                var aggregateResult = transformFunction.RunResult(null, out _, CancellationToken.None).returnValue;
                Assert.NotNull(aggregateResult);

                Assert.Equal(expectedResult, aggregateResult);

                transformFunction.Reset();
            }
        }
        
        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.PivotToColumns))]
        public void PivotColumnsTests(Type type, string methodName)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(int));

            var data1 = new object[] {"val1", "val2", "val3"};
            var data2 = new object[] {1, 2, 3};

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i < data1.Length; i++)
                {
                    transformFunction.RunFunction(new[] { data1[i], data2[i], data1 }, CancellationToken.None);
                }

                var aggregateResult = transformFunction.RunResult(null, out var outputs, CancellationToken.None).returnValue;
                Assert.True((bool)aggregateResult);

                var result = (int[]) outputs[0];
                
                for (var i = 0; i < data1.Length; i++)
                {
                    Assert.Equal(data2[i], result[i]);
                }

                transformFunction.Reset();
            }
        }

        [Theory]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.FirstWhen), 1, 1)]
        [InlineData(typeof(AggregateFunctions<>), nameof(AggregateFunctions<int>.LastWhen), 1, 9)]
        public  void AggregateWhenFunctionTest(Type type, string methodName, int test, object expectedResult)
        {
            var function = Functions.GetFunction(type.FullName, methodName, BuiltInAssembly);
            var transformFunction = function.GetTransformFunction(typeof(int));

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    var value = i % 2;

                    var functionResult = transformFunction.RunFunction(new object[] { test == value, i }, CancellationToken.None);
                }

                var aggregateResult = transformFunction.RunResult(null, out _, CancellationToken.None).returnValue;
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
            var minFunction = Functions.GetFunction(typeof(AggregateFunctions<>).FullName, nameof(AggregateFunctions<int>.Min), BuiltInAssembly).GetTransformFunction(typeof(DateTime)); 
            var maxFunction = Functions.GetFunction(typeof(AggregateFunctions<>).FullName, nameof(AggregateFunctions<int>.Max), BuiltInAssembly).GetTransformFunction(typeof(DateTime)); 

            var baseDate = DateTime.Now;

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 0; i <= 10; i++)
                {
                    var minFunctionResult = minFunction.RunFunction(new object[] { baseDate.AddDays(i) }, CancellationToken.None);
                    var maxFunctionResult = maxFunction.RunFunction(new object[] { baseDate.AddDays(i) }, CancellationToken.None);
                }

                var minResult = minFunction.RunResult(null, out _, CancellationToken.None).returnValue;
                var maxResult = maxFunction.RunResult(null, out _, CancellationToken.None).returnValue;
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
            var globalVariables = new GlobalSettings("abc");
            const string value = "encrypt this";
            var function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.FastEncrypt), BuiltInAssembly).GetTransformFunction(typeof(string), null, globalVariables);
            var encrypted = function.RunFunction(new object[] {value}, CancellationToken.None).returnValue;

            function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.FastDecrypt), BuiltInAssembly).GetTransformFunction(typeof(string),null , globalVariables);
            var decrypted = function.RunFunction(new[] {encrypted}, CancellationToken.None).returnValue;

            Assert.Equal(value, decrypted);
        }

        [Fact]
        public void StrongEncryptTest()
        {
            var globalVariables = new GlobalSettings("abc");
            const string value = "encrypt this";
            var function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.StrongEncrypt), BuiltInAssembly).GetTransformFunction(typeof(string), null, globalVariables);
            var encrypted = function.RunFunction(new object[] {value}, CancellationToken.None).returnValue;

            function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.StrongDecrypt), BuiltInAssembly).GetTransformFunction(typeof(string), null, globalVariables);
            var decrypted = function.RunFunction(new[] {encrypted}, CancellationToken.None).returnValue;

            Assert.Equal(value, decrypted);
        }
        
        [Fact]
        public void EncryptTest()
        {
            const string value = "encrypt this";
            const string key = "abc";
            const int iterations = 10;
            var function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.Encrypt), BuiltInAssembly).GetTransformFunction(typeof(string));
            var encrypted = function.RunFunction(new object[] {value, key, iterations}, CancellationToken.None).returnValue;

            function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.Decrypt), BuiltInAssembly).GetTransformFunction(typeof(string));
            var decrypted = function.RunFunction(new [] {encrypted, key, iterations}, CancellationToken.None).returnValue;

            Assert.Equal(value, decrypted);
        }
        
        [Fact]
        public void HashTest()
        {
            const string value = "hash this";
            var function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.SecureHash), BuiltInAssembly).GetTransformFunction(typeof(string));
            var hashed = function.RunFunction(new object[] {value}, CancellationToken.None).returnValue;

            function = Functions.GetFunction(typeof(SecurityFunctions).FullName, nameof(SecurityFunctions.ValidateSecureHash), BuiltInAssembly).GetTransformFunction(typeof(string));
            var passed = (bool)function.RunFunction(new object[] {value, hashed}, CancellationToken.None).returnValue;

            Assert.True(passed);
            
            //check hash fails with different value.
            passed = (bool) function.RunFunction(new object[] {"hash thiS", hashed}, CancellationToken.None).returnValue;
            Assert.False(passed);
        }
        
        [Fact]
        public void SHA1Test()
        {
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.CreateSHA1), BuiltInAssembly).GetTransformFunction(typeof(string));
            var sha1 = function.RunFunction(new object[] {"sha this"}, CancellationToken.None);
            var sha1a = function.RunFunction(new [] {"sha this"}, CancellationToken.None);

            // check same value hashed the same.
            Assert.Equal(sha1, sha1a);

            var sha1b = function.RunFunction(new [] {"sha thiS"}, CancellationToken.None);

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
            var function = Functions.GetFunction(typeof(AggregateFunctions<int>).FullName, nameof(AggregateFunctions<int>.Count), BuiltInAssembly).GetTransformFunction(typeof(int)); 

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    var functionResult = function.RunFunction(new FunctionVariables(),new object[] { }, CancellationToken.None);
                }

                var aggregateResult = function.RunResult(new FunctionVariables(), null, out _, CancellationToken.None).returnValue;
                Assert.NotNull(aggregateResult);
                Assert.Equal(10, aggregateResult);

                function.Reset();
            }
        }

        [Fact]
        public void ConcatAggTest()
        {
            var function = Functions.GetFunction(typeof(AggregateFunctions<string>).FullName, nameof(AggregateFunctions<string>.ConcatAgg), BuiltInAssembly).GetTransformFunction(typeof(string)); 

            for (var a = 0; a < 2; a++) // run all tests twice to ensure reset functions are working
            {
                for (var i = 1; i <= 10; i++)
                {
                    var functionResult = function.RunFunction(new FunctionVariables(),new object[] { i.ToString(), "," }, CancellationToken.None);
                }

                var aggregateResult =  function.RunResult(new FunctionVariables(), null, out _, CancellationToken.None).returnValue;
                Assert.NotNull(aggregateResult);
                Assert.Equal("1,2,3,4,5,6,7,8,9,10", aggregateResult);

                function.Reset();
            }

        }

        [Fact]
        public void Function_XPathValue()
        {
            //Get a rows that exists.
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.XPathValues), BuiltInAssembly).GetTransformFunction(typeof(XmlDocument));
            var xmlDoc = Operations.Parse<XmlDocument>("<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>");
            var param = new object[] { xmlDoc, new [] { "//row[1]", "//row[2]", "//row[3]"} };
            Assert.True((bool)function.RunFunction(param, out var outputs, CancellationToken.None).returnValue);
            var result = (object[]) outputs[0];;
            Assert.Equal("0", result[0]);
            Assert.Equal("1", result[1]);
            Assert.Equal("2", result[2]);
        }

        [Fact]
        public void Function_JSONValue()
        {
            //Get a rows that exists.
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.JsonValues), BuiltInAssembly).GetTransformFunction(typeof(JsonElement));
            var json = "{ \"value1\": \"1\", \"value2\" : \"2\", \"value3\": \"3\", \"array\" : {\"v1\" : \"1\", \"v2\" : \"2\"} }";
            var param = new object[] { json, new [] { "value1", "value2", "value3", "array", "badvalue" }};
            
            Assert.False((bool)function.RunFunction(new FunctionVariables(), param, out var outputs, CancellationToken.None).returnValue);
            var result = (object[])outputs[0];
            Assert.Equal("1", result[0]);
            Assert.Equal("2", result[1]);
            Assert.Equal("3", result[2]);
            Assert.Null(result[4]);

            //get the sub Json string, and run another parse over this.
            var moreValues = result[3];
            param = new object[] { moreValues, new [] { "v1", "v2"} };
            function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.JsonValues), BuiltInAssembly).GetTransformFunction(typeof(JsonElement));
            Assert.True((bool)function.RunFunction(param, out outputs, CancellationToken.None).returnValue);
            result = (object[]) outputs[0];;
            Assert.Equal("1", result[0]);
            Assert.Equal("2", result[1]);
        }


        [Fact]
        public void RowFunctions_GenerateSequence()
        {
            //Use a for loop to simulate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.GenerateSequence), BuiltInAssembly).GetTransformFunction(typeof(int));
            var param = new object[] { 0, 10, 2 };
            for (var i = 0; i <= 10; i += 2)
            {
                Assert.True((bool)function.RunFunction(param, out var outputs, CancellationToken.None).returnValue);
                Assert.Equal(i, (int)outputs[0]);
            }
            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.RunFunction(param, CancellationToken.None).returnValue);
        }

        [Fact]
        public void RowFunctions_SplitColumnToRows()
        {
            //Use a for loop to simulate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.SplitColumnToRows), BuiltInAssembly).GetTransformFunction(typeof(string));
            var param = new object[] { "|", "|value2|value3||value5||", 6 };
            var compare = new[] { "", "value2", "value3", "", "value5", "", "" };
            for (var i = 0; i < 6; i++)
            {
                Assert.True((bool)function.RunFunction(param, out var outputs, CancellationToken.None).returnValue);
                Assert.Equal(compare[i], (string)outputs[0]);
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.RunFunction(param, CancellationToken.None).returnValue);
        }

        [Fact]
        public void RowFunctions_XPathNodesToRows()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.XPathNodesToRows), BuiltInAssembly).GetTransformFunction(typeof(XmlDocument));
            var xmlDoc = Operations.Parse<XmlDocument>("<root><row>0</row><row>1</row><row>2</row><row>3</row><row>4</row><row>5</row></root>");
            var param = new object[] { xmlDoc, "//row", 5 };
            for (var i = 0; i < 5; i++)
            {
                Assert.True((bool)function.RunFunction(param, out var outputs, CancellationToken.None).returnValue);
                Assert.Equal(i.ToString(), (string)outputs[0]);
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.RunFunction(param, CancellationToken.None).returnValue);
        }

        [Fact]
        public void RowFunctions_JsonElementsToRows()
        {
            //Use a for loop to similate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.JsonElementsToRows), BuiltInAssembly).GetTransformFunction(typeof(JToken));
            var json = "{\"results\" : [{\"value1\" : \"r1v1\", \"value2\" : \"r1v2\"}, {\"value1\" : \"r2v1\", \"value2\" : \"r2v2\"}]} ";
            var param = new object[] { json , "results[*]", 2 };
            for (var i = 1; i <= 2; i++)
            {
                Assert.True((bool)function.RunFunction(param, out var outputs, CancellationToken.None).returnValue);
                var jsonResult = (string)outputs[0];
                var results = JObject.Parse(jsonResult);
                Assert.Equal("r" + i.ToString() + "v1", results.SelectToken("value1").ToString());
                Assert.Equal("r" + i.ToString() + "v2", results.SelectToken("value2").ToString());
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.RunFunction(param, CancellationToken.None).returnValue);
        }
        
        [Fact]
        public void RowFunctions_JsonPivotElementToRows()
        {
            //Use a for loop to simulate gen sequence.
            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.JsonPivotElementToRows), BuiltInAssembly).GetTransformFunction(typeof(JToken));
            var json = "{\"results\" : {\"name1\" : \"value1\", \"name2\" : \"value2\", \"name3\" : \"value3\"}} ";
            var param = new object[] { json, "results", 3 };
            for (var i = 1; i <= 3; i++)
            {
                Assert.True((bool)function.RunFunction(param, out var outputs, CancellationToken.None).returnValue);
                var name = (string)outputs[0];
                var value = (string)outputs[1];
                Assert.Equal(name, "name" + i.ToString());
                Assert.Equal(value, "value" + i.ToString());
            }

            //last value should be false as the sequence has been exceeded.
            Assert.False((bool)function.RunFunction(param, CancellationToken.None).returnValue);
        }
        
        [Fact]
        public void GroupFunction_ParentChildFlatten()
        {
            var data = new[]
            {
                new object[] {"EMP1", "MGR1"},
                new object[] {"EMP2", "MGR1"},
                new object[] {"EMP3", "MGR2"},
                new object[] {"MGR2", "MGR1"},
                new object[] {"EMP4", "MGR2"},
                new object[] {"EMP5", "EMP4"},
                new object[] {"EMP6", "EMP5"},
            };

            var expected = new[]
            {
                new object[] {"MGR1", 0, new object[] { "MGR1", null, null, null, null}},
                new object[] {"EMP1", 1, new object[] { "MGR1", "EMP1", null, null, null}},
                new object[] {"EMP2", 1, new object[] { "MGR1", "EMP2", null, null, null}},
                new object[] {"EMP3", 2, new object[] { "MGR1", "MGR2", "EMP3", null, null}},
                new object[] {"MGR2", 1, new object[] { "MGR1", "MGR2", null, null, null}},
                new object[] {"EMP4", 2, new object[] { "MGR1", "MGR2", "EMP4", null, null}},
                new object[] {"EMP5", 3, new object[] { "MGR1", "MGR2", "EMP4", "EMP5", null}},
                new object[] {"EMP6", 4, new object[] { "MGR1", "MGR2", "EMP4", "EMP5", "EMP6"}},
            };

            // run function for each data row
            var function = Functions.GetFunction(typeof(HierarchyFunctions).FullName, nameof(HierarchyFunctions.FlattenParentChild), BuiltInAssembly).GetTransformFunction(typeof(string));
            foreach (var row in data)
            {
                function.RunFunction(row, out _, CancellationToken.None);
            }

            // run the result function to get flattened dataset.
            var pos = 0;
            for (var i = 0; i < data.Length; i++)
            {
                while ((bool)function.RunResult(new FunctionVariables() {Index = i}, new object[] {4}, out object[] outputs, CancellationToken.None).returnValue)
                {


                    // first value if the leaf value
                    Assert.Equal(expected[pos][0], outputs[0]);

                    // second value is the number of levels to the top
                    Assert.Equal(expected[pos][1], outputs[1]);

                    // third value contains the flattened array.
                    var expectedHierarchy = (object[]) expected[pos][2];
                    var actualHierarchy = (object[]) outputs[2];

                    Assert.Equal(expectedHierarchy, actualHierarchy);

                    pos++;
                }
            }
        }
    }
}