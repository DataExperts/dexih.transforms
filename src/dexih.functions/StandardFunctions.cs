using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using CsvHelper;
using Newtonsoft.Json.Linq;
using Dexih.Utils.Crypto;
using Newtonsoft.Json;

namespace dexih.functions
{
    public class StandardFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private int? _cacheInt;

        private double? _cacheDouble;
        private DateTime? _cacheDate;
        private string _cacheString;
        private Dictionary<string, string> _cacheStringDictionary;
        private Dictionary<string, int> _cacheIntDictionary;
        private List<object> _cacheList;
        private string[] _cacheArray;
        private List<KeyValuePair<DateTime, double>> _cacheSeriesList;
        private StringBuilder _cacheStringBuilder;
        private XPathNodeIterator _cacheXmlNodeList;
        private JToken[] _cacheJsonTokens;

        private const string NullPlaceHolder = "A096F007-26EE-479E-A9E1-4E12427A5AF0"; //used a a unique string that can be substituted for null

        public bool Reset()
        {
            _cacheInt = null;
            _cacheDouble = null;
            _cacheString = null;
            _cacheStringDictionary = null;
            _cacheList = null;
            _cacheStringDictionary = null;
            _cacheArray = null;
            _cacheSeriesList = null;
            _cacheStringBuilder = null;
            _cacheXmlNodeList = null;
            _cacheJsonTokens = null;
            return true;
        }

        public static Function GetFunctionReference(string functionName, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
        {
            return new Function(typeof(StandardFunctions), functionName, functionName + "Result", "Reset", inputMappings, targetColumn, outputMappings);
        }

        public static Function GetFunctionReference(string functionName)
        {
            if (typeof(StandardFunctions).GetMethod(functionName) == null)
                throw new Exception("The method " + functionName + " was not found in the standard functions");
            return new Function(typeof(StandardFunctions), functionName, functionName + "Result", "Reset", null, null, null);
        }

        #region Regular Functions
        public string Concat(string[] values) { return string.Concat(values); }
        public int IndexOf(string value, string search) { return value.IndexOf(search, StringComparison.Ordinal); }
        public string Insert(string value, int startIndex, string insertString) { return value.Insert(startIndex, insertString); }
        public string Join(string separator, string[] values) { return string.Join(separator, values); }
        public string PadLeft(string value, int width, string paddingChar) { return value.PadLeft(width, paddingChar[0]); }
        public string PadRight(string value, int width, string paddingChar) { return value.PadRight(width, paddingChar[0]); }
        public string Remove(string value, int startIndex, int count) { return value.Remove(startIndex, count); }

        public string Replace(string value, string oldValue, string newValue)
        {
            if (string.IsNullOrEmpty(value))
                return null;

            if (string.IsNullOrEmpty(oldValue))
                return value;

            if (newValue == null)
                newValue = "";

            return value.Replace(oldValue, newValue);
        }

        public int Split(string value, string separator, int count, out string[] result)
        {
            if(string.IsNullOrEmpty(value))
            {
                result = null;
                return 0;
            }
            result = value.Split(separator.ToCharArray(), count);
            return result.Length;
        }
        public string Substring(string stringValue, int start, int length)
        {
            var stringLength = stringValue.Length;
            if (start > stringLength) return "";
            if (start + length > stringLength) return stringValue.Substring(start);
            return stringValue.Substring(start, length);
        }
        public string ToLower(string value) { return value.ToLower(); }
        public string ToUpper(string value) { return value.ToUpper(); }
        public string Trim(string value) { return value.Trim(); }
        public string TrimEnd(string value) { return value.TrimEnd(); }
        public string TrimStart(string value) { return value.TrimStart(); }
        public int Length(string value) { return value.Length; }
        public int WordCount(string value)
        {
            if (string.IsNullOrEmpty(value))
                return 0;

            var c = 0;
            for (var i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i])) c++;
            if (value.Length > 2) c++;
            return c;
        }
        public string WordExtract(string value, int wordNumber)
        {
            if (string.IsNullOrEmpty(value))
                return null;

            var start = 0; var c = 0; int i;
            for (i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i]))
                    { c++; if (c == wordNumber) start = i; else if (c > wordNumber) break; }
            if (value.Length > 2) c++;
            return c == 0 || c <= wordNumber ? "" : value.Substring(start, i - start);
        }
        public bool LessThan(double value, double compare) { return value < compare; }
        public bool LessThanEqual(double value, double compare) { return value <= compare; }
        public bool GreaterThan(double value, double compare) { return value > compare; }
        public bool GreaterThanEqual(double value, double compare) { return value >= compare; }
        public bool IsEqual(string[] values)
        {
            for(var i = 1; i< values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }
        public bool IsNumericEqual(decimal[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }
        public bool IsDateTimeEqual(DateTime[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }
        public bool IsBooleanEqual(bool[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }

        public bool IsTrue(bool value) => value;


        public bool IsNumber(string value)
        {
            decimal result;
            return decimal.TryParse(value, out result);
        }
        public bool ToDate(string value, out DateTime result)
        {
            if (string.IsNullOrEmpty(value))
            {
                result = DateTime.MinValue;
                return false;
            }
            return DateTime.TryParse(value, out result);
        }

        public DateTime UnixTimeStampToDate(long unixTimeStamp)
        {
            var origDate = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            var convertedDate = origDate.AddSeconds(unixTimeStamp).ToLocalTime();
            return convertedDate; 
        }

        public bool IsNull(string value) { return value == null;  }
        public bool IsBetween(double value, double lowRange, double highRange) { return value > lowRange && value < highRange; }
        public bool IsBetweenInclusive(double value, double lowRange, double highRange) { return value >= lowRange && value <= highRange; }
        public bool IsDateBetween(DateTime value, DateTime lowRange, DateTime highRange) { return value > lowRange && value < highRange; }
        public bool IsDateBetweenInclusive(DateTime value, DateTime lowRange, DateTime highRange) { return value >= lowRange && value <= highRange; }
        public bool IsDateBetweenNow(DateTime lowRange, DateTime highRange) { return DateTime.Now > lowRange && DateTime.Now < highRange; }
        public bool IsDateBetweenInclusiveNow(DateTime lowRange, DateTime highRange) { return DateTime.Now >= lowRange && DateTime.Now <= highRange; }
        public bool RegexMatch(string input, string pattern) { return Regex.Match(input, pattern).Success; }
        public bool Contains(string value, string contains) { return value.Contains(contains); }
        public bool EndsWith(string value, string endsWith) { return value.EndsWith(endsWith); }
        public bool StartsWith(string value, string startsWith) { return value.StartsWith(startsWith); }
        public bool IsUpper(string value, bool skipNonAlpha)
        {
            foreach (var t in value)
            {
                if ((skipNonAlpha && char.IsLetter(t) && !char.IsUpper(t)) || (skipNonAlpha == false && !char.IsUpper(t)))
                    return false;
            }
            return true;
        }

        public bool IsLower(string value, bool skipNonAlpha)
        {
            foreach (var t in value)
            {
                if ((skipNonAlpha && char.IsLetter(t) && !char.IsLower(t)) || (skipNonAlpha == false && !char.IsLower(t)))
                    return false;
            }
            return true;
        }

        public bool IsAlpha(string value)
        {
            foreach (var t in value)
            {
                if (!char.IsLetter(t))
                    return false;
            }
            return true;
        }

        public bool IsAlphaNumeric(string value)
        {
            foreach (var t in value)
            {
                if (!char.IsLetter(t) && !char.IsNumber(t))
                    return false;
            }
            return true;
        }

        public bool IsPattern(string value, string pattern)
        {
            if (value.Length != pattern.Length) return false;
            for (var i = 0; i < pattern.Length; i++)
            {
                if ((pattern[i] == '9' && !char.IsNumber(value[i])) ||
                   (pattern[i] == 'A' && !char.IsUpper(value[i])) ||
                   (pattern[i] == 'a' && !char.IsLower(value[i])) ||
                   (pattern[i] == 'Z' && !char.IsLetter(value[i])))
                    return false;
            }
            return true;
        }
        public DateTime AddDays(DateTime dateValue, double addValue) { return dateValue.AddDays(addValue); }
        public DateTime AddHours(DateTime dateValue, double addValue) { return dateValue.AddHours(addValue); }
        public DateTime AddMilliseconds(DateTime dateValue, double addValue) { return dateValue.AddMilliseconds(addValue); }
        public DateTime AddMinutes(DateTime dateValue, double addValue) { return dateValue.AddMinutes(addValue); }
        public DateTime AddMonths(DateTime dateValue, int addValue) { return dateValue.AddMonths(addValue); }
        public DateTime AddSeconds(DateTime dateValue, double addValue) { return dateValue.AddSeconds(addValue); }
        public DateTime AddYears(DateTime dateValue, int addValue) { return dateValue.AddYears(addValue); }
        public int DaysInMonth(DateTime dateValue) { return DateTime.DaysInMonth(dateValue.Year, dateValue.Month); }
        public bool IsDaylightSavingTime(DateTime dateValue) { return TimeZoneInfo.Local.IsDaylightSavingTime(dateValue); }
        public bool IsLeapYear(DateTime dateValue) { return DateTime.IsLeapYear(dateValue.Year); }
        public bool IsWeekend(DateTime dateValue) { return dateValue.DayOfWeek == DayOfWeek.Saturday || dateValue.DayOfWeek == DayOfWeek.Sunday; }
        public bool IsWeekDay(DateTime dateValue) { return !(dateValue.DayOfWeek == DayOfWeek.Saturday || dateValue.DayOfWeek == DayOfWeek.Sunday); }
        public int DayOfMonth(DateTime dateValue) { return dateValue.Day; }
        public string DayOfWeekName(DateTime dateValue) { return dateValue.DayOfWeek.ToString(); }
        public int DayOfWeekNumber(DateTime dateValue) { return (int)dateValue.DayOfWeek; }
        public int WeekOfYear(DateTime dateValue)
        {
            var dfi = DateTimeFormatInfo.CurrentInfo;
            var cal = dfi.Calendar;
            return cal.GetWeekOfYear(dateValue, dfi.CalendarWeekRule, dfi.FirstDayOfWeek);
        }
        public int DayOfYear(DateTime dateValue) { return dateValue.DayOfYear; }
        public int Month(DateTime dateValue) { return dateValue.Month; }
        public string ShortMonth(DateTime dateValue) { return CultureInfo.CurrentCulture.DateTimeFormat.GetAbbreviatedMonthName(dateValue.Month); }
        public string LongMonth(DateTime dateValue) { return CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(dateValue.Month); }
        public int Year(DateTime dateValue) { return dateValue.Year; }
        public string ToLongDateString(DateTime dateValue) { return dateValue.ToString("dddd, dd MMMM yyyy"); } // .ToLongDateString(); } 
        public string ToLongTimeString(DateTime dateValue) { return dateValue.ToString("h:mm:ss tt").ToUpper(); } // .ToLongTimeString(); } 
        public string ToShortDateString(DateTime dateValue) { return dateValue.ToString("d/MM/yyyy"); }// ToShortDateString(); } 
        public string ToShortTimeString(DateTime dateValue) { return dateValue.ToString("h:mm tt").ToUpper(); } // .ToShortTimeString(); } 
        public string DateToString(DateTime dateValue, string format) { return dateValue.ToString(format); }
        public DateTime DateNow() { return DateTime.Now; }
        public DateTime DateNowUtc() { return DateTime.UtcNow; }

        public int AgeInYears(DateTime dateValue) { return AgeInYearsAtDate(dateValue, DateTime.Today);}

        public int AgeInYearsAtDate(DateTime startDate, DateTime endDate)
        {
            var age = endDate.Year - startDate.Year;
            if (startDate > endDate.AddYears(-age)) age--;
            return age;
        }
        
        public string Encrypt(string value, string key) { return EncryptString.Encrypt(value, key, 1000); }
        public string Decrypt(string value, string key) { return EncryptString.Decrypt(value, key, 1000); }
        public string CreateSaltedHash(string value) { return HashString.CreateHash(value); }
        public bool ValidateSaltedHash(string value, string hash) { return HashString.ValidateHash(value, hash); }

        public double Abs(double value) { return Math.Abs(value); }
        public double Acos(double value) { return Math.Acos(value); }
        public double Asin(double value) { return Math.Asin(value); }
        public double Atan(double value) { return Math.Atan(value); }
        public double Atan2(double x, double y) { return Math.Atan2(x, y); }
        public double Cos(double value) { return Math.Cos(value); }
        public double Cosh(double value) { return Math.Cosh(value); }

        public int DivRem(int dividend, int divisor, out int remainder)
        {
            //return Math.DivRem(dividend, divisor, out remainder); Not working in DNX50
            var quotient = dividend / divisor;
            remainder = dividend - (divisor * quotient);
            return quotient;
        }
        public double Exp(double value) { return Math.Exp(value); }
        public double IeeeRemainder(double x, double y) { return Math.IEEERemainder(x, y); }
        public double Log(double value) { return Math.Log(value); }
        public double Log10(double value) { return Math.Log10(value); }
        public double Pow(double x, double y) { return Math.Pow(x, y); }
        public double Round(double value) { return Math.Round(value); }
        public double Sign(double value) { return Math.Sign(value); }
        public double Sin(double value) { return Math.Sin(value); }
        public double Sinh(double value) { return Math.Sinh(value); }
        public double Sqrt(double value) { return Math.Sqrt(value); }
        public double Tan(double value) { return Math.Tan(value); }
        public double Tanh(double value) { return Math.Tanh(value); }
        public double Truncate(double value) { return Math.Truncate(value); }
        public decimal Add(decimal value1, decimal value2) { return value1 + value2; }
        public decimal Ceiling(decimal value) { return decimal.Ceiling(value); }
        public decimal Divide(decimal value1, decimal value2) { return value1 / value2; }
        public decimal Floor(decimal value) { return decimal.Floor(value); }
        public decimal Multiply(decimal value1, decimal value2) { return value1 * value2; }
        public decimal Negate(decimal value) { return value * -1; }
        public decimal Remainder(decimal value1, decimal value2) { return decimal.Remainder(value1, value2); }
        public decimal Subtract(decimal value1, decimal value2) { return value1 - value2; }
        public bool IsIn(string value, string[] compareTo)
        {
            var returnValue = false;
            foreach (var t in compareTo)
                returnValue = returnValue | value == t;
            return returnValue;
        }
        #endregion

        #region Geographical
        public double GetDistanceTo(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude)
        {
            var rlat1 = Math.PI * fromLatitude / 180;
            var rlat2 = Math.PI * toLatitude / 180;
            var theta = fromLongitude - toLongitude;
            var rtheta = Math.PI * theta / 180;
            var dist =
                Math.Sin(rlat1) * Math.Sin(rlat2) + Math.Cos(rlat1) *
                Math.Cos(rlat2) * Math.Cos(rtheta);
            dist = Math.Acos(dist);
            dist = dist * 180 / Math.PI;
            dist = dist * 60 * 1853.159616F;

            return dist;
        }

        #endregion

        #region Parsing Functions
        public bool XPathValues(string xml, string[] xPaths, out string[] values)
        {
            var returnValue = true;

            try
            {
                //XmlDocument xmlDoc = new XmlDocument();
                //xmlDoc.LoadXml(xml);

                var stream = new StringReader(xml);
                var xPathDocument = new XPathDocument(stream);
                var xPathNavigator = xPathDocument.CreateNavigator();

                values = new string[xPaths.Length];

                for (var i = 0; i < xPaths.Length; i++)
                {
                    //XmlNode xmlNode = xmlDoc.SelectSingleNode(xPaths[i]);
                    var xmlNode = xPathNavigator.SelectSingleNode(xPaths[i]);

                    //((XmlElement) xmlNode)?.RemoveAttribute("xmlns");

                    if (xmlNode == null)
                    {
                        returnValue = false;
                        values[i] = null;
                    }
                    else
                    {
                        values[i] = xmlNode.InnerXml;
                    }
                }

                return returnValue;
            }
            catch
            {
                values = new string[xPaths.Length];
                return false;
            }
        }

        public bool JsonValues(string json, string[] jsonPaths, out string[] values)
        {
            try
            {
                var returnValue = true;

                var results = JToken.Parse(json);

                values = new string[jsonPaths.Length];

                for (var i = 0; i < jsonPaths.Length; i++)
                {
                    var token = results.SelectToken(jsonPaths[i]);
                    if (token == null)
                    {
                        returnValue = false;
                        values[i] = null;
                    }
                    else
                    {
                        values[i] = token.ToString();
                    }
                }

                return returnValue;
            }
            catch
            {
                values = new string[jsonPaths.Length];
                return false;
            }
        } 
        #endregion

        #region Aggregation Functions
        public void Sum(double value)
        {
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
        }

        public double SumResult(int index)
        {
            if (_cacheDouble == null)
                return 0;

            return (double)_cacheDouble;
        }
        public void Average(double value)
        {
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
            _cacheInt = _cacheInt + 1;
        }
        public double AverageResult(int index)
        {
            if (_cacheDouble == null || _cacheInt == null || _cacheInt == 0)
                return 0;

            return (double)_cacheDouble / (double)_cacheInt;
        }

        public void Median(double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            _cacheList.Add(value);
        }
        public double MedianResult(int index)
        {
            if (_cacheList == null)
                return 0;
            var sorted = _cacheList.OrderBy(c => (double)c).ToArray();
            var count = sorted.Length;

            if (count % 2 == 0)
            {
                // count is even, average two middle elements
                var a = (double)sorted[count / 2 - 1];
                var b = (double)sorted[count / 2];
                return (a + b) / 2;
            }
            // count is odd, return the middle element
            return (double)sorted[count / 2];
        }

        //variance and stdev share same input formula
        public void Variance(double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;

            _cacheList.Add(value);
            _cacheInt++;
            _cacheDouble += value;
        }

        public double VarianceResult(int index)
        {
            if (_cacheList == null || _cacheInt == null || _cacheInt == 0 || _cacheDouble == null || _cacheDouble == 0 )
                return 0;

            var average = (double)_cacheDouble / (double)_cacheInt;
            var sumOfSquaresOfDifferences = _cacheList.Select(val => ((double)val - average) * ((double)val - average)).Sum();
            var sd = sumOfSquaresOfDifferences / (double)_cacheInt;

            return sd;
        }

        public void StdDev(double value)
        {
            Variance(value);
        }

        public double StdDevResult(int index)
        {
            var sd = Math.Sqrt(VarianceResult(index));
            return sd;
        }

        public void Min(double value)
        {
            if (_cacheDouble == null) _cacheDouble = value;
            else if (value < _cacheDouble) _cacheDouble = value;
        }

        public double? MinResult(int index)
        {
            return _cacheDouble;
        }

        public void Max(double value)
        {
            if (_cacheDouble == null) _cacheDouble = value;
            else if (value > _cacheDouble) _cacheDouble = value;
        }
        public double? MaxResult(int index)
        {
            return _cacheDouble;
        }

        public void MinDate(DateTime value)
        {
            if (_cacheDate == null) _cacheDate = value;
            else if (value < _cacheDate) _cacheDate = value;
        }

        public DateTime? MinDateResult(int index)
        {
            return _cacheDate;
        }

        public void MaxDate(DateTime value)
        {
            if (_cacheDate == null) _cacheDate = value;
            else if (value > _cacheDate) _cacheDate = value;
        }
        public DateTime? MaxDateResult(int index)
        {
            return _cacheDate;
        }

        public void First(string value)
        {
            if (_cacheString == null) _cacheString = value;
        }

        public string FirstResult(int index)
        {
            return _cacheString;
        }

        public void Last(string value)
        {
            _cacheString = value;
        }
        public string LastResult(int index)
        {
            return _cacheString;
        }

        public void Count()
        {
            if (_cacheInt == null) _cacheInt = 1;
            else _cacheInt = _cacheInt + 1;
        }

        public int CountResult(int index)
        {
            if (_cacheInt == null)
                return 0;

            return (int)_cacheInt;
        }

        public void CountDistinct(string value)
        {
            if (_cacheStringDictionary == null) _cacheStringDictionary = new Dictionary<string, string>();
            if (value == null) value = NullPlaceHolder; //dictionary can't use nulls, so substitute null values.
            if (_cacheStringDictionary.ContainsKey(value) == false)
                _cacheStringDictionary.Add(value, null);
        }
        public int CountDistinctResult(int index)
        {
            return _cacheStringDictionary.Keys.Count;
        }
        public void ConcatAgg(string value, string delimiter)
        {
            if (_cacheStringBuilder == null)
            {
                _cacheStringBuilder = new StringBuilder();
                _cacheStringBuilder.Append(value);
            }
            else
                _cacheStringBuilder.Append(delimiter + value);
        }
        public string ConcatAggResult(int index)
        {
            return _cacheStringBuilder.ToString();
        }

        public void FirstWhen(string condition, string conditionValue, string resultValue)
        {
            if (condition == conditionValue && _cacheString == null)
                _cacheString = resultValue;
        }

        public string FirstWhenResult(int index)
        {
            return _cacheString;
        }

        public void LastWhen(string condition, string conditionValue, string resultValue)
        {
            if (condition == conditionValue)
                _cacheString = resultValue;
        }

        public string LastWhenResult(int index)
        {
            return _cacheString;
        }

        #endregion

        #region Series Functions
        public void MovingAverage(DateTime series, double value, int preCount, int postCount)
        {
            if (_cacheSeriesList == null)
            {
                _cacheIntDictionary = new Dictionary<string, int> {{"PreCount", preCount}, {"PostCount", preCount}};
                _cacheSeriesList = new List<KeyValuePair<DateTime, double>>();
            }
            _cacheSeriesList.Add(new KeyValuePair<DateTime, double>(series, value));
        }

        public double MovingAverageResult(int index)
        {
            var lowDate = _cacheSeriesList[index].Key.AddDays(-_cacheIntDictionary["PreCount"]);
            var highDate = _cacheSeriesList[index].Key.AddDays(_cacheIntDictionary["PostCount"]);
            var valueCount = _cacheSeriesList.Count;

            double sum = 0;
            var denominator = 0;

            //loop backwards from the index to sum the before items.
            for (var i = index; i >= 0; i--)
            {
                if (_cacheSeriesList[i].Key < lowDate)
                    break;
                sum += _cacheSeriesList[i].Value;
                denominator++;
            }

            //loop forwards from the index+1 to sum the after items.
            for (var i = index + 1; i < valueCount; i++)
            {
                if (_cacheSeriesList[i].Key > highDate)
                    break;
                sum += _cacheSeriesList[i].Value;
                denominator++;
            }

            //return the result.
            if (denominator == 0)
                return 0;
            return sum / denominator;
        }

        public void HighestSince(DateTime series, double value)
        {
            if (_cacheSeriesList == null)
            {
                _cacheSeriesList = new List<KeyValuePair<DateTime, double>>();
            }
            _cacheSeriesList.Add(new KeyValuePair<DateTime, double>(series, value));
        }

        public DateTime HighestSinceResult(int index, out double value)
        {
            var i = index - 1;
            var currentValue = _cacheSeriesList[index].Value;
            while (i > 0)
            {
                if (_cacheSeriesList[i].Value > currentValue)
                {
                    value = _cacheSeriesList[i].Value;
                    return _cacheSeriesList[i].Key;
                }
                i--;
            }
            value = _cacheSeriesList[index].Value;
            return _cacheSeriesList[index].Key;
        }
        #endregion

        #region Row Functions
        public bool GenerateSequence(int start, int end, int step, out int sequence)
        {
            if (_cacheInt == null)
                _cacheInt = start;

            sequence = (int)_cacheInt;
            _cacheInt = _cacheInt + step;

            if (sequence > end)
                return false;
            return true;
        }

        public bool GenerateDateSequence(DateTime start, DateTime end, int step, out DateTime sequence)
        {
            if (_cacheDate == null)
                _cacheDate = start;

            sequence = (DateTime)_cacheDate;
            _cacheDate = _cacheDate.Value.AddDays(step);

            if (sequence > end)
                return false;
            return true;
        }

        public bool SplitColumnToRows(string separator, string value, int maxItems, out string item)
        {
            if (_cacheArray == null)
            {
                _cacheArray = value.Split(separator.ToCharArray(), maxItems + 1); 
                _cacheInt = 0;
            }
            else
                _cacheInt++;

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheInt > _cacheArray.Length - 1)
            {
                item = "";
                return false;
            }
            item = _cacheArray[(int)_cacheInt];
            return true;
        }

        public bool XPathNodesToRows(string xml, string xPath, int maxItems, out string node)
        {
            if (_cacheXmlNodeList == null)
            {
                // NOT WORKING IN DNX50
                //XmlDocument xmlDoc = new XmlDocument();
                //xmlDoc.LoadXml(xml);
                //_cacheXmlNodeList = xmlDoc.SelectNodes(xPath);

                var stream = new StringReader(xml);
                var xPathDocument = new XPathDocument(stream);

                _cacheXmlNodeList = xPathDocument.CreateNavigator().Select(xPath);
                _cacheInt = 0;
            }

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheXmlNodeList.MoveNext() == false)
            {
                node = "";
                return false;
            }

            _cacheInt++;
            node = _cacheXmlNodeList.Current.InnerXml;
            return true;
        }

        public bool JsonElementsToRows(string json, string jsonPath, int maxItems, out string item)
        {
            if (_cacheJsonTokens == null)
            {
                var results = JToken.Parse(json);
                _cacheJsonTokens = string.IsNullOrEmpty(jsonPath) ? results.ToArray() : results.SelectTokens(jsonPath).ToArray();
                _cacheInt = 0;
            }
            else
                _cacheInt++;

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheInt > _cacheJsonTokens.Length - 1)
            {
                item = "";
                return false;
            }
            item = _cacheJsonTokens[(int)_cacheInt].ToString();
            return true;
        }

        public bool JsonPivotElementToRows(string json, string jsonPath, int maxItems, out string name, out string value)
        {
            if (_cacheJsonTokens == null)
            {
                var results = JToken.Parse(json);

                _cacheJsonTokens = string.IsNullOrEmpty(jsonPath) ? results.SelectTokens(" ").ToArray() : results.SelectTokens(jsonPath).ToArray();

                _cacheInt = 0;
            }
            else
            {
                _cacheInt++;
            }

            var item = _cacheJsonTokens == null || _cacheJsonTokens.Length ==0 ? null : _cacheJsonTokens[0].ElementAtOrDefault((int) _cacheInt);
            if ((maxItems > 0 && _cacheInt > maxItems - 1) || item == null)
            {
                name = "";
                value = "";
                return false;
            }

            var property = (JProperty) item;
            name = property.Name;
            value = item.Values().FirstOrDefault()?.ToString();
            return true;
        }
        
        #endregion

        #region Validation Functions

        public bool MaxLength(string value, int maxLength, out string trimmedValue)
        {
            if (value.Length > maxLength)
            {
                trimmedValue = value.Substring(0, maxLength);
                return false;
            }
            trimmedValue = null;
            return true;
        }

        public bool MaxValue(decimal value, decimal maxValue, out decimal adjustedValue)
        {
            if (value > maxValue)
            {
                adjustedValue = maxValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }

        public bool DefaultNullString(string value, string defaultValue, out string adjustedValue)
        {
            if (value == null)
            {
                adjustedValue = defaultValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }

        public bool DefaultBlankString(string value, string defaultValue, out string adjustedValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                adjustedValue = defaultValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }

        public bool DefaultNullNumber(decimal? value, decimal defaultValue, out decimal adjustedValue)
        {
            if (value == null)
            {
                adjustedValue = defaultValue;
                return false;
            }
            adjustedValue = (decimal)value;
            return true;
        }

        #endregion
    }
}

 