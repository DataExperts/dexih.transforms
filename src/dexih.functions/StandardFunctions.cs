using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using Newtonsoft.Json.Linq;
using System.Reflection;

namespace dexih.functions
{
    public class StandardFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        int? _cacheInt;
        double? _cacheDouble;
        DateTime? _cacheDate;
        string _cacheString;
        Dictionary<string, string> _cacheStringDictionary;
        Dictionary<string, Int32> _cacheIntDictionary;
        List<object> _cacheList;
        string[] _cacheArray;
        List<KeyValuePair<DateTime, double>> _cacheSeriesList;
        StringBuilder _cacheStringBuilder;
        XPathNodeIterator _cacheXmlNodeList;
        JToken[] _cacheJsonTokens;

        const string NullPlaceHolder = "A096F007-26EE-479E-A9E1-4E12427A5AF0"; //used a a unique string that can be substituted for null

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
            return true;
        }

        public static Function GetFunctionReference(string FunctionName, string[] inputMappings, string targetColumn, string[] outputMappings)
        {
            return new Function(typeof(StandardFunctions), FunctionName, FunctionName + "Result", "Reset", inputMappings, targetColumn, outputMappings);
        }

        public static Function GetFunctionReference(string FunctionName)
        {
            if (typeof(StandardFunctions).GetMethod(FunctionName) == null)
                throw new Exception("The method " + FunctionName + " was not found in the standard functions");
            return new Function(typeof(StandardFunctions), FunctionName, FunctionName + "Result", "Reset", null, null, null);
        }

        #region Regular Functions
        public String Concat(String[] values) { return String.Concat(values); }
        public Int32 IndexOf(String value, String search) { return value.IndexOf(search, StringComparison.Ordinal); }
        public String Insert(String value, Int32 startIndex, String insertString) { return value.Insert(startIndex, insertString); }
        public String Join(String separator, String[] values) { return String.Join(separator, values); }
        public String PadLeft(String value, Int32 width, String paddingChar) { return value.PadLeft(width, paddingChar[0]); }
        public String PadRight(String value, Int32 width, String paddingChar) { return value.PadRight(width, paddingChar[0]); }
        public String Remove(String value, Int32 startIndex, Int32 count) { return value.Remove(startIndex, count); }

        public String Replace(String value, String oldValue, String newValue)
        {
            if (String.IsNullOrEmpty(value))
                return null;

            if (String.IsNullOrEmpty(oldValue))
                return value;

            if (newValue == null)
                newValue = "";

            return value.Replace(oldValue, newValue);
        }

        public Int32 Split(String value, String separator, Int32 count, out string[] result)
        {
            if(String.IsNullOrEmpty(value))
            {
                result = null;
                return 0;
            }
            result = value.Split(separator.ToCharArray(), count);
            return result.Length;
        }
        public String Substring(String stringValue, Int32 start, Int32 length)
        {
            int stringLength = stringValue.Length;
            if (start > stringLength) return "";
            if (start + length > stringLength) return stringValue.Substring(start);
            return stringValue.Substring(start, length);
        }
        public String ToLower(String value) { return value.ToLower(); }
        public String ToUpper(String value) { return value.ToUpper(); }
        public String Trim(String value) { return value.Trim(); }
        public String TrimEnd(String value) { return value.TrimEnd(); }
        public String TrimStart(String value) { return value.TrimStart(); }
        public Int32 Length(String value) { return value.Length; }
        public Int32 WordCount(String value)
        {
            if (string.IsNullOrEmpty(value))
                return 0;

            int c = 0;
            for (int i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i])) c++;
            if (value.Length > 2) c++;
            return c;
        }
        public String WordExtract(String value, Int32 wordNumber)
        {
            if (string.IsNullOrEmpty(value))
                return null;

            int start = 0; int c = 0; int i;
            for (i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i]))
                    { c++; if (c == wordNumber) start = i; else if (c > wordNumber) break; }
            if (value.Length > 2) c++;
            return c == 0 || c <= wordNumber ? "" : value.Substring(start, i - start);
        }
        public Boolean LessThan(Double value, Double compare) { return value < compare; }
        public Boolean LessThanEqual(Double value, Double compare) { return value <= compare; }
        public Boolean GreaterThan(Double value, Double compare) { return value > compare; }
        public Boolean GreaterThanEqual(Double value, Double compare) { return value >= compare; }
        public Boolean IsEqual(String[] values)
        {
            for(int i = 1; i< values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }
        public Boolean IsNumericEqual(Decimal[] values)
        {
            for (int i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }
        public Boolean IsDateTimeEqual(DateTime[] values)
        {
            for (int i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }
        public Boolean IsBooleanEqual(Boolean[] values)
        {
            for (int i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }

        public Boolean IsNumber(String value)
        {
            Decimal result;
            return Decimal.TryParse(value, out result);
        }
        public Boolean ToDate(String value, out DateTime result)
        {
            if (string.IsNullOrEmpty(value))
            {
                result = DateTime.MinValue;
                return false;
            }
            return DateTime.TryParse(value, out result);
        }
        public Boolean IsNull(String value) { return value == null;  }
        public Boolean IsBetween(Double value, Double lowRange, Double highRange) { return value > lowRange && value < highRange; }
        public Boolean IsBetweenInclusive(Double value, Double lowRange, Double highRange) { return value >= lowRange && value <= highRange; }
        public Boolean RegexMatch(String input, String pattern) { return Regex.Match(input, pattern).Success; }
        public Boolean Contains(String value, String contains) { return value.Contains(contains); }
        public Boolean EndsWith(String value, String endsWith) { return value.EndsWith(endsWith); }
        public Boolean StartsWith(String value, String startsWith) { return value.StartsWith(startsWith); }
        public Boolean IsUpper(String value, Boolean skipNonAlpha)
        {
            foreach (char t in value)
            {
                if ((skipNonAlpha && Char.IsLetter(t) && !Char.IsUpper(t)) || (skipNonAlpha == false && !Char.IsUpper(t)))
                    return false;
            }
            return true;
        }

        public Boolean IsLower(String value, Boolean skipNonAlpha)
        {
            foreach (char t in value)
            {
                if ((skipNonAlpha && Char.IsLetter(t) && !Char.IsLower(t)) || (skipNonAlpha == false && !Char.IsLower(t)))
                    return false;
            }
            return true;
        }

        public Boolean IsAlpha(String value)
        {
            foreach (char t in value)
            {
                if (!Char.IsLetter(t))
                    return false;
            }
            return true;
        }

        public Boolean IsAlphaNumeric(String value)
        {
            foreach (char t in value)
            {
                if (!Char.IsLetter(t) && !Char.IsNumber(t))
                    return false;
            }
            return true;
        }

        public Boolean IsPattern(String value, String pattern)
        {
            if (value.Length != pattern.Length) return false;
            for (int i = 0; i < pattern.Length; i++)
            {
                if ((pattern[i] == '9' && !Char.IsNumber(value[i])) ||
                   (pattern[i] == 'A' && !Char.IsUpper(value[i])) ||
                   (pattern[i] == 'a' && !Char.IsLower(value[i])) ||
                   (pattern[i] == 'Z' && !Char.IsLetter(value[i])))
                    return false;
            }
            return true;
        }
        public DateTime AddDays(DateTime dateValue, Double addValue) { return dateValue.AddDays(addValue); }
        public DateTime AddHours(DateTime dateValue, Double addValue) { return dateValue.AddHours(addValue); }
        public DateTime AddMilliseconds(DateTime dateValue, Double addValue) { return dateValue.AddMilliseconds(addValue); }
        public DateTime AddMinutes(DateTime dateValue, Double addValue) { return dateValue.AddMinutes(addValue); }
        public DateTime AddMonths(DateTime dateValue, Int32 addValue) { return dateValue.AddMonths(addValue); }
        public DateTime AddSeconds(DateTime dateValue, Double addValue) { return dateValue.AddSeconds(addValue); }
        public DateTime AddYears(DateTime dateValue, Int32 addValue) { return dateValue.AddYears(addValue); }
        public Int32 DaysInMonth(DateTime dateValue) { return DateTime.DaysInMonth(dateValue.Year, dateValue.Month); }
        public Boolean IsDaylightSavingTime(DateTime dateValue) { return TimeZoneInfo.Local.IsDaylightSavingTime(dateValue); }
        public Boolean IsLeapYear(DateTime dateValue) { return DateTime.IsLeapYear(dateValue.Year); }
        public Boolean IsWeekend(DateTime dateValue) { return dateValue.DayOfWeek == DayOfWeek.Saturday || dateValue.DayOfWeek == DayOfWeek.Sunday; }
        public Boolean IsWeekDay(DateTime dateValue) { return !(dateValue.DayOfWeek == DayOfWeek.Saturday || dateValue.DayOfWeek == DayOfWeek.Sunday); }
        public Int32 DayOfMonth(DateTime dateValue) { return dateValue.Day; }
        public String DayOfWeekName(DateTime dateValue) { return dateValue.DayOfWeek.ToString(); }
        public Int32 DayOfWeekNumber(DateTime dateValue) { return (int)dateValue.DayOfWeek; }
        public Int32 WeekOfYear(DateTime dateValue)
        {
            DateTimeFormatInfo dfi = DateTimeFormatInfo.CurrentInfo;
            Calendar cal = dfi.Calendar;
            return cal.GetWeekOfYear(dateValue, dfi.CalendarWeekRule, dfi.FirstDayOfWeek);
        }
        public Int32 DayOfYear(DateTime dateValue) { return dateValue.DayOfYear; }
        public Int32 Month(DateTime dateValue) { return dateValue.Month; }
        public String ShortMonth(DateTime dateValue) { return CultureInfo.CurrentCulture.DateTimeFormat.GetAbbreviatedMonthName(dateValue.Month); }
        public String LongMonth(DateTime dateValue) { return CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(dateValue.Month); }
        public Int32 Year(DateTime dateValue) { return dateValue.Year; }
        public string ToLongDateString(DateTime dateValue) { return dateValue.ToString("dddd, dd MMMM yyyy"); } // .ToLongDateString(); } 
        public string ToLongTimeString(DateTime dateValue) { return dateValue.ToString("h:mm:ss tt"); } // .ToLongTimeString(); } 
        public String ToShortDateString(DateTime dateValue) { return dateValue.ToString("d/MM/yyyy"); }// ToShortDateString(); } 
        public String ToShortTimeString(DateTime dateValue) { return dateValue.ToString("h:mm tt"); } // .ToShortTimeString(); } 
        public String DateToString(DateTime dateValue, String format) { return dateValue.ToString(format); }
        public DateTime DateNow() { return DateTime.Now; }
        public DateTime DateNowUtc() { return DateTime.UtcNow; }
        public String Encrypt(String value, String key) { return EncryptString.Encrypt(value, key, 1000).Value; }
        public String Decrypt(String value, String key) { return EncryptString.Decrypt(value, key, 1000).Value; }
        public String CreateSaltedHash(String value) { return HashString.CreateHash(value); }
        public Boolean ValidateSaltedHash(String value, String hash) { return HashString.ValidateHash(value, hash); }

        public Double Abs(Double value) { return Math.Abs(value); }
        public Double Acos(Double value) { return Math.Acos(value); }
        public Double Asin(Double value) { return Math.Asin(value); }
        public Double Atan(Double value) { return Math.Atan(value); }
        public Double Atan2(Double x, Double y) { return Math.Atan2(x, y); }
        public Double Cos(Double value) { return Math.Cos(value); }
        public Double Cosh(Double value) { return Math.Cosh(value); }

        public Int32 DivRem(Int32 dividend, Int32 divisor, out int remainder)
        {
            //return Math.DivRem(dividend, divisor, out remainder); Not working in DNX50
            int quotient = dividend / divisor;
            remainder = dividend - (divisor * quotient);
            return quotient;
        }
        public Double Exp(Double value) { return Math.Exp(value); }
        public Double IeeeRemainder(Double x, Double y) { return Math.IEEERemainder(x, y); }
        public Double Log(Double value) { return Math.Log(value); }
        public Double Log10(Double value) { return Math.Log10(value); }
        public Double Pow(Double x, Double y) { return Math.Pow(x, y); }
        public Double Round(Double value) { return Math.Round(value); }
        public Double Sign(Double value) { return Math.Sign(value); }
        public Double Sin(Double value) { return Math.Sin(value); }
        public Double Sinh(Double value) { return Math.Sinh(value); }
        public Double Sqrt(Double value) { return Math.Sqrt(value); }
        public Double Tan(Double value) { return Math.Tan(value); }
        public Double Tanh(Double value) { return Math.Tanh(value); }
        public Double Truncate(Double value) { return Math.Truncate(value); }
        public Decimal Add(Decimal value1, Decimal value2) { return value1 + value2; }
        public Decimal Ceiling(Decimal value) { return Decimal.Ceiling(value); }
        public Decimal Divide(Decimal value1, Decimal value2) { return value1 / value2; }
        public Decimal Floor(Decimal value) { return Decimal.Floor(value); }
        public Decimal Multiply(Decimal value1, Decimal value2) { return value1 * value2; }
        public Decimal Negate(Decimal value) { return value * -1; }
        public Decimal Remainder(Decimal value1, Decimal value2) { return Decimal.Remainder(value1, value2); }
        public Decimal Subtract(Decimal value1, Decimal value2) { return value1 - value2; }
        public Boolean IsIn(String value, String[] compareTo)
        {
            bool returnValue = false;
            foreach (string t in compareTo)
                returnValue = returnValue | value == t;
            return returnValue;
        }
        #endregion

        #region Geographical
        public double GetDistanceTo(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude)
        {
            double rlat1 = Math.PI * fromLatitude / 180;
            double rlat2 = Math.PI * toLatitude / 180;
            double theta = fromLongitude - toLongitude;
            double rtheta = Math.PI * theta / 180;
            double dist =
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
            bool returnValue = true;

            try
            {
                //XmlDocument xmlDoc = new XmlDocument();
                //xmlDoc.LoadXml(xml);

                var stream = new StringReader(xml);
                XPathDocument xPathDocument = new XPathDocument(stream);
                XPathNavigator xPathNavigator = xPathDocument.CreateNavigator();

                values = new string[xPaths.Length];

                for (int i = 0; i < xPaths.Length; i++)
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
                bool returnValue = true;

                var results = JObject.Parse(json);

                values = new string[jsonPaths.Length];

                for (int i = 0; i < jsonPaths.Length; i++)
                {
                    JToken token = results.SelectToken(jsonPaths[i]);
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

        public Double SumResult(int index)
        {
            if (_cacheDouble == null)
                return 0;

            return (Double)_cacheDouble;
        }
        public void Average(Double value)
        {
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
            _cacheInt = _cacheInt + 1;
        }
        public Double AverageResult(int index)
        {
            if (_cacheDouble == null || _cacheInt == null || _cacheInt == 0)
                return 0;

            return (Double)_cacheDouble / (Double)_cacheInt;
        }

        public void Median(Double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            _cacheList.Add(value);
        }
        public Double MedianResult(int index)
        {
            if (_cacheList == null)
                return 0;
            object[] sorted = _cacheList.OrderBy(c => (Double)c).ToArray();
            int count = sorted.Length;

            if (count % 2 == 0)
            {
                // count is even, average two middle elements
                Double a = (double)sorted[count / 2 - 1];
                Double b = (double)sorted[count / 2];
                return (a + b) / 2;
            }
            // count is odd, return the middle element
            return (double)sorted[count / 2];
        }

        //variance and stdev share same input formula
        public void Variance(Double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;

            _cacheList.Add(value);
            _cacheInt++;
            _cacheDouble += value;
        }

        public Double VarianceResult(int index)
        {
            if (_cacheList == null || _cacheInt == null || _cacheInt == 0 || _cacheDouble == null || _cacheDouble == 0 )
                return 0;

            double average = (Double)_cacheDouble / (Double)_cacheInt;
            double sumOfSquaresOfDifferences = _cacheList.Select(val => ((double)val - average) * ((Double)val - average)).Sum();
            double sd = sumOfSquaresOfDifferences / (Double)_cacheInt;

            return sd;
        }

        public void StdDev(Double value)
        {
            Variance(value);
        }

        public Double StdDevResult(int index)
        {
            double sd = Math.Sqrt(VarianceResult(index));
            return sd;
        }

        public void Min(Double value)
        {
            if (_cacheDouble == null) _cacheDouble = value;
            else if (value < _cacheDouble) _cacheDouble = value;
        }

        public Double? MinResult(int index)
        {
            return _cacheDouble;
        }

        public void Max(Double value)
        {
            if (_cacheDouble == null) _cacheDouble = value;
            else if (value > _cacheDouble) _cacheDouble = value;
        }
        public Double? MaxResult(int index)
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

        public void First(String value)
        {
            if (_cacheString == null) _cacheString = value;
        }

        public String FirstResult(int index)
        {
            return _cacheString;
        }

        public void Last(String value)
        {
            _cacheString = value;
        }
        public String LastResult(int index)
        {
            return _cacheString;
        }

        public void Count()
        {
            if (_cacheInt == null) _cacheInt = 1;
            else _cacheInt = _cacheInt + 1;
        }

        public Int32 CountResult(int index)
        {
            if (_cacheInt == null)
                return 0;

            return (int)_cacheInt;
        }

        public void CountDistinct(String value)
        {
            if (_cacheStringDictionary == null) _cacheStringDictionary = new Dictionary<string, string>();
            if (value == null) value = NullPlaceHolder; //dictionary can't use nulls, so substitute null values.
            if (_cacheStringDictionary.ContainsKey(value) == false)
                _cacheStringDictionary.Add(value, null);
        }
        public Int32 CountDistinctResult(int index)
        {
            return _cacheStringDictionary.Keys.Count;
        }
        public void ConcatAgg(String value, String delimiter)
        {
            if (_cacheStringBuilder == null)
            {
                _cacheStringBuilder = new StringBuilder();
                _cacheStringBuilder.Append(value);
            }
            else
                _cacheStringBuilder.Append(delimiter + value);
        }
        public String ConcatAggResult(int index)
        {
            return _cacheStringBuilder.ToString();
        }

        public void FirstWhen(String condition, String conditionValue, String resultValue)
        {
            if (condition == conditionValue && _cacheString == null)
                _cacheString = resultValue;
        }

        public String FirstWhenResult(int index)
        {
            return _cacheString;
        }

        public void LastWhen(String condition, String conditionValue, String resultValue)
        {
            if (condition == conditionValue)
                _cacheString = resultValue;
        }

        public String LastWhenResult(int index)
        {
            return _cacheString;
        }

        #endregion

        #region Series Functions
        public void MovingAverage(DateTime series, Double value, int preCount, int postCount)
        {
            if (_cacheSeriesList == null)
            {
                _cacheIntDictionary = new Dictionary<string, int> {{"PreCount", preCount}, {"PostCount", preCount}};
                _cacheSeriesList = new List<KeyValuePair<DateTime, double>>();
            }
            _cacheSeriesList.Add(new KeyValuePair<DateTime, double>(series, value));
        }

        public Double MovingAverageResult(int index)
        {
            DateTime lowDate = _cacheSeriesList[index].Key.AddDays(-_cacheIntDictionary["PreCount"]);
            DateTime highDate = _cacheSeriesList[index].Key.AddDays(_cacheIntDictionary["PostCount"]);
            int valueCount = _cacheSeriesList.Count;

            double sum = 0;
            int denominator = 0;

            //loop backwards from the index to sum the before items.
            for (int i = index; i >= 0; i--)
            {
                if (_cacheSeriesList[i].Key < lowDate)
                    break;
                sum += _cacheSeriesList[i].Value;
                denominator++;
            }

            //loop forwards from the index+1 to sum the after items.
            for (int i = index + 1; i < valueCount; i++)
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

        public void HighestSince(DateTime series, Double value)
        {
            if (_cacheSeriesList == null)
            {
                _cacheSeriesList = new List<KeyValuePair<DateTime, double>>();
            }
            _cacheSeriesList.Add(new KeyValuePair<DateTime, double>(series, value));
        }

        public DateTime HighestSinceResult(int index, out Double value)
        {
            int i = index - 1;
            Double currentValue = _cacheSeriesList[index].Value;
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

            if (_cacheInt > maxItems || _cacheInt > _cacheArray.Length - 1)
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
                XPathDocument xPathDocument = new XPathDocument(stream);

                _cacheXmlNodeList = xPathDocument.CreateNavigator().Select(xPath);
                _cacheInt = 0;
            }

            if (_cacheInt > maxItems || _cacheXmlNodeList.MoveNext() == false)
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
                var results = JObject.Parse(json);
                _cacheJsonTokens = results.SelectTokens(jsonPath).ToArray();
                _cacheInt = 0;
            }
            else
                _cacheInt++;

            if (_cacheInt > maxItems - 1 || _cacheInt > _cacheJsonTokens.Length - 1)
            {
                item = "";
                return false;
            }
            item = _cacheJsonTokens[(int)_cacheInt].ToString();
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
            else
            {
                trimmedValue = null;
                return true;
            }
        }

        public bool MaxValue(decimal value, decimal maxValue, out decimal adjustedValue)
        {
            if (value > maxValue)
            {
                adjustedValue = maxValue;
                return false;
            }
            else
            {
                adjustedValue = value;
                return true;
            }
        }

        #endregion
    }
}

 