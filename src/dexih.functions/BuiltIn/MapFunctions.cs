using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Xml.XPath;
using Dexih.Utils.Crypto;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace dexih.functions.BuiltIn
{
    /// <summary>
    /// 
    /// </summary>

    public class MapFunctions
    {
        public GlobalVariables GlobalVariables { get; set; }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Concatenate",
            Description = "Concatenates multiple string fields.")]
        public string Concat([TransformFunctionParameter(Description = "Array of Values to Concatenate")] string[] values)
        {
            return string.Concat(values);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Index Of",
            Description = "The zero-based index of the first occurrence of the specified string in this field.")]
        public int IndexOf(string value, string search)
        {
            return value.IndexOf(search, StringComparison.Ordinal);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Insert",
            Description =
                "Returns a new string in which a specified string is inserted at a specified index position in this instance.")]
        public string Insert(string value, int startIndex, string insertString)
        {
            return value.Insert(startIndex, insertString);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Join",
            Description =
                "Concatenates all the elements of a string array, using the specified separator between each element.")]
        public string Join(string separator, string[] values)
        {
            return string.Join(separator, values);
        }


        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Pad Left",
            Description =
                "Returns a new string that right-aligns the characters in this instance by padding them with spaces on the left, for a specified total length.")]
        public string PadLeft(string value, int width, string paddingChar)
        {
            return value.PadLeft(width, paddingChar[0]);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Pad Right",
            Description =
                "Returns a new string that left-aligns the characters in this string by padding them with spaces on the right, for a specified total length.")]
        public string PadRight(string value, int width, string paddingChar)
        {
            return value.PadRight(width, paddingChar[0]);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Remove",
            Description =
                "Returns a new string in which a specified number of characters in the current instance beginning at a specified position have been deleted.")]
        public string Remove(string value, int startIndex, int count)
        {
            return value.Remove(startIndex, count);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Replace",
            Description =
                "Returns a new string in which all occurrences of a specified string in the current instance are replaced with another specified string.")]
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Split",
            Description = "Splits a string into multiple return fields that are based on the characters in an array.")]
        public int Split(string value, string separator, int count, out string[] result)
        {
            if (string.IsNullOrEmpty(value))
            {
                result = null;
                return 0;
            }

            result = value.Split(separator.ToCharArray(), count);
            return result.Length;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Substring",
            Description =
                "Retrieves a substring from this instance. The substring starts at a specified character position and has a specified length.")]
        public string Substring(string stringValue, int start, int length)
        {
            var stringLength = stringValue.Length;
            if (start > stringLength) return "";
            if (start + length > stringLength) return stringValue.Substring(start);
            return stringValue.Substring(start, length);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "To Lowercase",
            Description = "Returns a copy of this string converted to lowercase.")]
        public string ToLower(string value)
        {
            return value.ToLower();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "To Uppercase",
            Description = "Returns a copy of this string converted to uppercase.")]
        public string ToUpper(string value)
        {
            return value.ToUpper();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Trim",
            Description = "Removes all leading and trailing white-space characters from the current field.")]
        public string Trim(string value)
        {
            return value.Trim();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Trim End",
            Description =
                "Removes all leading and trailing occurrences of a set of characters specified in an array from the current field.")]
        public string TrimEnd(string value)
        {
            return value.TrimEnd();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Trim Start",
            Description =
                "Removes all trailing occurrences of a set of characters specified in an array from the current field.")]
        public string TrimStart(string value)
        {
            return value.TrimStart();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Length",
            Description = "Return the length of the string.")]
        public int Length(string value)
        {
            return value.Length;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "WordCount",
            Description = "Returns the number of words in the string.")]
        public int WordCount(string value)
        {
            if (string.IsNullOrEmpty(value))
                return 0;

            var c = 0;
            for (var i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i]))
                        c++;
            if (value.Length > 2) c++;
            return c;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "WordExtract",
            Description = "Returns the nth word (starting from 0) in the string.")]
        public string WordExtract(string value, int wordNumber)
        {
            if (string.IsNullOrEmpty(value))
                return null;

            var start = 0;
            var c = 0;
            int i;
            for (i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i]))
                    {
                        c++;
                        if (c == wordNumber) start = i;
                        else if (c > wordNumber)
                            break;
                    }

            if (value.Length > 2) c++;
            return c == 0 || c <= wordNumber ? "" : value.Substring(start, i - start);
        }


 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Days",
            Description =
                "Returns a new DateTime that adds the specified number of days to the value of this instance.")]
        public DateTime AddDays(DateTime dateValue, double addValue)
        {
            return dateValue.AddDays(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Hours",
            Description =
                "Returns a new DateTime that adds the specified number of hours to the value of this instance.")]
        public DateTime AddHours(DateTime dateValue, double addValue)
        {
            return dateValue.AddHours(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Milliseconds",
            Description =
                "Returns a new DateTime that adds the specified number of milliseconds to the value of this instance.")]
        public DateTime AddMilliseconds(DateTime dateValue, double addValue)
        {
            return dateValue.AddMilliseconds(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Minutes",
            Description =
                "Returns a new DateTime that adds the specified number of minutes to the value of this instance.")]
        public DateTime AddMinutes(DateTime dateValue, double addValue)
        {
            return dateValue.AddMinutes(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Months",
            Description =
                "Returns a new DateTime that adds the specified number of months to the value of this instance.")]
        public DateTime AddMonths(DateTime dateValue, int addValue)
        {
            return dateValue.AddMonths(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Seconds",
            Description =
                "Returns a new DateTime that adds the specified number of seconds to the value of this instance.")]
        public DateTime AddSeconds(DateTime dateValue, double addValue)
        {
            return dateValue.AddSeconds(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "AddYears",
            Description =
                "Returns a new DateTime that adds the specified number of years to the value of this instance.")]
        public DateTime AddYears(DateTime dateValue, int addValue)
        {
            return dateValue.AddYears(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "DaysInMonth",
            Description = "Returns the number of days in the specified month and year.")]
        public int DaysInMonth(DateTime dateValue)
        {
            return DateTime.DaysInMonth(dateValue.Year, dateValue.Month);
        }


        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Day of the Month",
            Description = "The day number of the month")]
        public int DayOfMonth(DateTime dateValue)
        {
            return dateValue.Day;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Day of the Week Name",
            Description = "The name of the day of the week (e.g. Monday).")]
        public string DayOfWeekName(DateTime dateValue)
        {
            return dateValue.DayOfWeek.ToString();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Day of the Week Number",
            Description = "The number of the day of the week (Sunday=0 - Saturday=6).")]
        public int DayOfWeekNumber(DateTime dateValue)
        {
            return (int) dateValue.DayOfWeek;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Week of the Year",
            Description = "The week number of the year.")]
        public int WeekOfYear(DateTime dateValue)
        {
            var dfi = DateTimeFormatInfo.CurrentInfo;
            var cal = dfi.Calendar;
            return cal.GetWeekOfYear(dateValue, dfi.CalendarWeekRule, dfi.FirstDayOfWeek);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Day of the Year",
            Description = "The day number of the year.")]
        public int DayOfYear(DateTime dateValue)
        {
            return dateValue.DayOfYear;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Month ",
            Description = "The month number of the year (1-12)")]
        public int Month(DateTime dateValue)
        {
            return dateValue.Month;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "ShortMonth",
            Description = "A three letter value of the month (e.g. Jan, Feb, Mar).")]
        public string ShortMonth(DateTime dateValue)
        {
            return CultureInfo.CurrentCulture.DateTimeFormat.GetAbbreviatedMonthName(dateValue.Month);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "LongMonth",
            Description = "The full name of the month.")]
        public string LongMonth(DateTime dateValue)
        {
            return CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(dateValue.Month);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Year",
            Description = "The year")]
        public int Year(DateTime dateValue)
        {
            return dateValue.Year;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Date Only",
            Description = "Extract Date Only from a date/time field")]
        public DateTime DateOnly(DateTime dateValue)
        {
            return dateValue.Date;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Time Only",
            Description = "Extract Time from a date/time field")]
        public TimeSpan TimeOnly(DateTime dateValue)
        {
            return dateValue.TimeOfDay;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "To Long Date String",
            Description =
                "Converts the value of the current DateTime object to its equivalent long date string representation.")]
        public string ToLongDateString(DateTime dateValue)
        {
            return dateValue.ToString("dddd, dd MMMM yyyy");
        } // .ToLongDateString(); } 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "To Long Time String",
            Description =
                "Converts the value of the current DateTime object to its equivalent long time string representation.")]
        public string ToLongTimeString(DateTime dateValue)
        {
            return dateValue.ToString("h:mm:ss tt").ToUpper();
        } // .ToLongTimeString(); } 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "To Short Date String",
            Description =
                "Converts the value of the current DateTime object to its equivalent short date string representation.")]
        public string ToShortDateString(DateTime dateValue)
        {
            return dateValue.ToString("d/MM/yyyy");
        } // ToShortDateString(); } 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "To Short Time String",
            Description =
                "Converts the value of the current DateTime object to its equivalent short time string representation.")]
        public string ToShortTimeString(DateTime dateValue)
        {
            return dateValue.ToString("h:mm tt").ToUpper();
        } // .ToShortTimeString(); } 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Date To String",
            Description =
                "Converts the value of the current DateTime object to its equivalent string representation using the formatting conventions of the current culture.(OverridesValueType.ToString().)")]
        public string DateToString(DateTime dateValue, string format)
        {
            return dateValue.ToString(format);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Date/Time Now",
            Description = "The local date time")]
        public DateTime DateTimeNow()
        {
            return DateTime.Now;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Date/Time Now UTC",
            Description = "The current Universal Coordinated Time (UCT/GMT) (no time component). ")]
        public DateTime DateTimeNowUtc()
        {
            return DateTime.UtcNow.Date;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Date Now", Description = "The local date (no time component)")]
        public DateTime DateNow()
        {
            return DateTime.Now.Date;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Date Now UTC",
            Description = "The current Universal Coordinated Time (UCT/GMT). ")]
        public DateTime DateNowUtc()
        {
            return DateTime.UtcNow;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Manual Encrypt",
            Description = "Encrypts the string using the key string.  More iterations = stronger/slower")]
        public string Encrypt(string value, string key, int iterations)
        {
            return EncryptString.Encrypt(value, key, iterations);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Manual Decrypt",
            Description = "Decrypts the string using the key string and iteractions.  More iterations = stronger/slower encrypt.")]
        public string Decrypt(string value, string key, int iterations)
        {
            return EncryptString.Decrypt(value, key, iterations);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Strong Encrypt",
            Description = "Strong Encrypts the string.")]
        public string StrongEncrypt(string value)
        {
            return EncryptString.Encrypt(value, GlobalVariables?.EncryptionKey, 1000);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Strong Decrypt",
            Description = "Strong Decrypts the string.")]
        public string StrongDecrypt(string value)
        {
            return EncryptString.Decrypt(value, GlobalVariables?.EncryptionKey, 1000);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Fast Encrypt",
            Description = "Fast Encrypts the string.")]
        public string FastEncrypt(string value)
        {
            return EncryptString.Encrypt(value, GlobalVariables?.EncryptionKey, 5);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Fast Decrypt",
            Description = "Fast Decrypts the string.")]
        public string FastDecrypt(string value)
        {
            return EncryptString.Decrypt(value, GlobalVariables?.EncryptionKey, 5);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Secure Hash",
            Description =
                "Creates a random-salted, SHA256 hash of the string.  This is secure and can be used for passwords and other sensative data.  This can only be validated using the Validate Hash function.")]
        public string SecureHash(string value)
        {
            return HashString.CreateHash(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Validate Secure Hash",
            Description = "Validates a value created from the Secure Hash function.")]
        public bool ValidateSecureHash(string value, string hash)
        {
            return HashString.ValidateHash(value, hash);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Abs",
            Description = "Returns the absolute value of a Decimal number.")]
        public double Abs(double value)
        {
            return Math.Abs(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Acos",
            Description = "Returns the angle whose cosine is the specified number.")]
        public double Acos(double value)
        {
            return Math.Acos(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Asin",
            Description = "Returns the angle whose sine is the specified number.")]
        public double Asin(double value)
        {
            return Math.Asin(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Atan",
            Description = "Returns the angle whose tangent is the specified number.")]
        public double Atan(double value)
        {
            return Math.Atan(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Atan2",
            Description = "Returns the angle whose tangent is the quotient of two specified numbers.")]
        public double Atan2(double x, double y)
        {
            return Math.Atan2(x, y);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Cos",
            Description = "Returns the cosine of the specified angle.")]
        public double Cos(double value)
        {
            return Math.Cos(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Cosh",
            Description = "Returns the hyperbolic cosine of the specified angle.")]
        public double Cosh(double value)
        {
            return Math.Cosh(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "DivRem",
            Description =
                "Calculates the quotient of two 32-bit signed integers and also returns the remainder in an output parameter.")]
        public int DivRem(int dividend, int divisor, out int remainder)
        {
            //return Math.DivRem(dividend, divisor, out remainder); Not working in DNX50
            var quotient = dividend / divisor;
            remainder = dividend - (divisor * quotient);
            return quotient;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Exp",
            Description = "Returns e raised to the specified power.")]
        public double Exp(double value)
        {
            return Math.Exp(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "IEEERemainder",
            Description =
                "Returns the remainder resulting from the division of a specified number by another specified number.")]
        public double IeeeRemainder(double x, double y)
        {
            return Math.IEEERemainder(x, y);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Log",
            Description = "Returns the natural (base e) logarithm of a specified number.")]
        public double Log(double value)
        {
            return Math.Log(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Log10",
            Description = "Returns the base 10 logarithm of a specified number.")]
        public double Log10(double value)
        {
            return Math.Log10(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Pow",
            Description = "Returns a specified number raised to the specified power.")]
        public double Pow(double x, double y)
        {
            return Math.Pow(x, y);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Round",
            Description = "Rounds a decimal value to the nearest integral value.")]
        public double Round(double value)
        {
            return Math.Round(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sign",
            Description = "Returns a value indicating the sign of a decimal number.")]
        public double Sign(double value)
        {
            return Math.Sign(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sin",
            Description = "Returns the sine of the specified angle.")]
        public double Sin(double value)
        {
            return Math.Sin(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sinh",
            Description = "Returns the hyperbolic sine of the specified angle.")]
        public double Sinh(double value)
        {
            return Math.Sinh(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sqrt",
            Description = "Returns the square root of a specified number.")]
        public double Sqrt(double value)
        {
            return Math.Sqrt(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Tan",
            Description = "Returns the tangent of the specified angle.")]
        public double Tan(double value)
        {
            return Math.Tan(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Tanh",
            Description = "Returns the hyperbolic tangent of the specified angle.")]
        public double Tanh(double value)
        {
            return Math.Tanh(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Truncate",
            Description = "Calculates the integral part of a specified decimal number.")]
        public double Truncate(double value)
        {
            return Math.Truncate(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Add",
            Description = "Adds two or more specified Decimal values.")]
        public decimal Add(decimal value1, decimal[] value2)
        {
            return value1 + value2.Sum();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Ceiling",
            Description =
                "Returns the smallest integral value that is greater than or equal to the specified decimal number.")]
        public decimal Ceiling(decimal value)
        {
            return decimal.Ceiling(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Divide",
            Description = "Divides two specified Decimal values.")]
        public decimal Divide(decimal value1, decimal value2)
        {
            return value1 / value2;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Floor",
            Description = "Rounds a specified Decimal number to the closest integer toward negative infinity.")]
        public decimal Floor(decimal value)
        {
            return decimal.Floor(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Multiply",
            Description = "Multiplies two or more specified Decimal values.")]
        public decimal Multiply(decimal value1, decimal[] value2)
        {
            var returnValue = value1;
            foreach(var value in value2)
            {
                returnValue *= value;
            }
            return returnValue;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Negate",
            Description = "Returns the result of multiplying the specifiedDecimal value by negative one.")]
        public decimal Negate(decimal value)
        {
            return value * -1;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Remainder",
            Description = "Computes the remainder after dividing two Decimal values.")]
        public decimal Remainder(decimal value1, decimal value2)
        {
            return decimal.Remainder(value1, value2);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Subtract",
            Description = "Subtracts one or more specified Decimal values from another.")]
        public decimal Subtract(decimal value1, decimal[] value2)
        {
            return value1 - value2.Sum();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Xml", Name = "XPathValues",
            Description = "Parses an xml string into a series of xpath results.")]
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

        [TransformFunction(
            FunctionType = EFunctionType.Map, 
            Category = "JSON", 
            Name = "JSONValues",
            Description = "Parses a JSON string into a series of elements.  The JSON string must contain only one result set.",
            ImportMethod = nameof(JsonValuesImport))
        ]
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

        public string[] JsonValuesImport(string json)
        {
            var result = JToken.Parse(json);
            var values = JsonValueImportRecurse(result);
            return values.ToArray();
        }

        private List<string> JsonValueImportRecurse(JToken jToken)
        {
            var values = new List<string>();
            
            foreach (var child in jToken.Children())
            {
                if (child.Type == JTokenType.Object || child.Type == JTokenType.Property)
                {
                    values.AddRange((JsonValueImportRecurse(child)));
                }
                else
                {
                    values.Add(child.Path);
                }
            }

            return values;
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, 
            Category = "JSON", 
            Name = "JsonArrayToColumns",
            Description = "Pivots a json array into column values. ",
            ImportMethod = nameof(JsonArrayToColumnsImport))
        ]
        public bool JsonArrayToColumns(string json, string jsonPath, string columnPath, string valuePath, string[] columns, out string[] values)
        {
            try
            {
                var results = JToken.Parse(json);

                JToken array;
                if (string.IsNullOrEmpty(jsonPath))
                {
                    array = results;
                }
                else
                {
                    array = results.SelectToken(jsonPath);
                }

                if (array.Type == JTokenType.Property || array.Type == JTokenType.Object)
                {
                    array = array.FirstOrDefault();
                }

                if (array.Type != JTokenType.Array)
                {
                    throw new FunctionException($"The jsonPath {jsonPath} did not point to a json array.");
                }
                
                var keyValues = new Dictionary<string, string>();

                foreach (var item in array.AsJEnumerable())
                {
                    keyValues.Add(item.SelectToken(columnPath).ToString(), item.SelectToken(valuePath).ToString());
                }

                values = new string[columns.Length];
                for(var i = 0; i < columns.Length; i++)
                {
                    if (keyValues.ContainsKey(columns[i]))
                    {
                        values[i] = keyValues[columns[i]];
                    }
                }

                return true;
            }
            catch(Exception ex)
            {
                values = new string[columns.Length];
                return false;
            }
        }

        public string[] JsonArrayToColumnsImport(string json, string jsonPath, string columnPath)
        {
            var results = JToken.Parse(json);

            JToken array;
            if (string.IsNullOrEmpty(jsonPath))
            {
                array = results;
            }
            else
            {
                array = results.SelectToken(jsonPath);
            }

            if (array.Type == JTokenType.Property || array.Type == JTokenType.Object)
            {
                array = array.FirstOrDefault();
            }

            if (array.Type != JTokenType.Array)
            {
                throw new FunctionException($"The jsonPath {jsonPath} did not point to a json array.");
            }
                
            var columns = new List<string>();

            foreach (var item in array.AsJEnumerable())
            {
                var column = item.SelectToken(columnPath).ToString();
                if (!string.IsNullOrEmpty(column))
                {
                    columns.Add(column);
                }
            }

            return columns.ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "GetDistanceTo",
            Description = "The distance in meters between two Geographical Coordinates. ")]
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

      
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String Functions", Name = "Split Fixed Width",
            Description = "Splits a string based on the string positions specified.")]
        public string[] SplitFixedWidth(string value, int[] positions)
        {
            var previousPos = 0;
            var result = new string[positions.Length];

            for (var i = 0; i < positions.Length; i++)
            {
                result[i] = value.Substring(previousPos, positions[i] - previousPos);
                previousPos = positions[i];
            }

            return result;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Age(Years from now)",
            Description = "The age (now) in years from the date.")]
        public int AgeInYears(DateTime dateValue)
        {
            return AgeInYearsAtDate(dateValue, DateTime.Today);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Age(Years from Date)",
            Description = "The age at the date in years from the date")]
        public int AgeInYearsAtDate(DateTime startDate, DateTime endDate)
        {
            var age = endDate.Year - startDate.Year;
            if (startDate > endDate.AddYears(-age)) age--;
            return age;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Unix TimeStamp to Date",
            Description = "Convert an integer based unix timestamp to a datetime")]
        public DateTime UnixTimeStampToDate(long unixTimeStamp)
        {
            var origDate = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            var convertedDate = origDate.AddSeconds(unixTimeStamp).ToLocalTime();
            return convertedDate;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Create SHA1 Hash",
            Description = "Creates a hash based on the SHA1 algorithm.")]
        public string CreateSHA1(string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            var sha1 = System.Security.Cryptography.SHA1.Create();
            var hash = sha1.ComputeHash(bytes);
            // Loop through each byte of the hashed data 
            // and format each one as a hexadecimal string.
            var sBuilder = new StringBuilder();

            for (var i = 0; i < hash.Length; i++)
            {
                sBuilder.Append(hash[i].ToString("x2"));
            }

            // Return the hexadecimal string.
            return sBuilder.ToString();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Maximum Number",
    Description = "Returns the highest value in the array ")]
        public decimal MaxNumber(decimal[] value)
        {
            return value.Max();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Minimum Number",
            Description = "Returns the lowest value in the array ")]
        public decimal MinNumber(decimal[] value)
        {
            return value.Min();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Maximum Date",
            Description = "Returns the highest date in the array ")]
        public DateTime MaxDate(DateTime[] value)
        {
            return value.Max();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Minimum Date",
            Description = "Returns the lowest date in the array ")]
        public DateTime MinNDate(DateTime[] value)
        {
            return value.Min();
        }

    }

}

 