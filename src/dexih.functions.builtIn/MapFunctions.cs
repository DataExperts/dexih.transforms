using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml;
using dexih.functions.Exceptions;
using Dexih.Utils.Crypto;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;

namespace dexih.functions.BuiltIn
{
    /// <summary>
    /// 
    /// </summary>

    public class MapFunctions
    {
        public GlobalSettings GlobalSettings { get; set; }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Concatenate Strings",
            Description = "Concatenates multiple string fields.")]
        public string Concat([TransformFunctionParameter(Description = "Array of Values to Concatenate")] string[] values)
        {
            return string.Concat(values);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Index Of",
            Description = "The zero-based index of the first occurrence of the specified string in this field.")]
        public int IndexOf(string value, string search)
        {
            return value.IndexOf(search, StringComparison.Ordinal);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Insert",
            Description =
                "Returns a new string in which a specified string is inserted at a specified index position in this instance.")]
        public string Insert(string value, int startIndex, string insertString)
        {
            return value.Insert(startIndex, insertString);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Join",
            Description =
                "Concatenates all the elements of a string array, using the specified separator between each element.")]
        public string Join(string separator, string[] values)
        {
            return string.Join(separator, values);
        }


        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Pad Left",
            Description =
                "Returns a new string that right-aligns the characters in this instance by padding them with spaces on the left, for a specified total length.")]
        public string PadLeft(string value, int width, string paddingChar)
        {
            return value.PadLeft(width, paddingChar[0]);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Pad Right",
            Description =
                "Returns a new string that left-aligns the characters in this string by padding them with spaces on the right, for a specified total length.")]
        public string PadRight(string value, int width, string paddingChar)
        {
            return value.PadRight(width, paddingChar[0]);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Remove",
            Description =
                "Returns a new string in which a specified number of characters in the current instance beginning at a specified position have been deleted.")]
        public string Remove(string value, int startIndex, int count)
        {
            return value.Remove(startIndex, count);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Replace",
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Split",
            Description = "Splits a string into multiple return fields that are based on the characters in an array.")]
        public int Split(string value, string separator, int? count, out string[] result)
        {
            if (string.IsNullOrEmpty(value))
            {
                result = null;
                return 0;
            }

            result = count == null ? value.Split(separator) : value.Split(separator, count.Value);
            
            return result.Length;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Substring",
            Description =
                "Retrieves a substring from this instance. The substring starts at a specified character position and has a specified length.")]
        public string Substring(string stringValue, int start, int length)
        {
            var stringLength = stringValue.Length;
            if (start > stringLength) return "";
            if (start + length > stringLength) return stringValue.Substring(start);
            return stringValue.Substring(start, length);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "To Lowercase",
            Description = "Returns a copy of this string converted to lowercase.")]
        public string ToLower(string value)
        {
            return value.ToLower();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "To Uppercase",
            Description = "Returns a copy of this string converted to uppercase.")]
        public string ToUpper(string value)
        {
            return value.ToUpper();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Trim",
            Description = "Removes all leading and trailing white-space characters from the current field.")]
        public string Trim(string value)
        {
            return value.Trim();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Trim End",
            Description =
                "Removes all leading and trailing occurrences of a set of characters specified in an array from the current field.")]
        public string TrimEnd(string value)
        {
            return value.TrimEnd();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Trim Start",
            Description =
                "Removes all trailing occurrences of a set of characters specified in an array from the current field.")]
        public string TrimStart(string value)
        {
            return value.TrimStart();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Length",
            Description = "Return the length of the string.")]
        public int Length(string value)
        {
            return value.Length;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Word Count",
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Word Extract",
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
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date", Name = "To Date",
            Description =
                "Return boolean if the value is a valid date.  If the date is value the result parameter contains the converted date.")]
        public bool ToDate(string value, out DateTime result)
        {
            if (string.IsNullOrEmpty(value))
            {
                result = DateTime.MinValue;
                return false;
            }

            return DateTime.TryParse(value, out result);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date", Name = "To Date (Format)",
            Description =
                "Converts a sting to a date based on a specific string format.  See [format strings](https://docs.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings?view=netframework-4.7.2) for more information.")]
        public bool ToDateExact(string value, string[] format, out DateTime result)
        {
            if (string.IsNullOrEmpty(value))
            {
                result = DateTime.MinValue;
                return false;
            }

            return DateTime.TryParseExact(value, format, CultureInfo.CurrentCulture, DateTimeStyles.None, out result);
        }
        

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Create Date",
            Description =
                "Returns a new DateTime from separate year,month,day,hour,minute,second values.")]
        public DateTime CreateDate(
            [TransformFunctionParameter(Name = "Year")] int? year, 
            [TransformFunctionParameter(Name = "Numbered Month (1-12)")] int? month,
            [TransformFunctionParameter(Name = "Named Month (Jan-Dec)")] string monthName,
            [TransformFunctionParameter(Name = "Day of Month (1-31)")]int? day, 
            [TransformFunctionParameter(Name = "Hour (0-23)")]int? hour, 
            [TransformFunctionParameter(Name = "Minute (0-59)")]int? minute, 
            [TransformFunctionParameter(Name = "Second (0-59)")]int? second)
        {
            return DateTime.Parse(
                $"{year ?? DateTime.Now.Year}-{month?.ToString() ?? (monthName?? "1")}-{day ?? 1} {hour ?? 0}:{minute ?? 0}:{second ?? 0}");
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Add Years",
            Description =
                "Returns a new DateTime that adds the specified number of years to the value of this instance.")]
        public DateTime AddYears(DateTime dateValue, int addValue)
        {
            return dateValue.AddYears(addValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Days in Month",
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Short Month",
            Description = "A three letter value of the month (e.g. Jan, Feb, Mar).")]
        public string ShortMonth(DateTime dateValue)
        {
            return CultureInfo.CurrentCulture.DateTimeFormat.GetAbbreviatedMonthName(dateValue.Month);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Long Month",
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
            return EncryptString.Encrypt(value, GlobalSettings?.EncryptionKey, 1000);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Strong Decrypt",
            Description = "Strong Decrypts the string.")]
        public string StrongDecrypt(string value)
        {
            return EncryptString.Decrypt(value, GlobalSettings?.EncryptionKey, 1000);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Fast Encrypt",
            Description = "Fast Encrypts the string.")]
        public string FastEncrypt(string value)
        {
            return EncryptString.Encrypt(value, GlobalSettings?.EncryptionKey, 5);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Fast Decrypt",
            Description = "Fast Decrypts the string.")]
        public string FastDecrypt(string value)
        {
            return EncryptString.Decrypt(value, GlobalSettings?.EncryptionKey, 5);
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

 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Xml", Name = "XPath Values",
            Description = "Parses an xml string into a series of xpath results.")]
        public bool XPathValues(XmlDocument xml, [TransformFunctionLinkedParameter("XPath to Value")] string[] xPaths, [TransformFunctionLinkedParameter("XPath to Value")] out string[] values)
        {
            var returnValue = true;

            try
            {
                //XmlDocument xmlDoc = new XmlDocument();
                //xmlDoc.LoadXml(xml);

                //var stream = new StringReader(xml);
                //var xPathDocument = new XPathDocument(stream);
                //var xPathNavigator = xPathDocument.CreateNavigator();

                values = new string[xPaths.Length];

                for (var i = 0; i < xPaths.Length; i++)
                {
                    var xmlNode = xml.SelectSingleNode(xPaths[i]);
                    //var xmlNode = xPathNavigator.SelectSingleNode(xPaths[i]);

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
            Name = "JSON Values",
            Description = "Parses a JSON string into a series of elements.  The JSON string must contain only one result set.",
            ImportMethod = nameof(JsonValuesImport))
        ]
        public bool JsonValues(JToken json, [TransformFunctionLinkedParameter("JsonPath to Value")] string[] jsonPaths, [TransformFunctionLinkedParameter("JsonPath to Value")] out string[] values)
        {
            try
            {
                var returnValue = true;

                // var results = JToken.Parse(json);

                values = new string[jsonPaths.Length];

                for (var i = 0; i < jsonPaths.Length; i++)
                {
                    var token = json.SelectToken(jsonPaths[i]);
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
            Name = "Json Array To Columns",
            Description = "Pivots a json array into column values. ",
            ImportMethod = nameof(JsonArrayToColumnsImport))
        ]
        public bool JsonArrayToColumns(JToken json, string jsonPath, string columnPath, string valuePath, [TransformFunctionLinkedParameter("Column to Value")] string[] columns, [TransformFunctionLinkedParameter("Column to Value")] out string[] values)
        {
            try
            {
                // var results = JToken.Parse(json);

                JToken array;
                if (string.IsNullOrEmpty(jsonPath))
                {
                    array = json;
                }
                else
                {
                    array = json.SelectToken(jsonPath);
                }

                if (array.Type == JTokenType.Property || array.Type == JTokenType.Object)
                {
                    array = array.FirstOrDefault();
                }

                if (array == null || array.Type != JTokenType.Array)
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
            catch(Exception)
            {
                values = new string[columns.Length];
                return false;
            }
        }

        public string[] JsonArrayToColumnsImport(JToken json, string jsonPath, string columnPath)
        {
            JToken array;
            if (string.IsNullOrEmpty(jsonPath))
            {
                array = json;
            }
            else
            {
                array = json.SelectToken(jsonPath);
            }

            if (array.Type == JTokenType.Property || array.Type == JTokenType.Object)
            {
                array = array.FirstOrDefault();
            }

            if (array == null || array.Type != JTokenType.Array)
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Get Distance To",
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

      
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Split Fixed Width",
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
            return unixTimeStamp.UnixTimeStampToDate();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Create SHA1/Hash",
            Description = "Creates a sha1 hash of the value.  This will (virtually) always be unique for any different value.")]
        public string CreateSHA1(string value)
        {
            return value?.CreateSHA1();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Maximum Number",
            Description = "Returns the highest value in the array ")]
        public T MaxValue<T>(T[] value)
        {
            return value.Max();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Minimum Number",
            Description = "Returns the lowest value in the array ")]
        public T MinValue<T>(T[] value)
        {
            return value.Min();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Switch Condition", GenericTypeDefault = DataType.ETypeCode.String,
            Description = "Maps the 'value' to the matching 'when' and returns the 'then'.  No matches returns default value (or original value if default is null."),
        ]
        public T Switch<T>(object value, [TransformFunctionLinkedParameter("When")] object[] when, [TransformFunctionLinkedParameter("When")] T[] then, T defaultValue)
        {
            for(var i = 0; i < when.Length; i++)
            {
                if (i > then.Length)
                {
                    break;
                }

                if (Operations.Equal(value, when[i]))
                {
                    return then[i];
                }
            }

            return defaultValue;
        }

    }

}

 