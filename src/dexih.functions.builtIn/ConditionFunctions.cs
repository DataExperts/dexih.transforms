using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using dexih.functions.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class ConditionFunctions<T>
    {
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Less Than",
            Description = "Less than", GenericType = EGenericType.All)]
        [TransformFunctionCompare(Compare = ECompare.LessThan)]
        public bool LessThan(T value, T compare) => Operations.LessThan(value, compare);

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Less Than/Equal",
            Description = "Less than or Equal", GenericType = EGenericType.All)]
        [TransformFunctionCompare(Compare = ECompare.LessThanEqual)]
        public bool LessThanOrEqual(T value, T compare) => Operations.LessThanOrEqual(value, compare);

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Greater Than",
            Description = "Greater than", GenericType = EGenericType.All)]
        [TransformFunctionCompare(Compare = ECompare.GreaterThan)]
        public bool GreaterThan(T value, T compare) => Operations.GreaterThan(value, compare);

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition",
            Name = "Greater Than/Equal", Description = "Greater or Equal", GenericType = EGenericType.All)]
        [TransformFunctionCompare(Compare = ECompare.GreaterThanEqual)]
        public bool GreaterThanOrEqual(T value, T compare) => Operations.GreaterThanOrEqual(value, compare);

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Equal",
            Description = "The list of values are equal.", GenericType = EGenericType.All)]
        [TransformFunctionCompare(Compare = ECompare.IsEqual)]
        public bool IsEqual(T[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (!Equals(values[0], values[i])) return false;
            }

            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Array Contains",
            Description = "The value is equal to at least one of the values in the values array", GenericType = EGenericType.All)]
        [TransformFunctionCompare(Compare = ECompare.IsIn)]
        public bool ArrayContains(T value, T[] values)
        {
            return values.Contains(value);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Date",
            Description =
                "Return boolean if the value is a valid date.")]
        public bool IsDate(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return false;
            }

            return DateTime.TryParse(value, out _);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Date (using exact format)",
    Description =
        "Return boolean if the value is a valid date using the specified format.")]
        public bool IsDateFormatExact(string value, string format)
        {
            if (string.IsNullOrEmpty(value))
            {
                return false;
            }

            return DateTime.TryParseExact(value, format, CultureInfo.CurrentCulture, DateTimeStyles.None, out _);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Number",
            Description = "Value is a valid number")]
        public bool IsNumber(string value)
        {
            return decimal.TryParse(value, out _);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Is Null",
            Description = "Value is null")]
        public bool IsNull(object value)
        {
            return value == null || value is DBNull;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Is Null Or Empty",
            Description = "Value is null or any empty string")]
        public bool IsNullOrEmpty(string value)
        {
            return string.IsNullOrEmpty(value);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Is Null Or Whitespace",
            Description = "Value is null or a string with only whitespaces")]
        public bool IsNullOrWhitespace(string value)
        {
            return string.IsNullOrWhiteSpace(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Is Between",
            Description = "Value is between the specified values but not equal to them.", GenericType = EGenericType.All)]
        public bool IsBetween(T value, T lowRange, T highRange)
        {
            return GreaterThan(value, lowRange) && LessThan(value, highRange);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition",
            Name = "Is Between Inclusive", Description = "Value is equal or between the specified values", GenericType = EGenericType.All)]
        public bool IsBetweenInclusive(T value, T lowRange, T highRange)
        {
            return GreaterThanOrEqual(value, lowRange) && LessThanOrEqual(value, highRange);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition",
            Name = "Is Regular Expression", Description = "Value matches the specified regular expression.")]
        public bool RegexMatch(string input, string pattern)
        {
            return Regex.Match(input, pattern).Success;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Contains",
            Description = "Returns a true when a specified \"contains\" string occurs within the value.")]
        public bool Contains(string value, string contains, bool ignoreCase = false)
        {
            return value.IndexOf(contains,
                       ignoreCase
                           ? StringComparison.InvariantCultureIgnoreCase
                           : StringComparison.CurrentCulture) >= 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Like",
            Description = "Sql \"like\" equivalent match which returns true when the pattern matches the matchExpression.")]
        public bool Like(
            [TransformFunctionParameter(Description = "Expression to search")] string matchExpression, 
            [TransformFunctionParameter(Description = "Pattern to search for (use % for multiple characters, _ for any single character)")]string pattern, 
            [TransformFunctionParameter(Description = "Escape character to proceed a % or _ search.")] string escapeCharacter = null)
        {
            return Operations.Like(matchExpression, pattern, escapeCharacter);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Ends With",
            Description = "Determines whether the end of this string instance matches the specified string.")]
        public bool EndsWith(string value, string endsWith, bool ignoreCase = false)
        {
            return value.EndsWith(endsWith, ignoreCase ? StringComparison.InvariantCultureIgnoreCase : StringComparison.CurrentCulture);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Starts With",
            Description = "Determines whether the beginning of this string instance matches the specified string.")]
        public bool StartsWith(string value, string startsWith, bool ignoreCase = false)
        {
            return value.StartsWith(startsWith, ignoreCase ? StringComparison.InvariantCultureIgnoreCase : StringComparison.CurrentCulture);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Is Uppercase",
            Description = "All character values are upper case.")]
        public bool IsUpper(string value, bool skipNonAlpha)
        {
            foreach (var t in value)
            {
                if ((skipNonAlpha && char.IsLetter(t) && !char.IsUpper(t)) ||
                    (skipNonAlpha == false && !char.IsUpper(t)))
                    return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Is Lowercase",
            Description = "All character values are lower case.")]
        public bool IsLower(string value, bool skipNonAlpha)
        {
            foreach (var t in value)
            {
                if ((skipNonAlpha && char.IsLetter(t) && !char.IsLower(t)) ||
                    (skipNonAlpha == false && !char.IsLower(t)))
                    return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Is Letters",
            Description = "Only contains letters")]
        public bool IsAlpha(string value)
        {
            foreach (var t in value)
            {
                if (!char.IsLetter(t))
                    return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "IsLetter/Digit",
            Description = "Only contains letters and digits (number).")]
        public bool IsAlphaNumeric(string value)
        {
            foreach (var t in value)
            {
                if (!char.IsLetter(t) && !char.IsNumber(t))
                    return false;
            }
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Is Pattern",
            Description =
                "Matches the pattern where A=upper character, a=lower character, 9=number and other values match precisely.")]
        public bool IsPattern(string value, string pattern)
        {
            if (value is null) return false;
            
            return value.IsPattern(pattern);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Daylight Saving",
            Description =
                "Indicates whether this instance of DateTime is within the daylight saving time range for the current time zone.")]
        public bool IsDaylightSavingTime(DateTime dateValue)
        {
            return TimeZoneInfo.Local.IsDaylightSavingTime(dateValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Leap Year",
            Description = "Returns an indication whether the specified year is a leap year.")]
        public bool IsLeapYear(DateTime dateValue)
        {
            return DateTime.IsLeapYear(dateValue.Year);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Weekend",
            Description = "Date falls on a weekend")]
        public bool IsWeekend(DateTime dateValue)
        {
            return dateValue.DayOfWeek == DayOfWeek.Saturday || dateValue.DayOfWeek == DayOfWeek.Sunday;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Weekday",
            Description = "Date falls on a week day")]
        public bool IsWeekDay(DateTime dateValue)
        {
            return !(dateValue.DayOfWeek == DayOfWeek.Saturday || dateValue.DayOfWeek == DayOfWeek.Sunday);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Numeric Equals",
            Description = "Are the specified decimal values equal with the specified precision.")]
        [TransformFunctionCompare(Compare = ECompare.IsEqual)]
        public bool IsDecimalEqual(decimal[] values, int precision = 6)
        {
            if (values.Length == 0) return true;
            
            var value1 = decimal.Round(values[0], precision);
            
            for (var i = 1; i < values.Length; i++)
            {
                if (value1 != decimal.Round(values[i], precision)) return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Current date between dates",
            Description = "The current date/time is between start/end date ")]
        public bool IsDateBetweenNow(DateTime lowRange, DateTime highRange)
        {
            return DateTime.Now > lowRange && DateTime.Now < highRange;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "General Condition", Name = "Range Intersect",
            Description = "The two ranges (lowRange1-highRange1 & lowRange2-highRange2) intersect.", GenericType = EGenericType.All)]
        public bool RangeIntersect(T lowRange1, T highRange1, T lowRange2, T highRange2)
        {
            return LessThan(lowRange1, highRange2) && LessThan(lowRange2, highRange1);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Even", Description = "The specific number is even")]
        public bool IsEven(long number)
        {
            return (number & 1) == 0;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Odd", Description = "The specific number is odd")]
        public bool IsOdd(long number)
        {
            return (number & 1) == 1;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Boolean Condition", Name = "And", Description = "The logical \"And\" of the conditions.  Evaluates true when all conditions are true.")]
        public bool And(bool[] condition)
        {
            if (condition.Length > 2)
            {
                throw new FunctionException("The \"And\" function requires at least two parameters.");
            }

            var result = true;
            foreach (var c in condition)
            {
                result = result && c;
                if (!c) return false;
            }

            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Boolean Condition", Name = "Or", Description = "The \"Or\" of the conditions.  Evaluates true when any condition is true.")]
        public bool Or(bool[] condition)
        {
            if (condition.Length > 2)
            {
                throw new FunctionException("The \"Or\" function requires at least two parameters.");
            }
            
            var result = false;
            foreach (var c in condition)
            {
                result = result || c;
                if (c) return true;
            }

            return false;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Boolean Condition", Name = "XOr", Description = "The logical \"Or\" of the conditions.  Evaluates true when one and only one condition is true.")]
        public bool Xor(bool[] condition)
        {
            if (condition.Length > 2)
            {
                throw new FunctionException("The \"XOr\" function requires at least two parameters.");
            }

            var result = false;
            foreach (var c in condition)
            {
                result = result ^ c;
            }

            return result;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Boolean Condition", Name = "Not", Description = "The not of the boolean value (true = false, false = true)")]
        public bool Not(bool value)
        {
            return !value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Power Of Two", Description = "The specific number is a power of two")]
        public static bool IsPowerOfTwo(long number)
        {
            if (number > 0L)
                return (number & number - 1L) == 0L;
            return false;
        }

        /// <summary>
        /// Find out whether the provided 64 bit integer is a perfect square, i.e. a square of an integer.
        /// </summary>
        /// <param name="number">The number to very whether it's a perfect square.</param>
        /// <returns>True if and only if it is a perfect square.</returns>
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Perfect Square", Description = "The specific number is a perfect square (i.e. square of an integer)")]
        public static bool IsPerfectSquare(long number)
        {
            if (number < 0)
                return false;
            switch (number & 15)
            {
                case 0:
                case 1:
                case 4:
                case 9:
                    var num = (int) Math.Floor(Math.Sqrt(number) + 0.5);
                    return num * num == number;
                default:
                    return false;
            }
        }

    }
}