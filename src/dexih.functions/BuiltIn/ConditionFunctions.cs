using System;
using System.Text.RegularExpressions;
using dexih.functions.Query;

namespace dexih.functions.BuiltIn
{
    public class ConditionFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Less Than",
            Description = "Less than")]
        [TransformFunctionCompare(Compare = Filter.ECompare.LessThan)]
        public bool LessThan(double value, double compare)
        {
            return value < compare;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Less Than/Equal",
            Description = "Less than or Equal")]
        [TransformFunctionCompare(Compare = Filter.ECompare.LessThanEqual)]
        public bool LessThanEqual(double value, double compare)
        {
            return value <= compare;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Greater Than",
            Description = "Greater than")]
        [TransformFunctionCompare(Compare = Filter.ECompare.GreaterThan)]
        public bool GreaterThan(double value, double compare)
        {
            return value > compare;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition",
            Name = "Greater Than/Equal", Description = "Greater or Equal")]
        [TransformFunctionCompare(Compare = Filter.ECompare.GreaterThanEqual)]
        public bool GreaterThanEqual(double value, double compare)
        {
            return value >= compare;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Strings Equal",
            Description = "The list of string values are equal.")]
        [TransformFunctionCompare(Compare = Filter.ECompare.IsEqual)]
        public bool IsEqual(string[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Number",
            Description = "Value is a valid number")]
        public bool IsNumber(string value)
        {
            return decimal.TryParse(value, out _);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "To Date",
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

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Null",
            Description = "Value is null")]
        public bool IsNull(string value)
        {
            return value == null;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Is Between",
            Description = "Value is between the specified values but not equal to them.")]
        public bool IsBetween(double value, double lowRange, double highRange)
        {
            return value > lowRange && value < highRange;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition",
            Name = "Is Between Inclusive", Description = "Value is equal or between the specified values")]
        public bool IsBetweenInclusive(double value, double lowRange, double highRange)
        {
            return value >= lowRange && value <= highRange;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition",
            Name = "Is Regular Expression", Description = "Value matches the specified regular expression.")]
        public bool RegexMatch(string input, string pattern)
        {
            return Regex.Match(input, pattern).Success;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Contains",
            Description = "Returns a value indicating whether a specified substring occurs within this string.")]
        public bool Contains(string value, string contains)
        {
            return value.Contains(contains);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Ends With",
            Description = "Determines whether the end of this string instance matches the specified string.")]
        public bool EndsWith(string value, string endsWith)
        {
            return value.EndsWith(endsWith);
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Condition", Name = "Starts With",
            Description = "Determines whether the beginning of this string instance matches the specified string.")]
        public bool StartsWith(string value, string startsWith)
        {
            return value.StartsWith(startsWith);
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

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "String Functions", Name = "IsIn",
            Description = "Value is one of the elements in an array")]
        public bool IsIn(string value, string[] compareTo)
        {
            var returnValue = false;
            foreach (var t in compareTo)
                returnValue = returnValue | value == t;
            return returnValue;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "DateTime Equals",
            Description = "Are the specified datetime values equal.")]
        [TransformFunctionCompare(Compare = Filter.ECompare.IsEqual)]
        public bool IsDateTimeEqual(DateTime[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Numeric Condition", Name = "Numeric Equals",
            Description = "Are the specified numeric values equal.")]
        [TransformFunctionCompare(Compare = Filter.ECompare.IsEqual)]
        public bool IsNumericEqual(decimal[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Boolean Condition", Name = "Boolean Equals",
            Description = "Are the specified boolean conditions equal.")]
        [TransformFunctionCompare(Compare = Filter.ECompare.IsEqual)]
        public bool IsBooleanEqual(bool[] values)
        {
            for (var i = 1; i < values.Length; i++)
            {
                if (values[0] != values[i]) return false;
            }

            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Boolean Condition", Name = "Is True",
            Description = "Is the specified value equal to true.")]
        public bool IsTrue(bool value) => value;

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Date Between",
            Description = "Date is between the start/end date")]
        public bool IsDateBetween(DateTime value, DateTime lowRange, DateTime highRange)
        {
            return value > lowRange && value < highRange;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition",
            Name = "Is Date Between(inclusive)", Description = "Date is equal or between the start/end date")]
        public bool IsDateBetweenInclusive(DateTime value, DateTime lowRange, DateTime highRange)
        {
            return value >= lowRange && value <= highRange;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition", Name = "Is Now Between Dates",
            Description = "The date now is between start/end date ")]
        public bool IsDateBetweenNow(DateTime lowRange, DateTime highRange)
        {
            return DateTime.Now > lowRange && DateTime.Now < highRange;
        }

        [TransformFunction(FunctionType = EFunctionType.Condition, Category = "Date Condition",
            Name = "Is Now Between Dates(inclusive)",
            Description = "The date now is equal or between the start/end date")]
        public bool IsDateBetweenInclusiveNow(DateTime lowRange, DateTime highRange)
        {
            return DateTime.Now >= lowRange && DateTime.Now <= highRange;
        }
        
    }
}