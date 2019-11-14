using System;
using System.Globalization;

namespace dexih.functions.builtIn
{
    public class DateFunctions
    {
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

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Days Between",
            Description = "The days between the start and end date.")]
        public double DaysBetween(DateTime startDate, DateTime endDate)
        {
            return (endDate - startDate).TotalDays;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Hours Between",
            Description = "The hours between the start and end date.")]
        public double HoursBetween(DateTime startDate, DateTime endDate)
        {
            return (endDate - startDate).TotalHours;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Minutes Between",
            Description = "The minutes between the start and end date.")]
        public double MinutesBetween(DateTime startDate, DateTime endDate)
        {
            return (endDate - startDate).TotalMinutes;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Seconds Between",
            Description = "The seconds between the start and end date.")]
        public double SecondsBetween(DateTime startDate, DateTime endDate)
        {
            return (endDate - startDate).TotalSeconds;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Date", Name = "Milliseconds Between",
            Description = "The milliseconds between the start and end date.")]
        public double MillisecondsBetween(DateTime startDate, DateTime endDate)
        {
            return (endDate - startDate).TotalMilliseconds;
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
    }
}