using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dexih.functions.Tasks
{
    public class ManagedTaskTrigger
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EDayOfWeek
        {
            Sunday = 0,
            Monday = 1,
            Tuesday = 2,
            Wednesday = 3,
            Thursday = 4,
            Friday = 5,
            Saturday = 6
        }

        public long DatajobKey { get; set; }
        public DateTime? StartDate { get; set; }
        public TimeSpan? IntervalTime { get; set; }
        public EDayOfWeek[] DaysOfWeek { get; set; }
        public TimeSpan? StartTime { get; set; }
        public TimeSpan? EndTime { get; set; }
        public string CronExpression { get; set; }
        public int? MaxRecurrs { get; set; }
        public bool FileWatch { get; set; }
        public bool ExternalTrigger { get; set; }

        /// <summary>
        /// Gets a description of the trigger.
        /// </summary>
        public string Details
        {
            get
            {
                StringBuilder desc = new StringBuilder();

                if (StartDate != null)
                    desc.AppendLine("Starts on/after:" + StartDate);
                if (StartTime != null)
                    desc.AppendLine("Runs daily after:" + StartTime.Value.ToString());
                if (EndTime != null)
                    desc.AppendLine("Ends daily after:" + EndTime.Value.ToString());
                if (DaysOfWeek.Length > 0 && DaysOfWeek.Length < 7)
                    desc.AppendLine("Only on:" + String.Join(",", DaysOfWeek.Select(c => c.ToString()).ToArray()));
                if (IntervalTime != null)
                    desc.AppendLine("Runs every: " + IntervalTime.Value.ToString());
                if (MaxRecurrs != null)
                    desc.AppendLine("Recurrs for: " + MaxRecurrs.Value.ToString());

                return desc.ToString();
            }
        }

        /// <summary>
        /// Retrieves the next time this trigger will fire.
        /// </summary>
        /// <returns>DateTime of trigger, or null if no trigger available</returns>
        public DateTime? NextTrigger()
        {
            TimeSpan dailyStart = StartTime == null ? new TimeSpan(0, 0, 0) : (TimeSpan)StartTime;
            TimeSpan dailyEnd = EndTime == null ? new TimeSpan(23, 59, 59) : (TimeSpan)EndTime;

            //set the initial start date
            DateTime startAt = StartDate == null || StartDate < DateTime.Now ? DateTime.Now.Date : (DateTime)StartDate;

            if (DaysOfWeek != null && DaysOfWeek.Length == 0)
            {
                throw new ManagedTaskTriggerException(this, "No days of the week have been selected.");
            }

            if (dailyStart > dailyEnd)
            {
                throw new ManagedTaskTriggerException(this, "The daily end time is after the daily start time.");
            }

            //loop through each day from now until we find a day of the week that is selected.  If DaysOfWeek == null, then any day can be used.
            if (DaysOfWeek != null)
            {
                for (int i = 0; i < 7; i++)
                {
                    if (DaysOfWeek.Contains(DayOfWeek(startAt)))
                        break;
                    startAt = startAt.AddDays(1);
                }
            }

            //Combine that start date and time to get a final start date/time
            startAt = startAt.Add(dailyStart);
            bool passDate = true;
            int recurrs = 0;

            //loop through the intervals until we find one that is greater than the current time.
            while (startAt < DateTime.Now && passDate)
            {
                if (IntervalTime == null)
                {
                    //There is no interval set and the start date has already passed.
                    return null;
                }

                startAt = startAt.Add(IntervalTime.Value);
                passDate = true;

                //if this is an invalid day, move to next day/starttime.
                if (DaysOfWeek != null)
                {
                    if (DaysOfWeek.Contains(DayOfWeek(startAt)) == false)
                    {
                        passDate = false;
                    }
                }

                if (startAt.TimeOfDay < dailyStart || startAt.TimeOfDay > dailyEnd)
                {
                    passDate = false;
                }

                if (passDate)
                {
                    recurrs += 1;
                    if (MaxRecurrs != null && recurrs > MaxRecurrs.Value)
                    {
                        // The trigger has exceeded the maximum recurrences
                        return null;
                    }
                }
                else
                {
                    //if the day of the week is invalid, move the the start of the next valid one.
                    if (DaysOfWeek == null)
                    {
                        startAt = startAt.AddDays(1);
                    }
                    {
                        for (int i = 0; i < 6; i++)
                        {
                            startAt = startAt.AddDays(1);
                            if (DaysOfWeek.Contains(DayOfWeek(startAt)))
                                break;
                        }
                    }
                    startAt = startAt.Date.Add(dailyStart);
                }

            }

            return startAt;
        }

        private EDayOfWeek DayOfWeek(DateTime date)
        {
            return (EDayOfWeek)Enum.Parse(typeof(EDayOfWeek), date.DayOfWeek.ToString());
        }
    }
}
