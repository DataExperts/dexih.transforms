using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.functions.BuiltIn
{
    public class ThrottleResult
    {
        [TransformFunctionParameter(Description = "Number of periods processed.")]
        public int PeriodCount { get; set; }
        
        [TransformFunctionParameter(Description = "Rows processed in the current period.")]
        public int RowCount { get; set; }
    }

    public class ThrottleFunctions
    {
        private Stopwatch _stopwatch;
        private int _rowCount;

        public enum EPeriods
        {
            Millisecond = 1, Second, Minute, Hour, Day
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Throttle", Name = "Throttle the number of rows per time period.",
            Description = "Returns the row count for the current period.")]
        public async Task<long> Throttle(
            [TransformFunctionParameter(Description = "Rows allowed for each period unit.")] int rowsPerPeriod,
            [TransformFunctionParameter(Description = "Units (i.e. number of seconds) for each throttling period.")] int periodUnit,
            [TransformFunctionParameter(Description = "Time unit")] EPeriods period,
            CancellationToken cancellationToken = default
            )
        {
            if (_stopwatch == null)
            {
                _stopwatch = Stopwatch.StartNew();
                _rowCount = 0;
            }

            if (_rowCount >= rowsPerPeriod)
            {
                TimeSpan timePeriod;
                switch (period)
                {
                    case EPeriods.Day:
                        timePeriod = TimeSpan.FromDays(periodUnit);
                        break;
                    case EPeriods.Hour:
                        timePeriod = TimeSpan.FromHours(periodUnit);
                        break;
                    case EPeriods.Minute:
                        timePeriod = TimeSpan.FromMinutes(periodUnit);
                        break;
                    case EPeriods.Second:
                        timePeriod = TimeSpan.FromSeconds(periodUnit);
                        break;
                    case EPeriods.Millisecond:
                        timePeriod = TimeSpan.FromMilliseconds(periodUnit);
                        break;
                    default:
                        throw new Exception("Invalid time period");
                }

                var delay = timePeriod - _stopwatch.Elapsed;
                await Task.Delay(delay, cancellationToken);

                _rowCount = 0;
                _stopwatch.Reset();
            }

            _rowCount++;

            return _rowCount;
        }
    }
}