using System;
using System.Linq;
using dexih.functions.Exceptions;

namespace dexih.functions.BuiltIn
{
    public class CategorizeFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Map, 
            Category = "Categorize", 
            Name = "Range Categorize",
        Description = "Sorts a value into a specified range.  Range array should be sorted list of ranges.  Returns true if in the range, and rangeString low-high.")]
        public bool RangeCategorize(double value, double[] range, out double? rangeLow, out double? rangeHigh, out string rangeString)
        {
            if(range == null || range.Length == 0)
            {
                throw new FunctionException("The RangeCategorize failed, as no range was specified.");
            }

            if(value < range[0])
            {
                rangeString = $"< {range[0]}";
                rangeLow = null;
                rangeHigh = range[0];
                return false;
            }

            for(var i = 1; i < range.Length; i++)
            {
                if (value < range[i])
                {
                    rangeString = $"{range[i - 1]} - {range[i]}";
                    rangeLow = range[i - 1];
                    rangeHigh = range[i];
                    return true;
                }
            }

            rangeString = $"> {range.Last()}";
            rangeLow = range.Last();
            rangeHigh = null;
            return false;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, 
            Category = "Categorize", 
            Name = "Interval Categorize",
            Description = "Categorizes values into intervals starting with the low and ending with the high value.  Returns true if value within specified range, otherwise false.")]
        public bool IntervalCategorize(double value, long interval, long? lowValue, long? highValue, out double? rangeLow, out double? rangeHigh, out string rangeString)
        {
            if(interval == 0)
            {
                throw new FunctionException("The interval specified must be a non-zero value.");
            }

            if (lowValue >= highValue)
            {
                throw new FunctionException("The low value must be lower than the high value.");
            }

            if (value < lowValue)
            {
                rangeString = $"< {lowValue}";
                rangeLow = null;
                rangeHigh = lowValue;
                return false;
            }

            if (value > highValue)
            {
                rangeString = $"< {highValue}";
                rangeLow = null;
                rangeHigh = lowValue;
                return false;
            }

            rangeLow = ( Math.Floor((value - lowValue??0) / interval) * interval) + lowValue;
            rangeHigh = rangeLow + interval;

            if (rangeHigh > highValue)
            {
                rangeHigh = highValue;
            }

            rangeString = $"{rangeLow} - {rangeHigh}";
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map,
            Category = "Categorize",
            Name = "Discrete Range Categorize",
        Description = "Sorts a value into a specified range using discrete (integer) values.  Range array should be sorted list of ranges.  Returns true if in the range, and rangeString low-high.")]
        public bool DiscreteRangeCategorize(long value, long[] range, out long? rangeLow, out long? rangeHigh, out string rangeString)
        {
            if (range == null || range.Length == 0)
            {
                throw new FunctionException("The DiscreteRangeCategorize failed, as no range was specified.");
            }

            if (value < range[0])
            {
                rangeString = $"< {range[0]}";
                rangeLow = null;
                rangeHigh = range[0];
                return false;
            }

            for (var i = 1; i < range.Length; i++)
            {
                if (value <= range[i] || range.Length == i)
                {
                    var highRange = range.Length == i ? (long?) null : range[i];
                    rangeString = $"{range[i - 1]} - {highRange}";
                    rangeLow = range[i - 1];
                    rangeHigh = highRange;
                    return true;
                }
            }

            rangeString = $"> {range.Last()}";
            rangeLow = range.Last();
            rangeHigh = null;
            return false;
        }
    }
}
