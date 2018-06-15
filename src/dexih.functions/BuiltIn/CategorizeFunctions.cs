using System.Linq;

namespace dexih.functions.BuiltIn
{
    public class CategorizeFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Map, 
            Category = "Categorize Functions", 
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
            Category = "Categorize Functions",
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
