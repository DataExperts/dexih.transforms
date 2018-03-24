using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.XPath;
using dexih.functions;
using Newtonsoft.Json.Linq;

namespace dexih.standard.functions
{
    public class AggregateFunctions
    {
        private const string NullPlaceHolder = "A096F007-26EE-479E-A9E1-4E12427A5AF0"; //used a a unique string that can be substituted for null
        
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private int? _cacheInt;
        private double? _cacheDouble;
        private DateTime? _cacheDate;
        private string _cacheString;
        private Dictionary<string, string> _cacheStringDictionary;
        private List<object> _cacheList;
        private StringBuilder _cacheStringBuilder;

        public bool Reset()
        {
            _cacheInt = null;
            _cacheDouble = null;
            _cacheDate = null;
            _cacheString = null;
            _cacheStringDictionary = null;
            _cacheList = null;
            _cacheStringDictionary = null;
            _cacheStringBuilder = null;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Sum", Description = "Sum of the values", ResultMethod = nameof(SumResult), ResetMethod = nameof(Reset))]
        public void Sum(double value)
        {
            
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
        }

        public double SumResult([TransformFunctionIndex]int index)
        {
            if (_cacheDouble == null)
                return 0;

            return (double)_cacheDouble;
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Average", Description = "Average of the values", ResultMethod = nameof(AverageResult), ResetMethod = nameof(Reset))]
        public void Average(double value)
        {
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
            _cacheInt = _cacheInt + 1;
        }
        public double AverageResult([TransformFunctionIndex]int index)
        {
            if (_cacheDouble == null || _cacheInt == null || _cacheInt == 0)
                return 0;

            return (double)_cacheDouble / (double)_cacheInt;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Minimum", Description = "Minimum Value", ResultMethod = nameof(MinResult), ResetMethod = nameof(Reset))]
        public void Min(double value)
        {
            if (_cacheDouble == null) _cacheDouble = value;
            else if (value < _cacheDouble) _cacheDouble = value;
        }
        public double? MinResult([TransformFunctionIndex]int index)
        {
            return _cacheDouble;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Maximum", Description = "Maximum Value", ResultMethod = nameof(MaxResult), ResetMethod = nameof(Reset))]
        public void Max(double value)
        {
            if (_cacheDouble == null) _cacheDouble = value;
            else if (value > _cacheDouble) _cacheDouble = value;
        }
        public double? MaxResult([TransformFunctionIndex]int index)
        {
            return _cacheDouble;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "First", Description = "First Value", ResultMethod = nameof(FirstResult), ResetMethod = nameof(Reset))]
        public void First(string value)
        {
            if (_cacheString == null) _cacheString = value;
        }

        public string FirstResult([TransformFunctionIndex]int index)
        {
            return _cacheString;
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Last", Description = "Last Value", ResultMethod = nameof(LastResult), ResetMethod = nameof(Reset))]
        public void Last(string value)
        {
            _cacheString = value;
        }
        public string LastResult([TransformFunctionIndex]int index)
        {
            return _cacheString;
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Count", Description = "Number of records", ResultMethod = nameof(CountResult), ResetMethod = nameof(Reset))]
        public void Count()
        {
            if (_cacheInt == null) _cacheInt = 1;
            else _cacheInt = _cacheInt + 1;
        }

        public int CountResult([TransformFunctionIndex]int index)
        {
            if (_cacheInt == null)
                return 0;

            return (int)_cacheInt;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Count Distinct", Description = "Number if distinct values", ResultMethod = nameof(CountDistinctResult), ResetMethod = nameof(Reset))]
        public void CountDistinct(string value)
        {
            if (_cacheStringDictionary == null) _cacheStringDictionary = new Dictionary<string, string>();
            if (value == null) value = NullPlaceHolder; //dictionary can't use nulls, so substitute null values.
            if (_cacheStringDictionary.ContainsKey(value) == false)
                _cacheStringDictionary.Add(value, null);
        }
        public int CountDistinctResult([TransformFunctionIndex]int index)
        {
            return _cacheStringDictionary.Keys.Count;
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Concatenate Aggregate", Description = "Returns concatenated string of repeating values.", ResultMethod = nameof(ConcatAggResult), ResetMethod = nameof(Reset))]
        public void ConcatAgg(string value, string delimiter)
        {
            if (_cacheStringBuilder == null)
            {
                _cacheStringBuilder = new StringBuilder();
                _cacheStringBuilder.Append(value);
            }
            else
                _cacheStringBuilder.Append(delimiter + value);
        }
        public string ConcatAggResult([TransformFunctionIndex]int index)
        {
            return _cacheStringBuilder.ToString();
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Median", Description = "The median value in a series", ResultMethod = nameof(MedianResult), ResetMethod = nameof(Reset))]
        public void Median(double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            _cacheList.Add(value);
        }
        public double MedianResult([TransformFunctionIndex]int index)
        {
            if (_cacheList == null)
                return 0;
            var sorted = _cacheList.OrderBy(c => (double)c).ToArray();
            var count = sorted.Length;

            if (count % 2 == 0)
            {
                // count is even, average two middle elements
                var a = (double)sorted[count / 2 - 1];
                var b = (double)sorted[count / 2];
                return (a + b) / 2;
            }
            // count is odd, return the middle element
            return (double)sorted[count / 2];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Standard Deviation", Description = "The standard deviation in a set of numbers", ResultMethod = nameof(StdDevResult), ResetMethod = nameof(Reset))]
        public void StdDev(double value)
        {
            Variance(value);
        }

        public double StdDevResult([TransformFunctionIndex]int index)
        {
            var sd = Math.Sqrt(VarianceResult(index));
            return sd;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Variance", Description = "The variance in a set of numbers.", ResultMethod = nameof(VarianceResult), ResetMethod = nameof(Reset))]
        public void Variance(double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;

            _cacheList.Add(value);
            _cacheInt++;
            _cacheDouble += value;
        }

        public double VarianceResult([TransformFunctionIndex]int index)
        {
            if (_cacheList == null || _cacheInt == null || _cacheInt == 0 || _cacheDouble == null || _cacheDouble == 0 )
                return 0;

            var average = (double)_cacheDouble / (double)_cacheInt;
            var sumOfSquaresOfDifferences = _cacheList.Select(val => ((double)val - average) * ((double)val - average)).Sum();
            var sd = sumOfSquaresOfDifferences / (double)_cacheInt;

            return sd;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "MinimumDate", Description = "Minimum Date", ResultMethod = nameof(MinDateResult), ResetMethod = nameof(Reset))]
        public void MinDate(DateTime value)
        {
            if (_cacheDate == null) _cacheDate = value;
            else if (value < _cacheDate) _cacheDate = value;
        }

        public DateTime? MinDateResult([TransformFunctionIndex]int index)
        {
            return _cacheDate;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "MaximumDate", Description = "Maximum Date", ResultMethod = nameof(MaxDateResult), ResetMethod = nameof(Reset))]
        public void MaxDate(DateTime value)
        {
            if (_cacheDate == null) _cacheDate = value;
            else if (value > _cacheDate) _cacheDate = value;
        }
        
        public DateTime? MaxDateResult([TransformFunctionIndex]int index)
        {
            return _cacheDate;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "First When", Description = "First resultValue when the condition = conditionValue", ResultMethod = nameof(FirstWhenResult), ResetMethod = nameof(Reset))]
        public void FirstWhen(string condition, string conditionValue, string resultValue)
        {
            if (condition == conditionValue && _cacheString == null)
                _cacheString = resultValue;
        }
        public string FirstWhenResult([TransformFunctionIndex]int index)
        {
            return _cacheString;
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Last When", Description = "Last resultValue when the condition = conditionValue", ResultMethod = nameof(LastWhenResult), ResetMethod = nameof(Reset))]
        public void LastWhen(string condition, string conditionValue, string resultValue)
        {
            if (condition == conditionValue)
                _cacheString = resultValue;
        }

        public string LastWhenResult([TransformFunctionIndex]int index)
        {
            return _cacheString;
        }

    }
}