using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class AggregateFunctions<T>
    {
        private T _oneHundred;

        //The cache parameters are used by the functions to maintain a state during a transform process.
        // private int? _cacheInt;
        private double _cacheDouble;
        private DateTime? _cacheDate;
        private Dictionary<object, T> _cacheDictionary;
        private List<T> _cacheList;
        private List<double> _doubleList;
        private StringBuilder _cacheStringBuilder;
        private object _cacheObject;
        private object[] _cacheArray;
        private SortedRowsDictionary<T> _sortedRowsDictionary;
        private T _cacheGeneric;
        private int _cacheCount;
        private HashSet<T> _hashSet;

        private bool _isFirst = true;

        public bool Reset()
        {
            _isFirst = true;
            // _cacheInt = null;
            _cacheDouble = 0;
            _cacheDate = null;
            _cacheDictionary = null;
            _cacheList = null;
            _cacheDictionary = null;
            _cacheStringBuilder = null;
            _cacheObject = null;
            _cacheArray = null;
            _sortedRowsDictionary = null;
            _cacheGeneric = default(T);
            _cacheCount = 0;
            _doubleList = null;
            _hashSet = null;
            return true;
        }
        
        public DateTime? DateResult() => _cacheDate;
        public object ObjectResult() => _cacheObject;
        public T GenericResult() => _cacheGeneric;
        public int CountResult() => _cacheCount;


        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Sum", Description = "Sum of the values", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void Sum(T value) => _cacheGeneric = Operations.Add(_cacheGeneric, value);

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Average", Description = "Average of the values", ResultMethod = nameof(AverageResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void Average(T value)
        {
            _cacheGeneric = Operations.Add(_cacheGeneric, value);
            _cacheCount = _cacheCount + 1;
        }
        public T AverageResult()
        {
            return _cacheCount == 0 ? default(T) : Operations.DivideInt(_cacheGeneric, _cacheCount);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Minimum", Description = "Minimum Value", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void Min(T value)
        {
            if (_isFirst)
            {
                _isFirst = false;
                _cacheGeneric = value;
            }
            else if (Operations.LessThan(value, _cacheGeneric)) _cacheGeneric = value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Maximum", Description = "Maximum Value", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void Max(T value)
        {
            if (_isFirst)
            {
                _isFirst = false;
                _cacheGeneric = value;
            }
            else if (Operations.GreaterThan(value, _cacheGeneric)) _cacheGeneric = value;
        }

        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "First", Description = "First Value", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void First(T value)
        {
            if (_isFirst)
            {
                _isFirst = false;
                _cacheGeneric = value;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Last", Description = "Last Value", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void Last(T value)
        {
            _cacheGeneric = value;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Count", Description = "Number of records", ResultMethod = nameof(CountResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
        public void Count()
        {
            _cacheCount++;
        }

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "CountTrue", Description = "Count where the value is true", ResultMethod = nameof(CountResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
        public void CountTrue(bool value)
        {
            
            if (value)
            {
                _cacheCount++;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "CountFalse", Description = "Count where the value is false", ResultMethod = nameof(CountResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
        public void CountFalse(bool value)
        {
            
            if (!value)
            {
                _cacheCount++;
            }
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "CountEqual", Description = "Count where the values are equal", ResultMethod = nameof(CountResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
        public void CountEqual(object[] values)
        {
            
            for (var i = 1; i < values.Length; i++)
            {
                if (!Equals(values[0], values[i])) return;
            }

            _cacheCount++;
        }


        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Count Distinct", Description = "Number if distinct values", ResultMethod = nameof(CountDistinctResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
        public void CountDistinct(T value)
        {
            if (_hashSet == null) _hashSet = new HashSet<T>();
            // if (value == null) value = NullPlaceHolder; //dictionary can't use nulls, so substitute null values.
            _hashSet.Add(value);
        }
        
        public int CountDistinctResult()
        {
            return _hashSet.Count;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Concatenate Aggregate", Description = "Returns concatenated string of repeating values.", ResultMethod = nameof(ConcatAggResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
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
        
        public string ConcatAggResult()
        {
            return _cacheStringBuilder.ToString();
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Aggregate Array", Description = "Aggregates the values into an array variable.", ResultMethod = nameof(CreateArrayResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.String, GenericType = EGenericType.All)]
        public void CreateArray(T value)
        {
            if (_cacheList == null)
            {
                _cacheList = new List<T>();
            }
            
            _cacheList.Add(value);
        }
        
        public T[] CreateArrayResult(bool sortResult = true)
        {
            var array = _cacheList.ToArray();
            if (sortResult)
            {
                Array.Sort(array);
            }
            return array;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Median", Description = "The median value in a series", ResultMethod = nameof(MedianResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void Median(T value)
        {
            if (_cacheList == null) _cacheList = new List<T>();
            _cacheList.Add(value);
        }
        public T MedianResult()
        {
            if (_cacheList == null)
                return default(T);
            var sorted = _cacheList.OrderBy(c => c).ToArray();
            var count = sorted.Length;

            if (count % 2 == 0)
            {
                // count is even, average two middle elements
                var a = sorted[count / 2 - 1];
                var b = sorted[count / 2];
                return Operations.DivideInt(Operations.Add(a, b), 2);
            }
            // count is odd, return the middle element
            return sorted[count / 2];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Standard Deviation", Description = "The standard deviation in a set of numbers", ResultMethod = nameof(StdDevResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void StdDev(double value)
        {
            Variance(value);
        }

        public double StdDevResult()
        {
            var sd = Math.Sqrt(VarianceResult());
            return sd;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Variance", Description = "The variance in a set of numbers.", ResultMethod = nameof(VarianceResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void Variance(double value)
        {
            if (_doubleList == null) _doubleList = new List<double>();

            _doubleList.Add(value);
            _cacheCount++;
            _cacheDouble += value;
        }

        public double VarianceResult()
        {
            if (_doubleList == null || _cacheCount == 0 )
                return 0;

            var average = _cacheDouble / _cacheCount;
            var sumOfSquaresOfDifferences = _doubleList.Select(val => (val - average) * (val - average)).Sum();
            var sd = sumOfSquaresOfDifferences / _cacheCount;

            return sd;
        }
        
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "First When", Description = "First resultValue when the condition = conditionValue", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void FirstWhen(bool whenTrue, T resultValue)
        {
            if (whenTrue && _isFirst)
            {
                _cacheGeneric = resultValue;
                _isFirst = false;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Last When", Description = "Last resultValue when the condition = conditionValue", ResultMethod = nameof(GenericResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void LastWhen(bool whenTrue, T resultValue)
        {
            if (whenTrue)
            {
                _cacheGeneric = resultValue;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Pivot to Columns", 
            Description = "Pivots the labelColumn and valueColumn into separate columns specified by the labels.  Returns true if all labels are found, false is some are missing.", ResultMethod = nameof(PivotToColumnsResult), ResetMethod = nameof(Reset), GenericType = EGenericType.All)]
        public void PivotToColumns(
            [TransformFunctionParameter(Name = "Column to Pivot")] string labelColumn, 
            [TransformFunctionParameter(Name = "Value to Pivot")] T valueColumn, 
            [TransformFunctionLinkedParameter("Columns")] string[] labels)
        {
            if (_cacheDictionary == null)
            {
                _cacheDictionary = new Dictionary<object, T>();
                foreach (var label in labels)
                {
                    _cacheDictionary.Add(label, default(T));
                }
            }

            if (_cacheDictionary.ContainsKey(labelColumn))
            {
                _cacheDictionary[labelColumn] = valueColumn;
            }
        }
        
        public bool PivotToColumnsResult([TransformFunctionLinkedParameter("Columns")] out T[] values)
        {
            values = _cacheDictionary.Values.ToArray();
            return !values.Contains(default(T));
        }
        
        public enum EPercentFormat
        {
            AsDecimal = 1,
            AsPercent
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Percent of Total", Description = "The percentage total in the group.", ResultMethod = nameof(PercentTotalResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void PercentTotal(T value)
        {
            if (_cacheList == null)
            {
                _cacheList = new List<T>();
                _cacheGeneric = default(T);
                _oneHundred = Operations.Parse<T>(100);
            }
            _cacheGeneric = Operations.Add(_cacheGeneric, value);
            _cacheList.Add(value);        
        }

        public T PercentTotalResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, EPercentFormat percentFormat = EPercentFormat.AsPercent)
        {
            if (_cacheList == null || Operations.Equal(_cacheGeneric, default(T)))
                return default(T);

            var percent = Operations.Divide(_cacheList[index], _cacheGeneric);
            
            return percentFormat == EPercentFormat.AsDecimal ? percent : Operations.Multiply(percent, _oneHundred);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Rank", Description = "The ranking (starting at 1) of the item within the group", ResultMethod = nameof(RankResult), ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public void Rank(T[] values)
        {
            if (_sortedRowsDictionary == null)
            {
                _sortedRowsDictionary = new SortedRowsDictionary<T>();
                _cacheCount = 0;
            }

            if (_sortedRowsDictionary.ContainsKey(values))
            {
                var indexes = _sortedRowsDictionary[values];
                Array.Resize(ref indexes, indexes.Length + 1);
                indexes[indexes.Length-1] = _cacheCount;
                _sortedRowsDictionary[values] = indexes;
            }
            else
            {
                _sortedRowsDictionary.Add(values, new object[] {_cacheCount});    
            }

            _cacheCount++;
        }

        public int RankResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, ESortDirection sortSortDirection)
        {
            if (_cacheArray == null)
            {
                _cacheArray = new object[_cacheCount];
                int rank;
                int increment;
                if (sortSortDirection == ESortDirection.Ascending)
                {
                    rank = 1;
                    increment = 1;
                }
                else
                {
                    rank = _sortedRowsDictionary.Count;
                    increment = -1;
                }
                
                foreach (var item in _sortedRowsDictionary)
                {
                    foreach (var value in item.Value)
                    {
                        _cacheArray[(int) value] = rank;    
                    }
                    
                    rank += increment * item.Value.Length;
                }
            }

            return (int)_cacheArray[index];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Running Count", Description = "The running count of rows in the current group.", ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Int32, GenericType = EGenericType.Numeric)]
        public int RunningCount()
        {
            _cacheCount++;
            return _cacheCount;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Running Sum", Description = "The running sum of rows in the current group.", ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public T RunningSum(T value)
        {
            _cacheGeneric = Operations.Add(_cacheGeneric, value);
            return _cacheGeneric;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Running Average", Description = "The running average of rows in the current group.", ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public T RunningAverage(T value)
        {
            _cacheGeneric = Operations.Add(_cacheGeneric, value);
            _cacheCount++;
            return Operations.DivideInt(_cacheGeneric, _cacheCount);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Previous Row Change", Description = "The change from the previous row value to the current.", ResetMethod = nameof(Reset), GenericTypeDefault = ETypeCode.Decimal, GenericType = EGenericType.Numeric)]
        public T PreviousRowChange(T value)
        {
            var result = Operations.Subtract(value, _cacheGeneric);
            _cacheGeneric = value;
            return result;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Moving Average", Description = "Calculates the average of the last (pre-count) points and the future (post-count) points.", ResultMethod = nameof(MovingAverageResult), ResetMethod = nameof(Reset), GenericType = EGenericType.Numeric)]
        public void MovingAverage(T value)
        {
            if (_cacheList == null)
            {
                _cacheList = new List<T>();
            }
            
            _cacheList.Add(value);
        }

        public T MovingAverageResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, int preCount, int postCount)
        {
            var lowIndex = index < preCount ? 0 : index - preCount;
            var valueCount = _cacheList.Count;
            var highIndex = postCount + index + 1;
            if (highIndex > valueCount) highIndex = valueCount;

            T sum = default;
            var denominator = highIndex - lowIndex;

            for (var i = lowIndex; i < highIndex; i++)
            {
                sum = Operations.Add(sum, _cacheList[i]);
            }
            
            //return the result.
            if (denominator == 0)
            {
                return default;
            }

            return Operations.DivideInt(sum, denominator);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Moving Sum", Description = "Calculates the sum of the last (pre-count) points and the future (post-count) points.", ResultMethod = nameof(MovingSumResult), ResetMethod = nameof(Reset), GenericType = EGenericType.Numeric)]
        public void MovingSum(T value)
        {
            if (_cacheList == null)
            {
                _cacheList = new List<T>();
            }
            
            _cacheList.Add(value);
        }

        public T MovingSumResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, int preCount, int postCount)
        {
            var lowIndex = index < preCount ? 0 : index - preCount;
            var valueCount = _cacheList.Count;
            var highIndex = postCount + index + 1;
            if (highIndex > valueCount) highIndex = valueCount;

            T sum = default;

            for (var i = lowIndex; i < highIndex; i++)
            {
                sum = Operations.Add(sum, _cacheList[i]);
            }
            
            return sum;
        }
    }
}