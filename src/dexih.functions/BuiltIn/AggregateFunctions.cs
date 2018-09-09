using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class AggregateFunctions
    {
        private const string NullPlaceHolder = "A096F007-26EE-479E-A9E1-4E12427A5AF0"; //used a a unique string that can be substituted for null
        
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private int? _cacheInt;
        private double? _cacheDouble;
        private DateTime? _cacheDate;
        private string _cacheString;
        private Dictionary<object, object> _cacheDictionary;
        private List<object> _cacheList;
        private StringBuilder _cacheStringBuilder;
        private object _cacheObject;
        private object[] _cacheArray;
        private SortedRowsDictionary _sortedRowsDictionary = null;

        public bool Reset()
        {
            _cacheInt = null;
            _cacheDouble = null;
            _cacheDate = null;
            _cacheString = null;
            _cacheDictionary = null;
            _cacheList = null;
            _cacheDictionary = null;
            _cacheStringBuilder = null;
            _cacheObject = null;
            _cacheArray = null;
            _sortedRowsDictionary = null;
            return true;
        }
        
        public string StringResult() => _cacheString;
        public int IntResult() => _cacheInt??0;
        public double DoubleResult() => _cacheDouble??0;
        public DateTime? DateResult() => _cacheDate;
        public double? NullDoubleResult() => _cacheDouble;
        public object ObjectResult() => _cacheObject;

        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Sum", Description = "Sum of the values", ResultMethod = nameof(DoubleResult), ResetMethod = nameof(Reset))]
        public void Sum(double value)
        {
            
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
        }

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Average", Description = "Average of the values", ResultMethod = nameof(AverageResult), ResetMethod = nameof(Reset))]
        public void Average(double value)
        {
            if (_cacheInt == null) _cacheInt = 0;
            if (_cacheDouble == null) _cacheDouble = 0;
            _cacheDouble = _cacheDouble + value;
            _cacheInt = _cacheInt + 1;
        }
        public double AverageResult()
        {
            if (_cacheDouble == null || _cacheInt == null || _cacheInt == 0)
                return 0;

            return (double)_cacheDouble / (double)_cacheInt;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Minimum", Description = "Minimum Value", ResultMethod = nameof(ObjectResult), ResetMethod = nameof(Reset))]
        public void Min(object value)
        {
            if (_cacheObject == null) _cacheObject = value;
            else if (DataType.Compare(null, value, (_cacheObject)) == DataType.ECompareResult.Less) _cacheObject = value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Maximum", Description = "Maximum Value", ResultMethod = nameof(ObjectResult), ResetMethod = nameof(Reset))]
        public void Max(object value)
        {
            if (_cacheObject == null) _cacheObject = value;
            else if (DataType.Compare(null, value, (_cacheObject)) == DataType.ECompareResult.Greater) _cacheObject = value;
        }

        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "First", Description = "First Value", ResultMethod = nameof(ObjectResult), ResetMethod = nameof(Reset))]
        public void First(object value)
        {
            if (_cacheObject == null) _cacheObject = value;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Last", Description = "Last Value", ResultMethod = nameof(ObjectResult), ResetMethod = nameof(Reset))]
        public void Last(object value)
        {
            _cacheObject = value;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Count", Description = "Number of records", ResultMethod = nameof(IntResult), ResetMethod = nameof(Reset))]
        public void Count()
        {
            if (_cacheInt == null) _cacheInt = 1;
            else _cacheInt = _cacheInt + 1;
        }

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "CountTrue", Description = "Count where the value is true", ResultMethod = nameof(IntResult), ResetMethod = nameof(Reset))]
        public void CountTrue(bool value)
        {
            
            if (_cacheInt == null) _cacheInt = 0;
            if (value)
            {
                _cacheInt = _cacheInt + 1;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "CountFalse", Description = "Count where the value is false", ResultMethod = nameof(IntResult), ResetMethod = nameof(Reset))]
        public void CountFalse(bool value)
        {
            
            if (_cacheInt == null) _cacheInt = 0;
            if (!value)
            {
                _cacheInt = _cacheInt + 1;
            }
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "CountEqual", Description = "Count where the values are equal", ResultMethod = nameof(IntResult), ResetMethod = nameof(Reset))]
        public void CountEqual(object[] values)
        {
            
            if (_cacheInt == null) _cacheInt = 0;

            for (var i = 1; i < values.Length; i++)
            {
                if (!object.Equals(values[0], values[i])) return;
            }

            _cacheInt = _cacheInt + 1;
        }


        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Count Distinct", Description = "Number if distinct values", ResultMethod = nameof(CountDistinctResult), ResetMethod = nameof(Reset))]
        public void CountDistinct(object value)
        {
            if (_cacheDictionary == null) _cacheDictionary = new Dictionary<object, object>();
            if (value == null) value = NullPlaceHolder; //dictionary can't use nulls, so substitute null values.
            if (_cacheDictionary.ContainsKey(value) == false)
                _cacheDictionary.Add(value, null);
        }
        
        public int CountDistinctResult()
        {
            return _cacheDictionary.Keys.Count;
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
        
        public string ConcatAggResult()
        {
            return _cacheStringBuilder.ToString();
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Median", Description = "The median value in a series", ResultMethod = nameof(MedianResult), ResetMethod = nameof(Reset))]
        public void Median(double value)
        {
            if (_cacheList == null) _cacheList = new List<object>();
            _cacheList.Add(value);
        }
        public double MedianResult()
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

        public double StdDevResult()
        {
            var sd = Math.Sqrt(VarianceResult());
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

        public double VarianceResult()
        {
            if (_cacheList == null || _cacheInt == null || _cacheInt == 0 || _cacheDouble == null || _cacheDouble == 0 )
                return 0;

            var average = (double)_cacheDouble / (double)_cacheInt;
            var sumOfSquaresOfDifferences = _cacheList.Select(val => ((double)val - average) * ((double)val - average)).Sum();
            var sd = sumOfSquaresOfDifferences / (double)_cacheInt;

            return sd;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "MinimumDate", Description = "Minimum Date", ResultMethod = nameof(DateResult), ResetMethod = nameof(Reset))]
        public void MinDate(DateTime value)
        {
            if (_cacheDate == null) _cacheDate = value;
            else if (value < _cacheDate) _cacheDate = value;
        }

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "MaximumDate", Description = "Maximum Date", ResultMethod = nameof(DateResult), ResetMethod = nameof(Reset))]
        public void MaxDate(DateTime value)
        {
            if (_cacheDate == null) _cacheDate = value;
            else if (value > _cacheDate) _cacheDate = value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "First When", Description = "First resultValue when the condition = conditionValue", ResultMethod = nameof(ObjectResult), ResetMethod = nameof(Reset))]
        public void FirstWhen(object condition, object conditionValue, object resultValue)
        {
            if (Equals(condition, conditionValue) && _cacheObject == null)
            {
                _cacheObject = resultValue;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Last When", Description = "Last resultValue when the condition = conditionValue", ResultMethod = nameof(ObjectResult), ResetMethod = nameof(Reset))]
        public void LastWhen(object condition, object conditionValue, object resultValue)
        {
            if (Equals(condition, conditionValue))
            {
                _cacheObject = resultValue;
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Pivot to Columns", 
            Description = "Pivots the labelColum and valueColumn into separate columns specified by the labels.  Returns true if all labels are found, false is some are missing.", ResultMethod = nameof(PivotToColumnsResult), ResetMethod = nameof(Reset))]
        public void PivotToColumns(string labelColumn, object valueColumn, [TransformFunctionParameter(TwinParameterName = "values")] object[] labels)
        {
            if (_cacheDictionary == null)
            {
                _cacheDictionary = new Dictionary<object, object>();
                foreach (var label in labels)
                {
                    _cacheDictionary.Add(label, null);
                }
            }

            if (_cacheDictionary.ContainsKey(labelColumn))
            {
                _cacheDictionary[labelColumn] = valueColumn;
            }
        }
        
        public bool PivotToColumnsResult(out object[] values)
        {
            values = _cacheDictionary.Values.ToArray();
            return !values.Contains(null);
        }
        
        public enum EPercentFormat
        {
            AsDecimal,
            AsPercent
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Percent of Total", Description = "The percentage total in the group.", ResultMethod = nameof(PercentTotalResult), ResetMethod = nameof(Reset))]
        public void PercentTotal(double value)
        {
            if (_cacheList == null)
            {
                _cacheList = new List<object>();
                _cacheDouble = 0;
            }
            _cacheDouble += value;
            _cacheList.Add(value);        
        }

        public double PercentTotalResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, EPercentFormat percentFormat = EPercentFormat.AsPercent)
        {
            if (_cacheList == null || _cacheDouble == null)
                return 0;

            var percent = (double)_cacheList[index] / _cacheDouble.Value;
            return percentFormat == EPercentFormat.AsDecimal ? percent : percent * 100;
        }


        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Aggregate", Name = "Rank", Description = "The ranking (starting at 1) of the item within the group", ResultMethod = nameof(RankResult), ResetMethod = nameof(Reset))]
        public void Rank(object[] values)
        {
            if (_sortedRowsDictionary == null)
            {
                _sortedRowsDictionary = new SortedRowsDictionary();
                _cacheInt = 0;
            }

            if (_sortedRowsDictionary.ContainsKey(values))
            {
                var indexes = _sortedRowsDictionary[values];
                Array.Resize(ref indexes, indexes.Length + 1);
                indexes[indexes.Length-1] = _cacheInt;
                _sortedRowsDictionary[values] = indexes;
            }
            else
            {
                _sortedRowsDictionary.Add(values, new object[] {_cacheInt});    
            }

            _cacheInt++;
        }

        public int RankResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, Sort.EDirection sortDirection)
        {
            if (_cacheArray == null)
            {
                _cacheArray = new object[_cacheInt.Value];
                int rank;
                int increment;
                if (sortDirection == Sort.EDirection.Ascending)
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
    }
}