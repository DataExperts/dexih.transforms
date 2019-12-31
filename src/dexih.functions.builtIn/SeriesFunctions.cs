using System;
using System.Collections.Specialized;
using dexih.functions.Query;

namespace dexih.functions.BuiltIn
{
    public class SeriesValue
    {
        public SeriesValue(object series, double value, EAggregate aggregate)
        {
            Series = series;
            Value = value;
            Count = 1;
            Aggregate = aggregate;
        }
        
        public object Series { get; set; }
        public double Value { get; set; }
        public int Count { get; set; }
        public EAggregate Aggregate { get; set; }

        public void AddValue(double value)
        {
            Count++;
            
            switch (Aggregate)
            {
                case EAggregate.Sum:
                case EAggregate.Average:
                    Value += value;
                    break;
                case EAggregate.Min:
                    if (value < Value)
                    {
                        Value = value;
                    }
                    break;
                case EAggregate.Max:
                    if (value > Value)
                    {
                        Value = value;
                    }
                    break;
                case EAggregate.Count:
                    break;
                case EAggregate.First:
                    break;
                case EAggregate.Last:
                    Value = value;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(Aggregate), Aggregate, null);
            }
        }

        public double Result()
        {
            if (Aggregate == EAggregate.Average)
            {
                return Value / Count;
            }

            return Value;
        }
    }
    
    public class SeriesFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private OrderedDictionary _cacheSeries;
        
        public bool Reset()
        {
            _cacheSeries?.Clear();
            return true;
        }

        private void AddSeries(object series, double value, EAggregate duplicateAggregate)
        {
            if (_cacheSeries == null)
            {
                _cacheSeries = new OrderedDictionary();
            }

            if (_cacheSeries.Contains(series))
            {
                var current = (SeriesValue) _cacheSeries[series];
                current.AddValue(value);
            }
            else
            {
                _cacheSeries.Add(series, new SeriesValue(series, value, duplicateAggregate));
            }
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Series", Name = "Moving Average", Description = "Calculates the average of the last (pre-count) points and the future (post-count) points.", ResultMethod = nameof(MovingAverageResult), ResetMethod = nameof(Reset))]
        public void MovingAverage([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            AddSeries(series, value, duplicateAggregate);
        }

        public double MovingAverageResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, int preCount, int postCount)
        {
            var lowIndex = index < preCount ? 0 : index - preCount;
            var valueCount = _cacheSeries.Count;
            var highIndex = postCount + index + 1;
            if (highIndex > valueCount) highIndex = valueCount;

            double sum = 0;
            var denominator = highIndex - lowIndex;

            for (var i = lowIndex; i < highIndex; i++)
            {
                sum += ((SeriesValue)_cacheSeries[i]).Result();
            }

            //return the result.
            if (denominator == 0)
            {
                return 0;
            }

            return sum / denominator;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Series", Name = "Highest Value Since ", Description = "Return the last period that had a higher value than this.", ResultMethod = nameof(HighestSinceResult), ResetMethod = nameof(Reset))]
        public void HighestSince([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            AddSeries(series, value, duplicateAggregate);
        }

        public object HighestSinceResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, out int count, out double value)
        {
            var i = index - 1;
            var currentValue = ((SeriesValue)_cacheSeries[index]).Result();
            while (i > 0)
            {
                var checkValue = ((SeriesValue)_cacheSeries[i]).Result();
                if (checkValue > currentValue)
                {
                    value = checkValue;
                    count = index - i;
                    return ((SeriesValue) _cacheSeries[i]).Series;
                }
                i--;
            }
            
            // if no value found, the current value is the highest.
            value = ((SeriesValue) _cacheSeries[index]).Result();
            count = 0;
            return ((SeriesValue) _cacheSeries[index]).Series;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Series", Name = "Highest Value Since ", Description = "Return the last period that had a lower value than this.", ResultMethod = nameof(LowestSinceResult), ResetMethod = nameof(Reset))]
        public void LowestSince([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            AddSeries(series, value, duplicateAggregate);
        }

        public object LowestSinceResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, out int count, out double value)
        {
            var i = index - 1;
            var currentValue = ((SeriesValue)_cacheSeries[index]).Result();
            while (i > 0)
            {
                var checkValue = ((SeriesValue)_cacheSeries[i]).Result();
                if (checkValue < currentValue)
                {
                    value = checkValue;
                    count = index - i;
                    return ((SeriesValue) _cacheSeries[i]).Series;
                }
                i--;
            }
            
            // if no value found, the current value is the highest.
            value = ((SeriesValue) _cacheSeries[index]).Result();
            count = 0;
            return ((SeriesValue) _cacheSeries[index]).Series;
        }
        
        
    }
}