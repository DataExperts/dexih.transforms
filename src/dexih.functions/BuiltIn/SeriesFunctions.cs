using System;
using System.Collections.Generic;

namespace dexih.functions.BuiltIn
{
    public class SeriesFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private List<KeyValuePair<DateTime, double>> _cacheSeriesList;
        private Dictionary<string, int> _cacheIntDictionary;

        public bool Reset()
        {
            _cacheSeriesList = null;
            _cacheIntDictionary = null;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Series", Name = "Moving Average", Description = "Calculates the average of the last (pre-count) points and the future (post-count) points.", ResultMethod = nameof(MovingAverageResult), ResetMethod = nameof(Reset))]
        public void MovingAverage(DateTime series, double value, int preCount, int postCount)
        {
            if (_cacheSeriesList == null)
            {
                _cacheIntDictionary = new Dictionary<string, int> {{"PreCount", preCount}, {"PostCount", preCount}};
                _cacheSeriesList = new List<KeyValuePair<DateTime, double>>();
            }
            _cacheSeriesList.Add(new KeyValuePair<DateTime, double>(series, value));
        }

        public double MovingAverageResult([TransformFunctionIndex]int index)
        {
            var lowDate = _cacheSeriesList[index].Key.AddDays(-_cacheIntDictionary["PreCount"]);
            var highDate = _cacheSeriesList[index].Key.AddDays(_cacheIntDictionary["PostCount"]);
            var valueCount = _cacheSeriesList.Count;

            double sum = 0;
            var denominator = 0;

            //loop backwards from the index to sum the before items.
            for (var i = index; i >= 0; i--)
            {
                if (_cacheSeriesList[i].Key < lowDate)
                    break;
                sum += _cacheSeriesList[i].Value;
                denominator++;
            }

            //loop forwards from the index+1 to sum the after items.
            for (var i = index + 1; i < valueCount; i++)
            {
                if (_cacheSeriesList[i].Key > highDate)
                    break;
                sum += _cacheSeriesList[i].Value;
                denominator++;
            }
            

            //return the result.
            if (denominator == 0)
                return 0;
            return sum / denominator;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Series", Name = "Highest Value Since ", Description = "Provides the last date that had a higher value than this.", ResultMethod = nameof(HighestSinceResult), ResetMethod = nameof(Reset))]
        public void HighestSince(DateTime series, double value)
        {
            if (_cacheSeriesList == null)
            {
                _cacheSeriesList = new List<KeyValuePair<DateTime, double>>();
            }
            _cacheSeriesList.Add(new KeyValuePair<DateTime, double>(series, value));
        }

        public DateTime HighestSinceResult([TransformFunctionIndex]int index, out double value)
        {
            var i = index - 1;
            var currentValue = _cacheSeriesList[index].Value;
            while (i > 0)
            {
                if (_cacheSeriesList[i].Value > currentValue)
                {
                    value = _cacheSeriesList[i].Value;
                    return _cacheSeriesList[i].Key;
                }
                i--;
            }
            value = _cacheSeriesList[index].Value;
            return _cacheSeriesList[index].Key;
        }
    }
}