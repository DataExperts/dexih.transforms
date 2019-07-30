using System;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions
{
    /// <summary>
    /// Used by series functions to store values in the series.
    /// </summary>
    public class SeriesValue<T>
    {
        public SeriesValue(object series, T value, SelectColumn.EAggregate aggregate)
        {
            Series = series;
            Value = value;
            Count = 1;
            Aggregate = aggregate;

            _addFunc = Operations<T>.Add.Value;
            _divFunc = Operations<T>.DivideInt.Value;
            _lessThanFunc = Operations<T>.LessThan.Value;
            _greaterThanFunc = Operations<T>.GreaterThan.Value;
        }

        private Func<T, T, T> _addFunc;
        private Func<T, int, T> _divFunc;
        private Func<T, T, bool> _lessThanFunc;
        private Func<T, T, bool> _greaterThanFunc;

        public object Series { get; set; }
        public T Value { get; set; }
        public int Count { get; set; }
        public SelectColumn.EAggregate Aggregate { get; set; }

        public void AddValue(T value)
        {
            Count++;

            switch (Aggregate)
            {
                case SelectColumn.EAggregate.Sum:
                case SelectColumn.EAggregate.Average:
                    Value = _addFunc(Value, value);
                    break;
                case SelectColumn.EAggregate.Min:
                    if (_lessThanFunc(value, Value))
                    {
                        Value = value;
                    }

                    break;
                case SelectColumn.EAggregate.Max:
                    if (_greaterThanFunc(value, Value))
                    {
                        Value = value;
                    }

                    break;
                case SelectColumn.EAggregate.Count:
                    break;
                case SelectColumn.EAggregate.First:
                    break;
                case SelectColumn.EAggregate.Last:
                    Value = value;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(Aggregate), Aggregate, null);
            }
        }

        public T Result
        {
            get
            {
                if (Aggregate == SelectColumn.EAggregate.Average)
                {
                    return _divFunc(Value, Count);
                }

                return Value;
            }
        }
    }
}