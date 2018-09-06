using System;
using dexih.functions.Query;

namespace dexih.functions
{
    public class SeriesValue
    {
        public SeriesValue(object series, double value, SelectColumn.EAggregate aggregate)
        {
            Series = series;
            Value = value;
            Count = 1;
            Aggregate = aggregate;
        }
        
        public object Series { get; set; }
        public double Value { get; set; }
        public int Count { get; set; }
        public SelectColumn.EAggregate Aggregate { get; set; }

        public void AddValue(double value)
        {
            Count++;
            
            switch (Aggregate)
            {
                case SelectColumn.EAggregate.Sum:
                case SelectColumn.EAggregate.Average:
                    Value += value;
                    break;
                case SelectColumn.EAggregate.Min:
                    if (value < Value)
                    {
                        Value = value;
                    }
                    break;
                case SelectColumn.EAggregate.Max:
                    if (value > Value)
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

        public double Result()
        {
            if (Aggregate == SelectColumn.EAggregate.Average)
            {
                return Value / Count;
            }

            return Value;
        }
    }
}