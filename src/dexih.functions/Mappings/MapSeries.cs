using System;
using dexih.functions;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions.Mappings
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ESeriesGrain
    {
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month,
        Year,
        Number
    }
    
    
    public class MapSeries: MapColumn
    {
        public MapSeries(TableColumn inputColumn, TableColumn outputColumn, ESeriesGrain seriesGrain, bool seriesFill, object seriesStart, object seriesFinish)
        {
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
            SeriesGrain = seriesGrain;
            SeriesFill = seriesFill;
            SeriesStart = seriesStart;
            SeriesFinish = seriesFinish;
        }

        public MapSeries(TableColumn inputColumn, ESeriesGrain seriesGrain, bool seriesFill, object seriesStart, object seriesFinish)
        {
            InputColumn = inputColumn;
            OutputColumn = inputColumn;
            SeriesGrain = seriesGrain;
            SeriesGrain = seriesGrain;
            SeriesFill = seriesFill;
            SeriesStart = seriesStart;
            SeriesFinish = seriesFinish;
        }


        public ESeriesGrain SeriesGrain { get; set; }
        public DayOfWeek StartOfWeek { get; set; } = DayOfWeek.Sunday;
        public bool SeriesFill { get; set; }
        public object SeriesStart { get; set; }
        public object SeriesFinish { get; set; }

        public object GetSeriesStart()
        {
            return Operations.Parse(InputColumn.DataType, SeriesStart);
        }

        public object GetSeriesFinish()
        {
            return Operations.Parse(InputColumn.DataType, SeriesFinish);
        }

        public override object GetInputValue(object[] row = null)
        {
            object value;
            if (InputOrdinal == -1)
            {
                value = InputValue;
            }
            else
            {
                value = row == null ? RowData[InputOrdinal] : row[InputOrdinal];    
            }

            if (value is DateTime dateValue)
            {
                switch (SeriesGrain)
                {
                    case ESeriesGrain.Second:
                        return new DateTime(dateValue.Year, dateValue.Month, dateValue.Day, dateValue.Hour, dateValue.Minute, dateValue.Second);
                    case ESeriesGrain.Minute:
                        return new DateTime(dateValue.Year, dateValue.Month, dateValue.Day, dateValue.Hour, dateValue.Minute, 0);
                    case ESeriesGrain.Hour:
                        return new DateTime(dateValue.Year, dateValue.Month, dateValue.Day, dateValue.Hour, 0, 0);
                    case ESeriesGrain.Day:
                        return new DateTime(dateValue.Year, dateValue.Month, dateValue.Day, 0, 0, 0);
                    case ESeriesGrain.Week:
                        var newDate = new DateTime(dateValue.Year, dateValue.Month, dateValue.Day, 0, 0, 0);
                        var diff = (7 + (newDate.DayOfWeek - StartOfWeek)) % 7;
                        return newDate.AddDays(-1 * diff).Date;
                    case ESeriesGrain.Month:
                        return new DateTime(dateValue.Year, dateValue.Month, 1, 0, 0, 0);
                    case ESeriesGrain.Year:
                        return new DateTime(dateValue.Year, 1, 1, 0, 0, 0);
                    case ESeriesGrain.Number:
                        throw new Exception("Can not generate an integer series on a date column.");
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if (SeriesGrain == ESeriesGrain.Number)
            {
                switch (value)
                {
                    case ushort valueShort:
                        return valueShort + 1;
                    case uint valueUInt:
                        return valueUInt + 1;
                    case ulong valueULong:
                        return valueULong + 1;
                    case short valueShort:
                        return valueShort + 1;
                    case int valueInt:
                        return valueInt + 1;
                    case long valueLong:
                        return valueLong + 1;
                    case decimal valueDecimal:
                        return Math.Round(valueDecimal, 0);
                    case float valueFloat:
                        return Math.Round(valueFloat, 0);
                    case double valueDouble:
                        return Math.Round(valueDouble, 0);
                }
            }

            throw new Exception($"Can not create a series grain of {SeriesGrain} on data type {value.GetType().Name}.");
        }
        

        
        public object NextValue(int count, object[] row = null)
        {
            var value = GetInputValue(row);
            return CalculateNextValue(value, count);
            
        }

        public object CalculateNextValue(object value, int count)
        {
            if (value is DateTime dateValue)
            {
                switch (SeriesGrain)
                {
                    case ESeriesGrain.Second:
                        return dateValue.AddSeconds(count);
                    case ESeriesGrain.Minute:
                        return dateValue.AddMinutes(count);
                    case ESeriesGrain.Hour:
                        return dateValue.AddHours(count);
                    case ESeriesGrain.Day:
                        return dateValue.AddDays(count);
                    case ESeriesGrain.Week:
                        return dateValue.AddDays(count * 7);
                    case ESeriesGrain.Month:
                        return dateValue.AddMonths(count);
                    case ESeriesGrain.Year:
                        return dateValue.AddYears(count);
                    case ESeriesGrain.Number:
                        throw new Exception("Can generate an integer series on a date column.");
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if (SeriesGrain == ESeriesGrain.Number)
            {
                switch (value)
                {
                    case ushort valueShort:
                        return valueShort + count;
                    case uint valueUInt:
                        return valueUInt + count;
                    case ulong valueULong:
                        return valueULong + Convert.ToUInt64(count);
                    case short valueShort:
                        return valueShort + count;
                    case int valueInt:
                        return valueInt + count;
                    case long valueLong:
                        return valueLong + count;
                }
            }

            throw new Exception($"Can not create a series grain of {SeriesGrain} on data type {value.GetType().Name}.");

        }

        public void ProcessNextValueOutput(int count, object[] row)
        {
            var value = NextValue(count);
            row[OutputOrdinal] = value;
        }

        public override void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            fillerRow[InputOrdinal] = seriesValue;
        }

    }
}