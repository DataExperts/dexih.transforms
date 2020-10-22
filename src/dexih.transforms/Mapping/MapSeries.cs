using System;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;



namespace dexih.transforms.Mapping
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ESeriesGrain
    {
        Second = 1,
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
        public MapSeries(TableColumn inputColumn, TableColumn outputColumn, ESeriesGrain seriesGrain, int seriesStep, bool seriesFill, object seriesStart, object seriesFinish, int seriesProject)
        {
            InputColumn = inputColumn;
            OutputColumn = outputColumn;
            SeriesGrain = seriesGrain;
            SeriesStep = seriesStep;
            SeriesFill = seriesFill;
            SeriesStart = seriesStart;
            SeriesFinish = seriesFinish;
            SeriesProject = seriesProject;
        }

        public MapSeries(TableColumn inputColumn, ESeriesGrain seriesGrain, int seriesStep, bool seriesFill, object seriesStart, object seriesFinish, int seriesProject)
        {
            InputColumn = inputColumn;
            OutputColumn = inputColumn;
            SeriesGrain = seriesGrain;
            SeriesStep = seriesStep;
            SeriesFill = seriesFill;
            SeriesStart = seriesStart;
            SeriesFinish = seriesFinish;
            SeriesProject = seriesProject;
        }

        public ESeriesGrain SeriesGrain { get; set; }
        
        public int SeriesStep { get; set; }
        public DayOfWeek StartOfWeek { get; set; } = DayOfWeek.Sunday;
        public bool SeriesFill { get; set; }
        public object SeriesStart { get; set; }
        public object SeriesFinish { get; set; }
        
        public int SeriesProject { get; set; }

        public object GetSeriesStart()
        {
            if (SeriesStart == null) return null;
            if (SeriesStart is string s && string.IsNullOrWhiteSpace(s))
            {
                return null;
            }

            return Operations.Parse(InputColumn.DataType, SeriesStart);
        }

        public object GetSeriesFinish()
        {
            if (SeriesFinish == null) return null;
            if (SeriesFinish is string s && string.IsNullOrWhiteSpace(s))
            {
                return null;
            }
            
            return Operations.Parse(InputColumn.DataType, SeriesFinish);
        }

        public override object GetOutputValue(object[] row = null)
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

            if (SeriesGrain == ESeriesGrain.Year)
            {
                try
                {
                    var year = Operations.Parse<int>(value);
                    if (year < 0 || year > 9999)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Cannot create a series grain of {SeriesGrain} on the value {year}");
                    }

                    return year;

                }
                catch (ArgumentOutOfRangeException)
                {
                    throw;
                }
                catch(Exception) 
                {
                    throw new Exception($"Cannot create a series grain of {SeriesGrain} on the data type {value.GetType().Name}");    
                }
            }

            if (SeriesGrain == ESeriesGrain.Number)
            {
                switch (value)
                {
                    case ushort valueShort:
                        return valueShort;
                    case uint valueUInt:
                        return valueUInt;
                    case ulong valueULong:
                        return valueULong;
                    case short valueShort:
                        return valueShort;
                    case int valueInt:
                        return valueInt;
                    case long valueLong:
                        return valueLong;
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
        

        
        public object SeriesValue(bool next, object[] row = null)
        {
            var value = GetOutputValue(row);
            return next ? CalculateNextValue(value) : value;
        }

        public object CalculateNextValue(object value)
        {
            if (value is DateTime dateValue)
            {
                switch (SeriesGrain)
                {
                    case ESeriesGrain.Second:
                        return dateValue.AddSeconds(SeriesStep);
                    case ESeriesGrain.Minute:
                        return dateValue.AddMinutes(SeriesStep);
                    case ESeriesGrain.Hour:
                        return dateValue.AddHours(SeriesStep);
                    case ESeriesGrain.Day:
                        return dateValue.AddDays(SeriesStep);
                    case ESeriesGrain.Week:
                        return dateValue.AddDays(SeriesStep * 7);
                    case ESeriesGrain.Month:
                        return dateValue.AddMonths(SeriesStep);
                    case ESeriesGrain.Year:
                        return dateValue.AddYears(SeriesStep);
                    case ESeriesGrain.Number:
                        throw new Exception("Can generate an integer series on a date column.");
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            
            if (SeriesGrain == ESeriesGrain.Year)
            {
                try
                {
                    var year = Operations.Parse<int>(value);
                    if (year < 0 || year > 9999)
                    {
                        throw new ArgumentOutOfRangeException($"Cannot create a series grain of {SeriesGrain} on the value {year}");
                    }

                    return year + SeriesStep;
                }
                catch (ArgumentOutOfRangeException)
                {
                    throw;
                }
                catch(Exception) 
                {
                    throw new Exception($"Cannot create a series grain of {SeriesGrain} on the data type {value.GetType().Name}");    
                }
            }

            if (SeriesGrain == ESeriesGrain.Number)
            {
                switch (value)
                {
                    case ushort valueShort:
                        return valueShort + SeriesStep;
                    case uint valueUInt:
                        return valueUInt + SeriesStep;
                    case ulong valueULong:
                        return valueULong + Convert.ToUInt64(SeriesStep);
                    case short valueShort:
                        return valueShort + SeriesStep;
                    case int valueInt:
                        return valueInt + SeriesStep;
                    case long valueLong:
                        return valueLong + SeriesStep;
                }
            }

            throw new Exception($"Can not create a series grain of {SeriesGrain} on data type {value.GetType().Name}.");

        }

        // public void ProcessNextValueOutput(int count, object[] row)
        // {
        //     var value = NextValue(count);
        //     row[OutputOrdinal] = value;
        // }

        public override void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            fillerRow[InputOrdinal] = seriesValue;
        }
        
        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            return false;
        }


    }
}