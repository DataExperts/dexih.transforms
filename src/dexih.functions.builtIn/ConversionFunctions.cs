using System;
using System.Collections.Generic;
using dexih.functions.Exceptions;

namespace dexih.functions.BuiltIn
{
    public class ConversionFunctions
    {
        public enum ETemperatureScale
        {
            Fahrenheit = 1, Celsius, Kelvin
        }
        
        public enum ELengthScale
        {
            Kilometer = 1, Meter, Centimeter, Millimeter, Micrometer, Nanometer, Mile, Yard, Foot, Inch, NauticalMile
        }

        private static readonly Dictionary<ELengthScale, double> DistanceConvert = new Dictionary<ELengthScale, double>()
        {
            {ELengthScale.Millimeter, 0.001},
            {ELengthScale.Centimeter, 0.01},
            {ELengthScale.Meter, 1},
            {ELengthScale.Kilometer, 1000},
            {ELengthScale.Inch, 0.0254},
            {ELengthScale.Foot,  0.3048},
            {ELengthScale.Yard, 0.9144},
            {ELengthScale.Mile, 1609.344},
            {ELengthScale.NauticalMile, 1852}
        };
        
        private static readonly Dictionary<string, ELengthScale> DistanceLookup = new Dictionary<string, ELengthScale>()
        {
            {"millimeter", ELengthScale.Millimeter},
            {"millimeters", ELengthScale.Millimeter},
            {"mm", ELengthScale.Millimeter},
            {"cm", ELengthScale.Centimeter},
            {"centimeter", ELengthScale.Centimeter},
            {"centimeters", ELengthScale.Centimeter},
            {"m", ELengthScale.Meter},
            {"meter", ELengthScale.Meter},
            {"meters", ELengthScale.Meter},
            {"km", ELengthScale.Kilometer},
            {"kilometer", ELengthScale.Kilometer},
            {"kilometers", ELengthScale.Kilometer},
            {"in", ELengthScale.Inch},
            {"inch", ELengthScale.Inch},
            {"inches", ELengthScale.Inch},
            {"ft", ELengthScale.Foot},
            {"foot", ELengthScale.Foot},
            {"feet", ELengthScale.Foot},
            {"yd", ELengthScale.Yard},
            {"yard", ELengthScale.Yard},
            {"yards", ELengthScale.Yard},
            {"mi", ELengthScale.Mile},
            {"mile", ELengthScale.Mile},
            {"miles", ELengthScale.Mile},
            {"nm", ELengthScale.NauticalMile},
            {"nauticalmile", ELengthScale.NauticalMile},
            {"nauticalmiles", ELengthScale.NauticalMile},
            {"nautical mile", ELengthScale.NauticalMile},
            {"nautical miles", ELengthScale.NauticalMile},
        };

        public enum EMassScale
        {
            Tonne = 1,
            Kilogram,
            Gram,
            Milligram,
            Microgram,
            ImperialTon,
            UsTon,
            Stone,
            Pound,
            Ounce
        }
        
        private static readonly Dictionary<EMassScale, double> MassConvert = new Dictionary<EMassScale, double>()
        {
            {EMassScale.Tonne, 1000},
            {EMassScale.Kilogram, 1},
            {EMassScale.Gram, 0.001},
            {EMassScale.Milligram, 1e-6},
            {EMassScale.Microgram, 1e-9},
            {EMassScale.ImperialTon, 1016.05},
            {EMassScale.UsTon, 907.18749965598},
            {EMassScale.Stone, 6.3503124975918607831},
            {EMassScale.Pound, 0.45359374982799000042},
            {EMassScale.Ounce, 0.028349609364249375026},
        };
        
        private static readonly Dictionary<string, EMassScale> MassLookup = new Dictionary<string, EMassScale>()
        {
            {"t", EMassScale.Tonne},
            {"tonne", EMassScale.Tonne},
            {"tonnes", EMassScale.Tonne},
            {"kg", EMassScale.Kilogram},
            {"kilo", EMassScale.Kilogram},
            {"kilos", EMassScale.Kilogram},
            {"kilogram", EMassScale.Kilogram},
            {"kilograms", EMassScale.Kilogram},
            {"g", EMassScale.Gram},
            {"gram", EMassScale.Gram},
            {"grams", EMassScale.Gram},
            {"mg", EMassScale.Milligram},
            {"milligram", EMassScale.Milligram},
            {"milligrams", EMassScale.Milligram},
            {"mcg", EMassScale.Microgram},
            {"μg", EMassScale.Microgram},
            {"ug", EMassScale.Microgram},
            {"microgram", EMassScale.Microgram},
            {"micrograms", EMassScale.Microgram},
            {"microgramme ", EMassScale.Microgram},
            {"ton", EMassScale.ImperialTon},
            {"tons", EMassScale.ImperialTon},
            {"impreialton", EMassScale.ImperialTon},
            {"impreialtons", EMassScale.ImperialTon},
            {"st", EMassScale.Stone},
            {"stone", EMassScale.Stone},
            {"stones", EMassScale.Stone},
            {"lb", EMassScale.Pound},
            {"pound", EMassScale.Pound},
            {"pounds", EMassScale.Pound},
            {"oz", EMassScale.Ounce},
            {"ounce", EMassScale.Ounce},
            {"ounces", EMassScale.Ounce},
        };


        public enum ETimeScale
        {
            Nanosecond, Microsecond, Millisecond, Second, Minute, Hour, Day, Week, Month, Year
        }
        
        private static readonly Dictionary<ETimeScale, double> TimeConvert = new Dictionary<ETimeScale, double>()
        {
            {ETimeScale.Nanosecond, 1e-9},
            {ETimeScale.Microsecond, 1e-6},
            {ETimeScale.Millisecond, 1e-3},
            {ETimeScale.Second, 1},
            {ETimeScale.Minute, 60},
            {ETimeScale.Hour, 3600},
            {ETimeScale.Day, 86400},
            {ETimeScale.Week, 604800},
            {ETimeScale.Month, 2.628e+6},
            {ETimeScale.Year, 31535965.4396976},
        };
        
        private static readonly Dictionary<string, ETimeScale> TimeLookup = new Dictionary<string, ETimeScale>()
        {
            {"ns", ETimeScale.Nanosecond},
            {"nanosecond", ETimeScale.Nanosecond},
            {"nanoseconds", ETimeScale.Nanosecond},
            {"μs", ETimeScale.Microsecond},
            {"us", ETimeScale.Microsecond},
            {"microsecond", ETimeScale.Microsecond},
            {"microseconds", ETimeScale.Microsecond},
            {"ms", ETimeScale.Millisecond},
            {"millisecond", ETimeScale.Millisecond},
            {"milliseconds", ETimeScale.Millisecond},
            {"s", ETimeScale.Second},
            {"second", ETimeScale.Second},
            {"seconds", ETimeScale.Second},
            {"mi", ETimeScale.Minute},
            {"min", ETimeScale.Minute},
            {"mins", ETimeScale.Minute},
            {"minute", ETimeScale.Minute},
            {"minutes", ETimeScale.Minute},
            {"h", ETimeScale.Hour},
            {"hr", ETimeScale.Hour},
            {"hour", ETimeScale.Hour},
            {"hours", ETimeScale.Hour},
            {"d", ETimeScale.Day},
            {"day", ETimeScale.Day},
            {"days", ETimeScale.Day},
            {"w", ETimeScale.Week},
            {"week", ETimeScale.Week},
            {"weeks", ETimeScale.Week},
            {"mo", ETimeScale.Month},
            {"month", ETimeScale.Month},
            {"months", ETimeScale.Month},
            {"y", ETimeScale.Year},
            {"year", ETimeScale.Year},
            {"years", ETimeScale.Year},
        };
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Temperature",
            Description = "Converts fromTemperature from the fromScale to the toScale")]
        public double ConvertTemperature(double fromTemperature, ETemperatureScale fromScale, ETemperatureScale toScale)
        {
            if (fromScale == toScale) return fromTemperature;

            double kelvin;
            switch (fromScale)
            {
                case ETemperatureScale.Fahrenheit:
                    kelvin = fromTemperature - 32 * (5 / 9) + 273.15;
                    break;
                case ETemperatureScale.Celsius:
                    kelvin = fromTemperature + 273.15;
                    break;
                case ETemperatureScale.Kelvin:
                    kelvin = fromTemperature;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(fromScale), fromScale, null);
            }

            switch (toScale)
            {
                case ETemperatureScale.Fahrenheit:
                    return (kelvin - 273.15) * 9 / 5 + 32;
                case ETemperatureScale.Celsius:
                    return kelvin - 273.15;
                case ETemperatureScale.Kelvin:
                    return kelvin;
                default:
                    throw new ArgumentOutOfRangeException(nameof(toScale), fromScale, null);
                
            }
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Length",
            Description = "Converts fromLength from the fromScale to the toScale")]
        public double ConvertLength(double fromLength, ELengthScale fromScale, ELengthScale toScale)
        {
            if (fromScale == toScale) return fromLength;

            var meter = fromLength * DistanceConvert[fromScale];
            return meter / DistanceConvert[toScale];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Length (string scale)",
            Description = "Converts fromLength from the (string) fromScale to the toScale.")]
        public double ConvertLengthString(double fromLength, string fromScale, ELengthScale toScale)
        {
            if(DistanceLookup.TryGetValue(fromScale, out var convertedScale))
            {
                return ConvertLength(fromLength, convertedScale, toScale);
            }
            
            throw new FunctionException($"The length scale {fromScale} was not recognized.");
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Mass",
            Description = "Converts fromMass from the fromScale to the toScale")]
        public double ConvertMass(double fromMass, EMassScale fromScale, EMassScale toScale)
        {
            if (fromScale == toScale) return fromMass;

            var kilogram = fromMass * MassConvert[fromScale];
            return kilogram / MassConvert[toScale];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Mass (string scale)",
            Description = "Converts fromMass from the (string) fromMass to the toMass.")]
        public double ConvertMassString(double fromMass, string fromScale, EMassScale toScale)
        {
            if(MassLookup.TryGetValue(fromScale, out var convertedScale))
            {
                return ConvertMass(fromMass, convertedScale, toScale);
            }
            
            throw new FunctionException($"The mass scale {fromScale} was not recognized.");
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Time",
            Description = "Converts fromTime from the fromScale to the toScale")]
        public double ConvertTime(double fromTime, ETimeScale fromScale, ETimeScale toScale)
        {
            if (fromScale == toScale) return fromTime;

            var seconds = fromTime * TimeConvert[fromScale];
            return seconds / TimeConvert[toScale];
        }
        
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Conversion", Name = "Convert Time (string scale)",
            Description = "Converts fromTime from the (string) fromTime to the toTime.")]
        public double ConvertTimeString(double fromTime, string fromScale, ETimeScale toScale)
        {
            if(TimeLookup.TryGetValue(fromScale, out var convertedScale))
            {
                return ConvertTime(fromTime, convertedScale, toScale);
            }
            
            throw new FunctionException($"The time scale {fromScale} was not recognized.");
        }
        
    }
}