using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace dexih.functions
{
    public class JsonDoubleConverter: JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var value = reader.GetString();
                
                if (string.IsNullOrEmpty(value))
                {
                    return default;
                }

                switch (value)
                {
                    case "NaN":
                        return double.NaN;
                    case "∞":
                        return double.PositiveInfinity;
                    case "-∞":
                        return double.NegativeInfinity;
                    default:
                        return double.Parse(value);
                }
            }

            return reader.GetDouble();
        }

        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
        {
            if (double.IsFinite(value))
            {
                writer.WriteNumberValue(value);
                return;
            }
            if (double.IsNaN(value))
            {
                writer.WriteStringValue("NaN");
                return;
            }
            if (double.IsPositiveInfinity(value))
            {
                writer.WriteStringValue("∞");
                return;
            }
            if (double.IsNegativeInfinity(value))
            {
                writer.WriteStringValue("-∞");
                return;
            }

            throw new JsonException($"The double value {value} could not be converted to json.");
        }

    }
}