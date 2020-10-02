using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace dexih.functions
{
    public class JsonFloatConverter: JsonConverter<float>
    {
        public override float Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
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
                        return float.NaN;
                    case "∞":
                        return float.PositiveInfinity;
                    case "-∞":
                        return float.NegativeInfinity;
                    default:
                        return float.Parse(value);
                }
            }

            return reader.GetSingle();
        }

        public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options)
        {
            if (float.IsFinite(value))
            {
                writer.WriteNumberValue(value);
                return;
            }
            if (float.IsNaN(value))
            {
                writer.WriteStringValue("NaN");
                return;
            }
            if (float.IsPositiveInfinity(value))
            {
                writer.WriteStringValue("∞");
                return;
            }
            if (float.IsNegativeInfinity(value))
            {
                writer.WriteStringValue("-∞");
                return;
            }

            throw new JsonException($"The float value {value} could not be converted to json.");
        }

    }
}