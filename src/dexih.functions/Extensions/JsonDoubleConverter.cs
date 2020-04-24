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

                if (value == "NaN")
                {
                    return double.NaN;
                }
            }

            return reader.GetDouble();
        }

        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
        {
            if (double.IsNaN(value))
            {
                writer.WriteStringValue("NaN");    
            }
            else
            {
                writer.WriteNumberValue(value);
            }
            
        }

    }
}