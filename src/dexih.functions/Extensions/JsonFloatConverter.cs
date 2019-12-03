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

                if (value == "NaN")
                {
                    return float.NaN;
                }
            }

            return reader.GetSingle();
        }

        public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options)
        {
            if (float.IsNaN(value))
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