using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace dexih.functions
{
    public class JsonTimeSpanConverter: JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var value = reader.GetString();
                if (string.IsNullOrEmpty(value))
                {
                    return default;
                }

                return TimeSpan.Parse(reader.GetString());
            }

            return default;
        }

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString());
        }
    }
}