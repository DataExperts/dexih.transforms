using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace dexih.functions
{
    /// <summary>
    /// A converter that converts System.Object similar to Newtonsoft's JSON.Net.
    /// Only primitives are the same; arrays and objects do not result in the same types.
    /// </summary>
    public class JsonObjectConverter : JsonConverter<object>
    {
        public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.None:
                    break;
                case JsonTokenType.StartObject:
                    break;
                case JsonTokenType.EndObject:
                    break;
                case JsonTokenType.StartArray:
                    var elements = new List<object>();

                    if (!reader.Read())
                    {
                        throw new JsonException();
                    }
                    
                    while (reader.TokenType != JsonTokenType.EndArray)
                    {
                        elements.Add(JsonSerializer.Deserialize<object>(ref reader, options));

                        if (!reader.Read())
                        {
                            throw new JsonException();
                        }
                    }

                    return elements.ToArray();
                case JsonTokenType.EndArray:
                    throw new JsonException();
                case JsonTokenType.PropertyName:
                    break;
                case JsonTokenType.Comment:
                    break;
                case JsonTokenType.String:
                    if (reader.TryGetDateTime(out var datetime))
                    {
                        return datetime;
                    }

                    return reader.GetString();
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out var l))
                    {
                        return l;
                    }
                    return reader.GetDouble();
                case JsonTokenType.True:
                    return true;
                case JsonTokenType.False:
                    return false;
                case JsonTokenType.Null:
                    return null;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            // Use JsonElement as fallback.
            // Newtonsoft uses JArray or JObject.
            using (var document = JsonDocument.ParseValue(ref reader))
            {
                return document.RootElement.Clone();
            }
            
        }

        public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        {
            throw new InvalidOperationException("Should not get here.");
        }
    }
}