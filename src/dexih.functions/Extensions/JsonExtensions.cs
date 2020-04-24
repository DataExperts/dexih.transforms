using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.functions
{
    public static class JsonExtensions
    {
        public static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            Converters = { new JsonObjectConverter(), new JsonTimeSpanConverter(), new JsonDateTimeConverter(), new JsonDoubleConverter(), new JsonFloatConverter()}
//            IgnoreNullValues = true
        };

        public static T ToObject<T>(this JsonElement element)
        {
            var json = element.GetRawText();
            return JsonSerializer.Deserialize<T>(json, SerializerOptions);
        }

        public static T ToObject<T>(this JsonDocument document)
        {
            var json = document.RootElement.GetRawText();
            return JsonSerializer.Deserialize<T>(json, SerializerOptions);
        }
        
        public static string Serialize<T>(this T value)
        {
            return JsonSerializer.Serialize(value, SerializerOptions);
        }

        public static T Deserialize<T>(this string value, bool ignoreError = false)
        {
            try
            {
                return JsonSerializer.Deserialize<T>(value, SerializerOptions);
            }
            catch (Exception)
            {
                return default;
            }
        }

        public static object Deserialize(this string value, Type type)
        {
            return JsonSerializer.Deserialize(value, type, SerializerOptions);
        }

        public static JsonDocument ToJsonDocument(this object content)
        {
            var utf8JsonData = JsonSerializer.SerializeToUtf8Bytes(content);
            return JsonDocument.Parse(utf8JsonData);
        }

        public static Task SerializeAsync<T>(this Stream stream, T value, CancellationToken cancellationToken)
        {
            return JsonSerializer.SerializeAsync(stream, value, SerializerOptions, cancellationToken);
        }

        public static ValueTask<T> DeserializeAsync<T>(this Stream stream, CancellationToken cancellationToken)
        {
            return JsonSerializer.DeserializeAsync<T>(stream, SerializerOptions, cancellationToken);
        }
    }
}