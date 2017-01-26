using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace dexih.functions
{
    public class Json
    {
        public static string SerializeObject(object value, string encryptionKey)
        {
            if(value == null)
            {
                return null;
            }
            if(encryptionKey == null)
            {
                return JsonConvert.SerializeObject(value, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
            }
            
            return JsonConvert.SerializeObject(value, new JsonSerializerSettings { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
        }

        public static T DeserializeObject<T>(string value, string encryptionKey)
        {
            if(String.IsNullOrEmpty(value))
            {
                return default(T);
            }
            if(encryptionKey == null)
            {
                return JsonConvert.DeserializeObject<T>(value, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
            }

            return JsonConvert.DeserializeObject<T>(value, new JsonSerializerSettings { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
        }

        public static JToken JTokenFromObject(object value, string encryptionKey)
        {
            if(value == null)
            {
                return null;
            }
            if(encryptionKey == null)
            {
                return JToken.FromObject(value, new JsonSerializer { ContractResolver = new CamelCasePropertyNamesContractResolver() });
            }

            return JToken.FromObject(value, new JsonSerializer { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
        }
        
    }
}
