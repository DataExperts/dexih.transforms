using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
            return JsonConvert.SerializeObject(value, new JsonSerializerSettings { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
        }

        public static T DeserializeObject<T>(string value, string encryptionKey)
        {
            if(string.IsNullOrEmpty(value))
            {
                return default(T);
            }
            return JsonConvert.DeserializeObject<T>(value, new JsonSerializerSettings { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
        }

		public static T JTokenToObject<T>(JToken value, string encryptionKey)
		{
			return DeserializeObject<T>(value.ToString(), encryptionKey);
			//if (encryptionKey == null)
			//{
			//	return value.ToObject<T>(new JsonSerializer { ContractResolver = new CamelCasePropertyNamesContractResolver() });
			//}

			//return value.ToObject<T>(new JsonSerializer { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
		}

        public static JToken JTokenFromObject(object value, string encryptionKey)
        {
            if(value == null)
            {
                return null;
            }
            return JToken.FromObject(value, new JsonSerializer { ContractResolver = new EncryptedStringPropertyResolver(encryptionKey) });
        }
        
    }
}
