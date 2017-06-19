using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace dexih.functions
{
    [AttributeUsage(AttributeTargets.Property)]
    public class JsonEncryptAttribute : Attribute
    {
    }

    public class EncryptedStringPropertyResolver : CamelCasePropertyNamesContractResolver
    {
        private readonly string _encryptionKey;

        public EncryptedStringPropertyResolver(string encryptionKey)
        {
            _encryptionKey = encryptionKey;
        }

        protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
        {
            IList<JsonProperty> props = base.CreateProperties(type, memberSerialization);

            if (_encryptionKey != null)
            {
                // Find all string properties that have a [JsonEncrypt] attribute applied
                // and attach an EncryptedStringValueProvider instance to them
                foreach (JsonProperty prop in props.Where(p => p.PropertyType == typeof(string)))
                {
                    PropertyInfo pi = type.GetProperty(prop.UnderlyingName);
                    if (pi != null && pi.GetCustomAttribute(typeof(JsonEncryptAttribute), true) != null)
                    {
                        prop.ValueProvider =
                            new EncryptedStringValueProvider(pi, _encryptionKey);
                    }

                }
            }

            return props;
        }

        private class EncryptedStringValueProvider : IValueProvider
        {
            private readonly PropertyInfo _targetProperty;
            private readonly string _encryptionKey;

            public EncryptedStringValueProvider(PropertyInfo targetProperty, string encryptionKey)
            {
                _targetProperty = targetProperty;
                _encryptionKey = encryptionKey;
            }

            // GetValue is called by Json.Net during serialization.
            // The target parameter has the object from which to read the unencrypted string;
            // the return value is an encrypted string that gets written to the JSON
            public object GetValue(object target)
            {
                string value = (string)_targetProperty.GetValue(target);
                if(String.IsNullOrEmpty((string)value))
                {
                    return null;
                }

                var returnValue = EncryptString.Encrypt(value, _encryptionKey, 1000);

                if(!returnValue.Success)
                {
                    throw new Exception("Encryption failed on property " + _targetProperty.Name);
                }

                return returnValue.Value;
            }

            // SetValue gets called by Json.Net during deserialization.
            // The value parameter has the encrypted value read from the JSON;
            // target is the object on which to set the decrypted value.
            public void SetValue(object target, object value)
            {
                if(String.IsNullOrEmpty((string)value))
                {
                    _targetProperty.SetValue(target, null);
                }

                var returnValue = EncryptString.Decrypt((string)value, _encryptionKey, 1000);

                if(!returnValue.Success)
                {
                    throw new Exception("Decryption failed on property " + _targetProperty.Name);
                }
                _targetProperty.SetValue(target, returnValue.Value);
            }

        }
    }
}