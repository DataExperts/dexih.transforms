using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Dexih.Utils.DataType;

namespace dexih.functions
{
    public static class Extensions
    {
        
        /// <summary>
        /// Use to get a value or a default from a dictionary.
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <typeparam name="TK"></typeparam>
        /// <typeparam name="TV"></typeparam>
        /// <returns></returns>
        public static TV GetValueOrDefault<TK, TV>(this IDictionary<TK, TV> dict, TK key, TV defaultValue = default(TV))
        {
            return dict.TryGetValue(key, out var value) ? value : defaultValue;
        }

        /// <summary>
        /// Checks if an object is null or a blank string
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool ObjectIsNullOrBlank(this object value)
        {
            if (value is null)
            {
                return true;
            }

            if (value is string s && string.IsNullOrWhiteSpace(s))
            {
                return true;
            }

            return false;
        }
        
        public static bool IsPattern(this string value, string pattern)
        {
            if (value.Length != pattern.Length) return false;
            for (var i = 0; i < pattern.Length; i++)
            {
                if (pattern[i] == '9' && !char.IsNumber(value[i]) ||
                    pattern[i] == 'A' && !char.IsUpper(value[i]) ||
                    pattern[i] == 'a' && !char.IsLower(value[i]) ||
                    pattern[i] == 'Z' && !char.IsLetter(value[i]))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Converts value to a string, when null insertes \<null\>
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string DisplayValue(this object value)
        {
            return value switch
            {
                null => "<null>",
                string valueString when valueString == "" => "<blank>",
                string valueString => valueString,
                _ => value.ToString()
            };
        }
        
        public static string CreateSHA1(this string value)
        {
            if (value == null)
            {
                return null;
            }
            
            var bytes = Encoding.UTF8.GetBytes(value);
            using (var sha1 = System.Security.Cryptography.SHA1.Create())
            {
                var hash = sha1.ComputeHash(bytes);
                // Loop through each byte of the hashed data 
                // and format each one as a hexadecimal string.
                var sBuilder = new StringBuilder();

                for (var i = 0; i < hash.Length; i++)
                {
                    sBuilder.Append(hash[i].ToString("x2"));
                }

                // Return the hexadecimal string.
                return sBuilder.ToString();
            }
        }
        
        public static DateTime UnixTimeStampToDate(this long unixTimeStamp)
        {
            var origDate = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            var convertedDate = origDate.AddSeconds(unixTimeStamp).ToLocalTime();
            return convertedDate;
        }

        public static T ToObject<T>(this object value)
        {
            if (value is T tValue)
            {
                return tValue;
            }

            if (value is JsonDocument jsonDocument)
            {
                return jsonDocument.ToObject<T>();
            }

            if (value is JsonElement jsonElement)
            {
                return jsonElement.ToObject<T>();
            }

            if (value is Array valueArray)
            {
                var type1 = typeof(T);
                if (type1.IsArray)
                {
                    var elementType = type1.GetElementType();
                    var array = Array.CreateInstance(elementType, valueArray.Length);
                    for(var i = 0; i < valueArray.Length; i++)
                    {
                        var v = valueArray.GetValue(i);

                        if (v.GetType() == elementType)
                        {
                            array.SetValue(v, i);
                        }
                        else if (v is JsonElement jsonElementItem)
                        {
                            array.SetValue(jsonElementItem.ToObject(elementType), i);
                        }
                        else
                        {
                            array.SetValue(Operations.Parse(elementType, value), i);
                        }
                    }
                    return (T)(object) array;
                }

                if (typeof(ICollection).IsAssignableFrom(type1))
                {
                    var collection = (ICollection) Activator.CreateInstance(type1);
                    var elementType = type1.GetProperty("Item").PropertyType;
                    var add = type1.GetMethod("Add", new Type[] {elementType});
                    foreach (var v in valueArray)
                    {
                        if (v.GetType() == elementType)
                        {
                            add.Invoke(collection, new object[] {v});
                        }
                        else if (v is JsonElement jsonElementItem)
                        {
                            add.Invoke(collection, new object[] {jsonElementItem.ToObject(elementType)});
                        }
                        else
                        {
                            add.Invoke(collection, new object[] {Operations.Parse(elementType, value)});
                        }
                    }
                    
                    return (T) collection;
                }
            }

            if (value is null)
            {
                return default;
            }

            var type = Nullable.GetUnderlyingType(typeof(T));
            if (type == null)
            {
                return Operations.Parse<T>(value);
            } else
            {
                return (T) Operations.Parse(type, value);
            }
        }
    }
}