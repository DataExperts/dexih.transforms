using System.Collections.Generic;

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
    }
}