using System.Collections.Generic;

namespace dexih.functions
{
    public static class DictionaryExtensions
    {
        public static U GetOrDefault<T, U>(this Dictionary<T,U> dictionary, T key)
        {
            if (dictionary.TryGetValue(key, out var value))
            {
                return value;
            }

            return default;
        }
    }
}