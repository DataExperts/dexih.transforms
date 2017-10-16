using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;

namespace dexih.transforms.Poco
{
    /// <summary>
    /// Maps a DbDataReader row to an object.
    /// </summary>
    /// <typeparam name="T">Object Type to Map</typeparam>
    public class PocoMapper<T>
    {
        private readonly DbDataReader _reader;
        private readonly List<PocoTableMapping> _fieldMappings;
        
        public PocoMapper(DbDataReader reader)
        {
            _reader = reader;
            
            _fieldMappings = new List<PocoTableMapping>();
            //Create a list of properties in the type T, and match them to ordinals in the inputReader.
            var properties = typeof(T).GetProperties();
            foreach (var propertyInfo in properties)
            {
                // if the property has a field attribute, use this as the reference in the reader
                var field = propertyInfo.GetCustomAttribute<FieldAttribute>(false);
                var name = string.IsNullOrEmpty(field?.Name) ? propertyInfo.Name : field.Name;

                var ordinal = reader.GetOrdinal(name);
                if (ordinal >= 0)
                {
                    _fieldMappings.Add(new PocoTableMapping(propertyInfo, ordinal));
                }
            }
        }

        /// <summary>
        /// Get the object using the current item in the DbDataReader
        /// </summary>
        /// <returns>Object</returns>
        public T GetItem()
        {
            var item = (T) Activator.CreateInstance(typeof(T));

            foreach (var mapping in _fieldMappings)
            {
                var value = _reader[mapping.Position];
                
                if (mapping.PropertyInfo.PropertyType.GetTypeInfo().IsEnum && value is string)
                {
                    value = Enum.Parse(mapping.PropertyInfo.PropertyType, (string)value);
                }
                mapping.PropertyInfo.SetValue(item, value is DBNull ? null : value);
            }

            return item;
        }
    }
}