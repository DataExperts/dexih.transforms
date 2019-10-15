using System;
using System.Data.Common;
using System.Reflection;
using dexih.functions;
using Dexih.Utils.DataType;

namespace dexih.transforms.Poco
{
    /// <summary>
    /// Maps a DbDataReader row to an object.
    /// </summary>
    /// <typeparam name="T">Object Type to Map</typeparam>
    public class PocoMapper<T>
    {
        private readonly DbDataReader _reader;
        // private readonly List<PocoTableMapping> _fieldMappings;
        private readonly PocoTable<T> _pocoTable;

        public PocoMapper(DbDataReader reader)
        {
            _reader = reader;
            _pocoTable = new PocoTable<T>();



            //_fieldMappings = new List<PocoTableMapping>();
            ////Create a list of properties in the type T, and match them to ordinals in the inputReader.
            //var properties = typeof(T).GetProperties();
            //foreach (var propertyInfo in properties)
            //{
            //    // if the property has a field attribute, use this as the reference in the reader
            //    var field = propertyInfo.GetCustomAttribute<PocoColumnAttribute>(false);
            //    if (field != null && !field.Skip)
            //    {
            //        var name = string.IsNullOrEmpty(field?.Name) ? propertyInfo.Name : field.Name;
            //        var isKey = field != null && field.IsKey;

            //        var ordinal = reader.GetOrdinal(name);
            //        if (ordinal >= 0)
            //        {
            //            _fieldMappings.Add(new PocoTableMapping(propertyInfo, ordinal, isKey));
            //        }
            //    }
            //}
        }

        public PocoMapper(DbDataReader reader, PocoTable<T> pocoTable)
        {
            _reader = reader;
            _pocoTable = pocoTable;
        }

        /// <summary>
        /// Get the object using the current item in the DbDataReader
        /// </summary>
        /// <returns>Object</returns>
        public T GetItem()
        {
            var item = (T)Activator.CreateInstance(typeof(T));

            foreach (var mapping in _pocoTable.TableMappings)
            {
                if (!mapping.PropertyInfo.CanWrite) continue;

                var column = _pocoTable.Table.Columns[mapping.Position];
                var ordinal = _reader.GetOrdinal(column.Name);
                if (ordinal >= 0)
                {
                    var value = _reader[ordinal];

                    try
                    {
                        var typeInfo = mapping.PropertyInfo.PropertyType.GetTypeInfo();
                        if(typeInfo.IsEnum && value is string s)
                        {
                            value = Enum.Parse(mapping.PropertyInfo.PropertyType, s);
                        }
                        else if (!DataType.IsSimple(mapping.PropertyInfo.PropertyType) && value is string s1 && column.DataType != ETypeCode.String)
                        {
                            value = s1.Deserialize(mapping.PropertyInfo.PropertyType);
                        }
                        else
                        {
                            value = Operations.Parse(column.DataType, value);
                        }

                        mapping.PropertyInfo.SetValue(item, value is DBNull ? null : value);
                    }
                    catch (Exception ex)
                    {
#if DEBUG
                        throw new PocoException($"Can't set value {value} to property {mapping.PropertyInfo.Name}.  {ex.Message}.", ex);
#else
                    throw new PocoException($"Can't set property {mapping.PropertyInfo.Name}.  {ex.Message}.", ex);
#endif
                    }
                }
            }

            return item;
        }
    }

    public class PocoReaderMapping
    {
        public PropertyInfo PropertyInfo { get; set; }
        public int ReaderOrdinal { get; set; }
        public TableColumn Column { get; set; }

        public PocoReaderMapping(PropertyInfo propertyInfo, int readerOrdinal, TableColumn column)
        {
            PropertyInfo = propertyInfo;
            ReaderOrdinal = readerOrdinal;
            Column = column;
        }
    }
}