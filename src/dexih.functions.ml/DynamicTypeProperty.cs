using System;
using Dexih.Utils.DataType;

namespace dexih.functions.ml
{
    /// <summary>
    /// A property name, and type used to generate a property in the dynamic class.
    /// </summary>
    public class DynamicTypeProperty
    {
        public DynamicTypeProperty(string name, Type type, EEncoding? encoding = null)
        {
            Name = name;
            Type = type;
            Encoding = encoding;

            TypeCode = DataType.GetTypeCode(type, out _);
        }
        public string Name { get; }
        public Type Type { get; }

        public EEncoding? Encoding { get; }

        public DataType.ETypeCode TypeCode { get; }

        public object Convert(object value)
        {
            return Operations.Parse(TypeCode, value);
        }
    }
}