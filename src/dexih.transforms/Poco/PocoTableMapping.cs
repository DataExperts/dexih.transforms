using System;
using System.Reflection;

namespace dexih.transforms.Poco
{
    public class PocoTableMapping
    {
        public PropertyInfo PropertyInfo { get; set; }
        public int Position { get; set; }

        public PocoTableMapping(PropertyInfo propertyInfo, int position)
        {
            PropertyInfo = propertyInfo;
            Position = position;
        }
    }
}
