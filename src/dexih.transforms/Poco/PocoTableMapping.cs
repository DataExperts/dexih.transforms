using System.Reflection;

namespace dexih.transforms.Poco
{
    public class PocoTableMapping
    {
        public PropertyInfo PropertyInfo { get; set; }
        public int Position { get; set; }
        public bool IsKey { get; set; }

        public PocoTableMapping(PropertyInfo propertyInfo, int position, bool isKey)
        {
            PropertyInfo = propertyInfo;
            Position = position;
            IsKey = isKey;
        }
    }
}
