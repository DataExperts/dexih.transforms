using System;

namespace dexih.functions
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false, Inherited = false)]
    public class ProtoInheritAttribute: Attribute
    {
        public ProtoInheritAttribute(int tag)
        {
            Tag = tag;
        }
        
        public int Tag { get; set; }
    }
}