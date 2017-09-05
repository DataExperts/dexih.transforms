using System;
using System.Collections.Generic;
using dexih.functions;
using static dexih.functions.TableColumn;

namespace dexih.transforms
{
    [AttributeUsage(AttributeTargets.Property)]
    public class FieldAttribute : Attribute
    {
        public FieldAttribute()
        {
        }

        public FieldAttribute(string name)
        {
            Name = name;
        }
        
        public string Name { get; set; }
        public TableColumn.EDeltaType DeltaType { get; set; }
        public int MaxLength;
        public int Scale;
        public int Precision;
    }
    
    [AttributeUsage(AttributeTargets.Property)]
    public class TableAttribute : Attribute
    {
        public TableAttribute()
        {
        }

        public TableAttribute(string name)
        {
            Name = name;
        }
        
        public string Name { get; set; }
        public List<Sort> SortFields { get; set; }
    }
}