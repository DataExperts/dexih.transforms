using System;
using System.Collections.Generic;
using dexih.functions;
using static dexih.functions.TableColumn;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.Poco
{
    /// <summary>
    /// Field attributes are used to decorate class properties with metadata required for the PocoReader and PocoTable classes. 
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class PocoColumnAttribute : Attribute
    {
        public PocoColumnAttribute()
        {
        }

        public PocoColumnAttribute(string name)
        {
            Name = name;
        }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the deltaType value
        /// </summary>
        /// <value>The type of the delta.</value>
        public TableColumn.EDeltaType DeltaType { get; set; } = EDeltaType.TrackingField;

        /// <summary>
        /// Gets or sets an override for the datatype.
        /// </summary>
        /// <value>The length of the max.</value>
        public ETypeCode DataType { get; set; } = ETypeCode.Unknown;

        /// <summary>
        /// Gets or sets the length of the string.
        /// </summary>
        /// <value>The length of the max.</value>
        public bool AllowDbNull { get; set; } = false;

        /// <summary>
        /// Gets or sets the length of the string.
        /// </summary>
        /// <value>The length of the max.</value>
        public int MaxLength { get; set; } = -1;

        /// <summary>
        /// Gets or sets the numeric scale.
        /// </summary>
        /// <value>The scale.</value>
        public int Scale { get; set; } = -1;

        /// <summary>
        /// Gets or sets the numeric precision.
        /// </summary>
        /// <value>The precision.</value>
        public int Precision { get; set; } = -1;

        /// <summary>
        /// Gets or sets a value indicating whether this <see cref="T:dexih.transforms.Poco.FieldAttribute"/> is key when running update/delete queries.
        /// </summary>
        /// <value><c>true</c> if is key; otherwise, <c>false</c>.</value>
        public bool IsKey { get; set; } = false;

        public bool Skip { get; set; } = false;

    }

    /// <summary>
    /// Table attributes are used to decorate classes with metadata required for the PocoReader and PocoTable classes. 
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class PocoTableAttribute : Attribute
    {
        public PocoTableAttribute()
        {
        }

        public PocoTableAttribute(string name)
        {
            Name = name;
        }
        
        public string Name { get; set; }
        public List<Sort> SortFields { get; set; }
    }
}