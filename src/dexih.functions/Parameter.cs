using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions
{

    /// <summary>
    /// The parameter class is used by the "Function" to provide input and output parameter capabilities.
    /// </summary>
    public class Parameter
    {
        public Parameter()
        {
        }

        /// <summary>
        /// Initializes are parameter
        /// </summary>
        /// <param name="name">Paramter name</param>
        /// <param name="parameterType">Parameter datatype</param>
        /// <param name="isColumn">Indicates if the parameter is a column (vs a hard coded value)</param>
        /// <param name="value">Value for the parameter (note: requires isColumn = false)</param>
        /// <param name="columnName">Column Name for the parameter to map to (note: requires isColumn = true)</param>
        /// <param name="isArray">Inidicates the parameter is an array of values (note array parameter</param>
        public Parameter(
            string name, 
            ETypeCode parameterType, 
            bool isColumn = false, 
            object value = null, 
            TableColumn column = null, 
            bool isArray = false)
        {
            Name = name;
            DataType = parameterType;
            IsArray = isArray;
            if (value != null)
            {
                SetValue(value);
            }
            Column = column;
            IsColumn = isColumn;
        }

        /// <summary>
        /// Initializes a parameter.
        /// </summary>
        /// <param name="parameterType">Parameter datatype</param>
        /// <param name="columnName">Column Name for the parameter to map to.</param>
        public Parameter( ETypeCode parameterType, TableColumn column)
        {
            Name = column.Name;
            DataType = parameterType;
            IsColumn = true;
            Column = column;
            IsColumn = true;
        }

        public Parameter(ETypeCode parameterType, string columnName)
        {
            Name = columnName;
            DataType = parameterType;
            IsColumn = true;
            Column = new TableColumn(columnName);
            IsColumn = true;
        }

        /// <summary>
        /// Name for the parameter.  This name must be used when referencing parameters in custom functions.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Parameter datatype
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public ETypeCode DataType { get; set; }

        /// <summary>
        /// Indicates if the parameter is a column (vs a hard coded value)
        /// </summary>
        public bool IsColumn { get; set; }
		public bool IsExpression { get; set; }
        public bool IsArray { get; set; }
        public TableColumn Column { get; set; } 

        private object _value;

        /// <summary>
        /// The returned value.
        /// </summary>
        public object Value
        {
            get
            {
                return _value;
            }
        }


        /// <summary>
        /// Sets and converts the value to the appropriate type.
        /// </summary>
        /// <param name="input"></param>
        public void SetValue(object input)
        {
            if (IsArray)
            {
                _value = input;
                return;
            }
			if (input == null) // || string.IsNullOrEmpty(input.ToString()))
			{
				_value = null;
			}
			else
			{
				var result = TryParse(DataType, input);
				_value = result;
			}
        }

    }

}
