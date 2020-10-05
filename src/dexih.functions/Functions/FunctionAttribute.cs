using System;
using Dexih.Utils.DataType;



namespace dexih.functions
{

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EGenericType
    {
        None = 1, Numeric, All, String
    }

    /// <summary>
    /// Attribute used to indicate that the function is for using in the integration hub.
    /// </summary>
    [Serializable]
    public class TransformFunctionAttribute : Attribute
    {
        public string Category { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public EFunctionType FunctionType { get; set; }

        /// <summary>
        /// Method called when requesting an aggregate value on a group.
        /// </summary>
        public string ResultMethod { get; set; }

        /// <summary>
        /// Method called to initialize the method
        /// </summary>
        public string InitializeMethod { get; set; }

        /// <summary>
        /// Method called to reset variables in the class
        /// </summary>
        public string ResetMethod { get; set; }

        /// <summary>
        /// Method called to return potential columns based on a sample dataset.
        /// </summary>
        public string ImportMethod { get; set; }

        public ETypeCode GenericTypeDefault { get; set; } = ETypeCode.String;

        public EGenericType GenericType { get; set; } = EGenericType.None;

        /// <summary>
        /// If true, the result function must return a boolean value which indicates function to be called again for more rows.
        /// </summary>
        public bool GeneratesRows = false;
    }

    public class TransformFunctionParameterAttribute : Attribute
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string[] ListOfValues { get; set; }
        
        /// <summary>
        ///  Override the ETypeCode for the parameter.
        /// Mostly used for datetime = time/date/datetime.
        /// </summary>
        public ETypeCode Type { get; set; } = ETypeCode.Unknown;

        /// <summary>
        /// Defines default formatting for the parameter (eg. ###0,00)
        /// </summary>
        public string DefaultFormat { get; set; }

    }

    /// <summary>
    /// Indicates field is password, and can be passed encrypted parameters.
    /// </summary>
    public class TransformFunctionPassword : Attribute
    {
        
    }

    
    /// <summary>
    /// Identifies an array parameters which are logically linked.
    /// This means the matching arrays should have the same length.
    /// </summary>
    public class TransformFunctionLinkedParameterAttribute : Attribute
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public TransformFunctionLinkedParameterAttribute(string name, string description = null)
        {
            Name = name;
            Description = description;
        }
    }

    /// <summary>
    /// Identifies the parameter as a label (rather than a value) which should have text entry.
    /// </summary>
    public class TransformParameterLabelAttribute : Attribute
    {
    }

    /// <summary>
    /// Ignore the parameter.
    /// </summary>
    public class TransformParameterIgnoreAttribute : Attribute
    {
        
    }

    public class ParameterDefaultAttribute: Attribute
    {
        public ParameterDefaultAttribute(string value)
        {
            Value = value;
        }
        
        public string Value { get; set; }
    }

    public class TransformFunctionCompareAttribute : Attribute
    {
        /// <summary>
        /// Indicates if the function is equivalent to a filter compare type.
        /// </summary>
        public ECompare Compare { get; set; } = ECompare.IsEqual;
    }

    public class TransformFunctionVariableAttribute : Attribute
    {
        public EFunctionVariable FunctionParameter { get; set; }

        public TransformFunctionVariableAttribute(EFunctionVariable functionParameter)
        {
            FunctionParameter = functionParameter;
        }
    }
    
    public class GlobalSettingsAttribute : Attribute
    {
    }

    public class ParameterNamesAttribute : Attribute
    {
    }
}