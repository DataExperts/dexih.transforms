using System;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{

    [JsonConverter(typeof(StringEnumConverter))]
    public enum EGenericType
    {
        None, Numeric, All, String
    }

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

        public DataType.ETypeCode GenericTypeDefault { get; set; } = DataType.ETypeCode.String;

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
        /// Identifies an array parameter is a twin with an output array parameter.
        /// This means the matching arrays should have the same length.
        /// </summary>
        // public string TwinParameterName { get; set; }
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
    public class ParameterLabelAttribute : Attribute
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
        public Filter.ECompare Compare { get; set; }
    }

    public class TransformFunctionVariableAttribute : Attribute
    {
        public EFunctionVariable FunctionParameter { get; set; }

        public TransformFunctionVariableAttribute(EFunctionVariable functionParameter)
        {
            FunctionParameter = functionParameter;
        }
    }
    
    public class TransformGlobalVariablesAttribute : Attribute
    {
    }
}