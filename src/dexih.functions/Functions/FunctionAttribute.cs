using System;
using dexih.functions.Query;

namespace dexih.functions
{

    
    public class TransformFunctionAttribute : Attribute
    {
        public string Category { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public EFunctionType FunctionType { get; set; }
        
        public string ResultMethod { get; set; }
        public string ResetMethod { get; set; }
        public string ImportMethod { get; set; }
    }

    public class TransformFunctionParameter : Attribute
    {
        public string Name { get; set; }
        public string Description { get; set; }

        /// <summary>
        /// Identifies an array parameter is a twin with an output array parameter.
        /// This means the matching arrays should have the same length.
        /// </summary>
        public string TwinParameterName { get; set; }
    }

    public class TransformFunctionCompareAttribute : Attribute
    {
        /// <summary>
        /// Indicates if the function is equivalent to a filter compare type.
        /// </summary>
        public Filter.ECompare Compare { get; set; }
    }

    public class TransformFunctionIndex : Attribute
    {
        
    }

    public class TransformFunctionDetailedFlagAttribute : Attribute
    {
        
    }

    public class EncryptionKeyFlagAttribute : Attribute
    {
        
    }
}