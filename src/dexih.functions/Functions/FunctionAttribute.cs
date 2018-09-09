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
        
        /// <summary>
        /// Method called when requesting an aggregate value on a group.
        /// </summary>
        public string ResultMethod { get; set; }
        
        /// <summary>
        /// Method called to reset variables in the class
        /// </summary>
        public string ResetMethod { get; set; }

        /// <summary>
        /// Method called to return potential columns based on a sample dataset.
        /// </summary>
        public string ImportMethod { get; set; }

        /// <summary>
        /// If true, the result function must return a boolean value which indicates function to be called again for more rows.
        /// </summary>
        public bool GeneratesRows = false;
    }

    public class TransformFunctionParameterAttribute : Attribute
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