using System;
using System.Reflection;

namespace dexih.functions
{
    public enum EFunctionType
    {
        Map, Condition, Aggregate, Rows, Validate
    }
    
    public class TransformFunctionAttribute : Attribute
    {
        public string Category { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public EFunctionType FunctionType { get; set; }
        
        public string ResultFunction { get; set; }
        public string ResetFunction { get; set; }
    }

    public class FunctionParameterAtttribute : Attribute
    {
        
    }
}