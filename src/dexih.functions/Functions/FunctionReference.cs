using System;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EFunctionType
    {
        Map, Condition, Aggregate, Series, Rows, Validate, Profile
    }
    
    /// <summary>
    /// Function reference contains details of a standard function
    /// </summary>
    [Serializable]
    public class FunctionReference
    {
        public EFunctionType FunctionType { get; set; }
        public string Category { get; set; }
        public string Name { get; set; }
        public string Description { get; set;}
        
        public string FunctionAssemblyName { get; set; }
        public string FunctionClassName { get; set; }
        public string FunctionMethodName { get; set; }
        public string ResultMethodName { get; set; }
        public string ResetMethodName { get; set; }
        public string ImportMethodName { get; set; }

        /// <summary>
        /// Indicates the function contains a generic tyep definition
        /// </summary>
        public EGenericType GenericType { get; set; }
        public DataType.ETypeCode GenericTypeDefault { get; set; }

        /// <summary>
        /// Used to map a filter equivalent operator
        /// </summary>
        public Filter.ECompare? Compare { get; set; }
        
        public bool IsStandardFunction { get; set; }

        public FunctionParameter[] ReturnParameters { get; set; }
        public FunctionParameter[] InputParameters { get; set; }
        public FunctionParameter[] OutputParameters { get; set; }

        public FunctionParameter[] ResultReturnParameters { get; set; }
        public FunctionParameter[] ResultInputParameters { get; set; }
        public FunctionParameter[] ResultOutputParameters { get; set; }

        public TransformFunction GetTransformFunction(Type genericType, Parameters parameters = null, GlobalVariables globalVariables = null)
        {
            var type = Functions.GetFunctionType(FunctionClassName, FunctionAssemblyName); 
            return new TransformFunction(type, FunctionMethodName, genericType, parameters, globalVariables);
        }

//        public TransformFunction GetTransformFunction(Parameters parameters, GlobalVariables globalVariables)
//        {
//            var type = GetTransformType();
//            return new TransformFunction(type, FunctionMethodName, parameters, globalVariables);
//        }

//        public TransformFunction GetTransformFunction(Parameters parameters, GlobalVariables globalVariables, bool detailed)
//        {
//            var type = Functions.GetFunctionType(FunctionClassName, FunctionAssemblyName); 
//            var obj = Activator.CreateInstance(type);
//
//            if (!string.IsNullOrEmpty(DetailedFlagName))
//            {
//                var property = type.GetProperty(DetailedFlagName);
//                if (property == null)
//                {
//                    throw new FunctionException($"The detailed flag property {DetailedFlagName} could not be found in in the type {FunctionClassName}.");
//                }
//                property.SetValue(obj, detailed);
//            }
//            
//            return new TransformFunction(obj, FunctionMethodName, parameters, globalVariables);
//        }

//        public TransformFunction GetTransformFunction()
//        {
//            var type = GetTransformType();
//            return new TransformFunction(type, FunctionMethodName, null, null, null, globalVariables);
//        }

//        public Type GetTransformType()
//        {
//            Type type;
//            if (string.IsNullOrEmpty(FunctionAssemblyName))
//            {
//                type = Assembly.GetExecutingAssembly().GetType(FunctionClassName);
//            }
//            else
//            {
//                var assembly = Assembly.Load(FunctionAssemblyName);
//
//                if (assembly == null)
//                {
//                    throw new FunctionNotFoundException($"The assembly {FunctionAssemblyName} was not found.");
//                }
//                type = assembly.GetType(FunctionClassName);
//            }
//
//            return type;
//        }


    }
}