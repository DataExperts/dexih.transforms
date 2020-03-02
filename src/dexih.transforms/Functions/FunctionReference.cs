using System;
using System.Runtime.Serialization;
using dexih.functions.Parameter;
using Dexih.Utils.DataType;

namespace dexih.functions
{
    /// <summary>
    /// Function reference contains details of a standard function
    /// </summary>
    [DataContract]
    public class FunctionReference
    {
        [DataMember(Order = 0)]
        public EFunctionType FunctionType { get; set; }

        [DataMember(Order = 1)]
        public string Category { get; set; }

        [DataMember(Order = 2)]
        public string Name { get; set; }

        [DataMember(Order = 3)]
        public string Description { get; set;}

        [DataMember(Order = 4)]
        public string FunctionAssemblyName { get; set; }

        [DataMember(Order = 5)]
        public string FunctionClassName { get; set; }

        [DataMember(Order = 6)]
        public string FunctionMethodName { get; set; }

        [DataMember(Order = 7)]
        public string ResultMethodName { get; set; }

        [DataMember(Order = 8)]
        public string ResetMethodName { get; set; }

        [DataMember(Order = 9)]
        public string ImportMethodName { get; set; }

        /// <summary>
        /// Indicates the function contains a generic tyep definition
        /// </summary>
        [DataMember(Order = 10)]
        public EGenericType GenericType { get; set; }

        [DataMember(Order = 11)]
        public ETypeCode GenericTypeDefault { get; set; }

        /// <summary>
        /// Used to map a filter equivalent operator
        /// </summary>
        [DataMember(Order = 12)]
        public ECompare? Compare { get; set; } = ECompare.IsEqual;

        [DataMember(Order = 13)]
        public bool IsStandardFunction { get; set; }

        [DataMember(Order = 14)]
        public FunctionParameter[] ReturnParameters { get; set; }

        [DataMember(Order = 15)]
        public FunctionParameter[] InputParameters { get; set; }

        [DataMember(Order = 16)]
        public FunctionParameter[] OutputParameters { get; set; }

        [DataMember(Order = 17)]
        public FunctionParameter[] ResultReturnParameters { get; set; }

        [DataMember(Order = 18)]
        public FunctionParameter[] ResultInputParameters { get; set; }

        [DataMember(Order = 19)]
        public FunctionParameter[] ResultOutputParameters { get; set; }

        public TransformFunction GetTransformFunction(Type genericType, Parameters parameters = null, GlobalSettings globalSettings = null)
        {
            var type = Functions.GetFunctionType(FunctionClassName, FunctionAssemblyName);
            var transformFunction =
                new TransformFunction(type, FunctionMethodName, genericType, parameters, globalSettings)
                {
                    Compare = Compare
                };
            return transformFunction;
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