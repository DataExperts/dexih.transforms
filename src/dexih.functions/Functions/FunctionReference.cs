using System;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.functions
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EFunctionType
    {
        Map = 1, Condition, Aggregate, Series, Rows, Validate, Profile, Sort, JoinCondition
    }
    
    /// <summary>
    /// Function reference contains details of a standard function
    /// </summary>
    [ProtoContract]
    public class FunctionReference
    {
        [ProtoMember(1)]
        public EFunctionType FunctionType { get; set; }

        [ProtoMember(2)]
        public string Category { get; set; }

        [ProtoMember(3)]
        public string Name { get; set; }

        [ProtoMember(4)]
        public string Description { get; set;}

        [ProtoMember(5)]
        public string FunctionAssemblyName { get; set; }

        [ProtoMember(6)]
        public string FunctionClassName { get; set; }

        [ProtoMember(7)]
        public string FunctionMethodName { get; set; }

        [ProtoMember(8)]
        public string ResultMethodName { get; set; }

        [ProtoMember(9)]
        public string ResetMethodName { get; set; }

        [ProtoMember(10)]
        public string ImportMethodName { get; set; }

        /// <summary>
        /// Indicates the function contains a generic tyep definition
        /// </summary>
        [ProtoMember(11)]
        public EGenericType GenericType { get; set; }

        [ProtoMember(12)]
        public DataType.ETypeCode GenericTypeDefault { get; set; }

        /// <summary>
        /// Used to map a filter equivalent operator
        /// </summary>
        [ProtoMember(13)]
        public ECompare? Compare { get; set; }

        [ProtoMember(14)]
        public bool IsStandardFunction { get; set; }

        [ProtoMember(15)]
        public FunctionParameter[] ReturnParameters { get; set; }

        [ProtoMember(16)]
        public FunctionParameter[] InputParameters { get; set; }

        [ProtoMember(17)]
        public FunctionParameter[] OutputParameters { get; set; }

        [ProtoMember(18)]
        public FunctionParameter[] ResultReturnParameters { get; set; }

        [ProtoMember(19)]
        public FunctionParameter[] ResultInputParameters { get; set; }

        [ProtoMember(20)]
        public FunctionParameter[] ResultOutputParameters { get; set; }

        public TransformFunction GetTransformFunction(Type genericType, Parameters parameters = null, GlobalSettings globalSettings = null)
        {
            var type = Functions.GetFunctionType(FunctionClassName, FunctionAssemblyName); 
            return new TransformFunction(type, FunctionMethodName, genericType, parameters, globalSettings);
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