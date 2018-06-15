using System;
using System.Reflection;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EFunctionType
    {
        Map, Condition, Aggregate, Rows, Validate, Profile
    }
    
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
        
        /// <summary>
        /// Used for profiling functions, indicates the property use to switch detailed results on/off.
        /// </summary>
        public string DetailedFlagName { get; set; }
        
        /// <summary>
        /// Used to map a filter equivalent operator
        /// </summary>
        public Filter.ECompare? Compare { get; set; }
        
        public bool IsStandardFunction { get; set; }

        public DataType.ETypeCode ReturnType { get; set; }
        public DataType.ETypeCode? ResultReturnType { get; set; }
        
        public FunctionParameter[] InputParameters { get; set; }
        public FunctionParameter[] OutputParameters { get; set; }
        public FunctionParameter[] ResultInputParameters { get; set; }
        public FunctionParameter[] ResultOutputParameters { get; set; }

        public TransformFunction GetTransformFunction(TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
        {
            var type = GetTransformType();
            return new TransformFunction(type, FunctionMethodName, inputMappings, targetColumn, outputMappings, null);
        }

        public TransformFunction GetTransformFunction(TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings, bool detailed)
        {
            var type = GetTransformType();
            var obj = Activator.CreateInstance(type);

            if (!string.IsNullOrEmpty(DetailedFlagName))
            {
                var property = type.GetProperty(DetailedFlagName);
                if (property == null)
                {
                    throw new FunctionException($"The detailed flag property {DetailedFlagName} could not be found in in the type {FunctionClassName}.");
                }
                property.SetValue(obj, detailed);
            }
            
            return new TransformFunction(obj, FunctionMethodName, inputMappings, targetColumn, outputMappings);
        }

        public TransformFunction GetTransformFunction()
        {
            var type = GetTransformType();
            return new TransformFunction(type, FunctionMethodName, null, null, null, null);
        }

        public Type GetTransformType()
        {
            Type type;
            if (string.IsNullOrEmpty(FunctionAssemblyName))
            {
                type = Assembly.GetExecutingAssembly().GetType(FunctionClassName);
            }
            else
            {
                var assembly = Assembly.Load(FunctionAssemblyName);

                if (assembly == null)
                {
                    throw new FunctionNotFoundException($"The assembly {FunctionAssemblyName} was not found.");
                }
                type = assembly.GetType(FunctionClassName);
            }

            return type;
        }
    }
    

}