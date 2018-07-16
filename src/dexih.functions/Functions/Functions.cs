using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Dexih.Utils.DataType;

namespace dexih.functions
{
    public class Functions
    {
        public static (Type type, MethodInfo method) GetFunctionMethod(string className, string methodName, string assemblyName = null)
        {
            Type type;

            if (string.IsNullOrEmpty(assemblyName))
            {
                type = Assembly.GetExecutingAssembly().GetType(className);
            }
            else
            {
                var assembly = Assembly.Load(assemblyName);

                if (assembly == null)
                {
                    throw new FunctionNotFoundException($"The assembly {assemblyName} was not found.");
                }
                type = assembly.GetType(className);
            }

            if (type == null)
            {
                throw new FunctionNotFoundException($"The type {className} was not found.");
            }

            var method = type.GetMethod(methodName);

            if (method == null)
            {
                throw new FunctionNotFoundException($"The method {methodName} was not found in the type {className}.");
            }

            return (type, method);
        }
        
        public static FunctionReference GetFunction(string className, string methodName, string assemblyName = null)
        {
            var functionMethod = GetFunctionMethod(className, methodName, assemblyName);

            if (functionMethod.method == null)
            {
                throw new FunctionNotFoundException($"The method {methodName} was not found in the type {className}.");
            }

            var function = GetFunction(functionMethod.type, functionMethod.method);
            function.FunctionAssemblyName = assemblyName;

            return function;
        }

        public static FunctionReference GetFunction(Type type, MethodInfo method)
        {
            var attribute = method.GetCustomAttribute<TransformFunctionAttribute>();
            if (attribute != null)
            {
                MethodInfo resultMethod = null;
                if (attribute.ResultMethod != null)
                {
                    resultMethod = type.GetMethod(attribute.ResultMethod);
                }

                string detailedPropertyName = null;

                foreach (var prop in type.GetProperties())
                {
                    if (prop.GetCustomAttribute(typeof(TransformFunctionDetailedFlagAttribute)) != null)
                    {
                        detailedPropertyName = prop.Name;
                    }
                }
                
                var compareAttribute = method.GetCustomAttribute<TransformFunctionCompareAttribute>();
                
                var function = new FunctionReference()
                {
                    Name = attribute.Name,
                    Description = attribute.Description,
                    FunctionType = attribute.FunctionType,
                    Category = attribute.Category,
                    FunctionMethodName = method.Name,
                    ResetMethodName = attribute.ResetMethod,
                    ResultMethodName = attribute.ResultMethod,
                    ImportMethodName = attribute.ImportMethod,
                    IsStandardFunction = true,
                    DetailedFlagName = detailedPropertyName,
                    Compare = compareAttribute?.Compare,
                    ReturnType = DataType.GetTypeCode(method.ReturnType),
                    ResultReturnType = resultMethod == null ? (DataType.ETypeCode?) null : DataType.GetTypeCode(resultMethod.ReturnType),
                    FunctionClassName = type.FullName,
                    InputParameters = method.GetParameters().Where(c => !c.IsOut).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameter>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType.IsArray ? p.ParameterType.GetElementType() : p.ParameterType),
                            IsArray = p.ParameterType.IsArray
                        };
                    }).ToArray(),
                    OutputParameters = method.GetParameters().Where(c => c.IsOut).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameter>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType.GetElementType().IsArray ? p.ParameterType.GetElementType().GetElementType() : p.ParameterType.GetElementType()),
                            IsArray = p.ParameterType.GetElementType().IsArray
                        };
                    }).ToArray(),
                    ResultInputParameters = resultMethod?.GetParameters().Where(c => !c.IsOut).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameter>();
                        var indexAttribute = p.GetCustomAttribute<TransformFunctionIndex>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType.IsArray ? p.ParameterType.GetElementType() : p.ParameterType),
                            IsArray = p.ParameterType.IsArray,
                            IsIndex = indexAttribute != null
                        };
                    }).ToArray(),
                    ResultOutputParameters  = resultMethod?.GetParameters().Where(c => c.IsOut).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameter>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType.GetElementType().IsArray ? p.ParameterType.GetElementType().GetElementType() : p.ParameterType.GetElementType()),
                            IsArray = p.ParameterType.GetElementType().IsArray,
                        };
                    }).ToArray(),
                };

                return function;
            }

            return null;
        }

        public static List<FunctionReference> GetAllFunctions()
        {
            var types = AppDomain.CurrentDomain.GetAssemblies().Where(c=>c.FullName.StartsWith("dexih.functions"))
                .SelectMany(s => s.GetTypes());

            var methods = new List<FunctionReference>();
            
            foreach (var type in types)
            {
                foreach (var method in type.GetMethods())
                {
                   var function = GetFunction(type, method);
                   if(function != null)
                   {
                        methods.Add(function);
                    }
                }
            }
            return methods;
        }
    }
}