using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using dexih.functions.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.functions
{
    public class Functions
    {
        public static (string path, string pattern)[] SearchPaths()
        {
            return new[]
            {
                (Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "dexih.functions*.dll"),
                (Path.Combine(Directory.GetCurrentDirectory(), "plugins", "functions"), "*.dll")
            };
        }
        
        public static Type GetFunctionType(string className, string assemblyName = null)
        {
            Type type = null;

            if (string.IsNullOrEmpty(assemblyName))
            {
                type = Assembly.GetExecutingAssembly().GetType(className);
            }
            else
            {
                foreach (var path in SearchPaths())
                {
                    if (Directory.Exists(path.path))
                    {
                        var fileName = Path.Combine(path.path, assemblyName);
                        if (System.IO.File.Exists(fileName))
                        {
                            var assembly = Assembly.LoadFile(fileName);
                            type = assembly.GetType(className);
                            break;
                        }
                    }
                }
            }

            if (type == null)
            {
                throw new FunctionNotFoundException($"The type {className} was not found.");
            }

            return type;
        }
        
        public static (Type type, MethodInfo method) GetFunctionMethod(string className, string methodName, string assemblyName = null)
        {
            var type = GetFunctionType(className, assemblyName);

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

        private static DataType.ETypeCode? GetElementType(Type p, out int rank)
        {
            if (p == typeof(void))
            {
                rank = 0;
                return null;
            }
            
            return DataType.GetTypeCode(p, out rank);   
        }
//
//        private static int GetRank(Type p)
//        {
//            if (typeof(void) == p) return 0;
//            return p.IsArray ? p.GetArrayRank() : 0;
//        }

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
                    Compare = compareAttribute?.Compare,
                    FunctionClassName = type.FullName,
                    
                    ReturnType = GetElementType(method.ReturnType, out var returnRank),
                    ReturnRank = returnRank,
                    InputParameters = method.GetParameters().Where(c =>
                    {
                        var variable = c.GetCustomAttribute<TransformFunctionVariableAttribute>();
                        return !c.IsOut && variable == null;
                    }).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameterAttribute>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType, out var paramRank),
                            Rank = paramRank,
                            IsTwin = p.GetCustomAttribute<TransformFunctionParameterTwinAttribute>() != null,
                            ListOfValues = EnumValues(p),
                            DefaultValue = DefaultValue(p)
                        };
                    }).ToArray(),
                    OutputParameters = method.GetParameters().Where(c => c.IsOut).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameterAttribute>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType, out var outParamRank),
                            Rank = outParamRank,
                            IsTwin = p.GetCustomAttribute<TransformFunctionParameterTwinAttribute>() != null,
                            ListOfValues = EnumValues(p)
                        };
                    }).ToArray(),
                    ResultReturnType = GetElementType(method.ReturnType, out var resultRank),
                    ResultReturnRank = resultRank,

                    ResultInputParameters = resultMethod?.GetParameters().Where(c =>
                    {
                        var variable = c.GetCustomAttribute<TransformFunctionVariableAttribute>();
                        return !c.IsOut && variable == null;
                    }).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameterAttribute>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType, out var paramRank),
                            Rank = paramRank,
                            IsTwin = p.GetCustomAttribute<TransformFunctionParameterTwinAttribute>() != null,
                            ListOfValues = EnumValues(p),
                            DefaultValue = DefaultValue(p)
                        };
                    }).ToArray(),
                    ResultOutputParameters = resultMethod?.GetParameters().Where(c => c.IsOut).Select(p =>
                    {
                        var paramAttribute = p.GetCustomAttribute<TransformFunctionParameterAttribute>();
                        return new FunctionParameter()
                        {
                            ParameterName = p.Name,
                            Name = paramAttribute?.Name?? p.Name,
                            Description = paramAttribute?.Description,
                            DataType = DataType.GetTypeCode(p.ParameterType, out var paramRank),
                            Rank = paramRank,
                            IsTwin = p.GetCustomAttribute<TransformFunctionParameterTwinAttribute>() != null,
                            ListOfValues = EnumValues(p)
                        };
                    }).ToArray(),
                };

                return function;
            }

            return null;
        }

        // convert enum to list of values
        private static string[] EnumValues(ParameterInfo p)
        {
            if (p.ParameterType.IsEnum)
            {
                return Enum.GetNames(p.ParameterType);
            }
            else
            {
                return null;
            }
        }

        private static object DefaultValue(ParameterInfo p)
        {
            if (p.ParameterType.IsEnum)
            {
                return p.DefaultValue?.ToString();
            }
            else
            {
                return p.DefaultValue;
            }
        }

        public static List<FunctionReference> GetAllFunctions()
        {
            var functions = new List<FunctionReference>();

            foreach (var path in SearchPaths())
            {
                if (Directory.Exists(path.path))
                {
                    foreach (var file in Directory.GetFiles(path.path, path.pattern))
                    {
                        var assembly = Assembly.LoadFrom(file);
                        var types = assembly.GetTypes();
                        // var types = AppDomain.CurrentDomain.GetAssemblies().Where(c=>c.FullName.StartsWith("dexih.functions")).SelectMany(s => s.GetTypes());

                        var assemblyName = Path.GetFileName(file);
                        if (assemblyName == Path.GetFileName(Assembly.GetExecutingAssembly().Location))
                        {
                            assemblyName = null;
                        }
                        foreach (var type in types)
                        {
                            foreach (var method in type.GetMethods())
                            {
                                var function = GetFunction(type, method);
                                if (function != null)
                                {
                                    function.FunctionAssemblyName = assemblyName;
                                    functions.Add(function);
                                }
                            }
                        }
                    }
                }
            }

            return functions;
        }
        
//        public static List<FunctionReference> GetAllFunctions()
//        {
//            var types = AppDomain.CurrentDomain.GetAssemblies().Where(c=>c.FullName.StartsWith("dexih.functions"))
//                .SelectMany(s => s.GetTypes());
//
//            var methods = new List<FunctionReference>();
//            
//            foreach (var type in types)
//            {
//                foreach (var method in type.GetMethods())
//                {
//                    var function = GetFunction(type, method);
//                    if(function != null)
//                    {
//                        methods.Add(function);
//                    }
//                }
//            }
//            return methods;
//        }
    }
}