using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Dexih.Utils.CopyProperties;

namespace dexih.transforms.Transforms
{
    public class Transforms
    {
        public static (string path, string pattern)[] SearchPaths()
        {
            return new[]
            {
                (Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "dexih.transforms*.dll"),
                (Path.Combine(Directory.GetCurrentDirectory(), "plugins", "transforms"), "*.dll")
            };
        }
        
        public static TransformReference GetTransform(string className, string assemblyName = null)
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
                        if (File.Exists(fileName))
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
                throw new TransformnNotFoundException($"The type {className} was not found.");
            }

            var transform = GetTransform(type);
            transform.TransformClassName = className;
            transform.TransformAssemblyName = assemblyName;

            return transform;
        }

        public static TransformReference GetTransform(Type type)
        {
            var attribute = type.GetCustomAttribute<TransformAttribute>();
            if (attribute != null)
            {
                var transform = attribute.CloneProperties<TransformReference>();
                transform.TransformClassName = type.FullName;
                return transform;
            }

            return null;
        }

        public static List<TransformReference> GetAllTransforms()
        {
            var transforms = new List<TransformReference>();
            
            foreach (var path in SearchPaths())
            {
                if (Directory.Exists(path.path))
                {
                    foreach (var file in Directory.GetFiles(path.path, path.pattern))
                    {
                        var assembly = Assembly.LoadFrom(file);
                        var assemblyName = Path.GetFileName(file);
                        if (assemblyName == Path.GetFileName(Assembly.GetExecutingAssembly().Location))
                        {
                            assemblyName = null;
                        }
                        foreach (var type in assembly.GetTypes())
                        {
                            var transform = GetTransform(type);
                            if (transform != null)
                            {
                                transform.TransformAssemblyName = assemblyName;
                                transforms.Add(transform);
                            }
                        }
                    }
                }
            }

            return transforms;
        }

        public static TransformReference GetDefaultMappingTransform()
        {
            var mapping = typeof(TransformMapping);
            return GetTransform(mapping);
        }
    }
}