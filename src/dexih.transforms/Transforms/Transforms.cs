using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Dexih.Utils.CopyProperties;

namespace dexih.transforms.Transforms
{
    public class Transforms
    {
        public static TransformReference GetTransform(string className, string assemblyName = null)
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
                    throw new TransformnNotFoundException($"The assembly {assemblyName} was not found.");
                }
                type = assembly.GetType(className);
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
            var types = AppDomain.CurrentDomain.GetAssemblies().Where(c=>c.FullName.StartsWith("dexih.transforms"))
                .SelectMany(s => s.GetTypes());

            var transforms = new List<TransformReference>();
            
            foreach (var type in types)
            {
                var transform = GetTransform(type);
                if (transform != null)
                {
                    transforms.Add(transform);
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