using System;
using System.Reflection;

namespace dexih.transforms.Transforms
{
    [Serializable]
    public class TransformReference: TransformAttribute
    {
        public string TransformClassName { get; set; }
        public string TransformAssemblyName { get; set; }
        
        public Type GetTransformType()
        {
            Type type;
            if (string.IsNullOrEmpty(TransformAssemblyName))
            {
                type = Assembly.GetExecutingAssembly().GetType(TransformClassName);
            }
            else
            {
                var assembly = Assembly.Load(TransformAssemblyName);

                if (assembly == null)
                {
                    throw new TransformnNotFoundException($"The assembly {TransformClassName} was not found.");
                }
                type = assembly.GetType(TransformClassName);
            }

            return type;
        }
        
        public Transform GetTransform()
        {
            var type = GetTransformType();
            var obj = (Transform) Activator.CreateInstance(type);
            return obj;
        }
    }
}