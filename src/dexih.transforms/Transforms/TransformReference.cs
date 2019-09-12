using ProtoBuf;
using System;
using System.Reflection;

namespace dexih.transforms.Transforms
{
    [ProtoContract]
    public class TransformReference: TransformAttribute
    {
        [ProtoMember(1)]
        public string TransformClassName { get; set; }

        [ProtoMember(2)]
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