using System;
using System.IO;
using System.Reflection;

namespace dexih.transforms
{
    [Serializable]
    public class ConnectionReference : ConnectionAttribute
    {
        public string ConnectionAssemblyName { get; set; }
        public string ConnectionClassName { get; set; }
        
        public Type GetConnectionType()
        {
            Type type;
            if (string.IsNullOrEmpty(ConnectionAssemblyName))
            {
                type = Assembly.GetExecutingAssembly().GetType(ConnectionClassName);
            }
            else
            {
                var pathName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), ConnectionAssemblyName);
                var assembly = Assembly.LoadFile(pathName);

                if (assembly == null)
                {
                    throw new ConnectionNotFoundException($"The assembly {ConnectionAssemblyName} was not found.");
                }
                type = assembly.GetType(ConnectionClassName);
            }

            return type;
        }
        
        public Connection GetConnection()
        {
            var type = GetConnectionType();
            var obj = (Connection) Activator.CreateInstance(type);
            return obj;
        }
        
    }
}