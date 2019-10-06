using MessagePack;
using System;
using System.IO;
using System.Reflection;

namespace dexih.transforms
{
    [MessagePackObject()]
    public class ConnectionReference : ConnectionAttribute
    {
        [Key(15)]
        public string ConnectionAssemblyName { get; set; }

        [Key(16)]
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
                var location = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                if (string.IsNullOrEmpty(location))
                {
                    throw new ConnectionNotFoundException($"The assembly {ConnectionAssemblyName} was not found.");
                }

                var pathName = Path.Combine(location, ConnectionAssemblyName);
                var assembly = Assembly.LoadFile(pathName);

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