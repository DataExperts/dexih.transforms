
using System;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    [DataContract]
    public class ConnectionReference : ConnectionAttribute
    {
        [DataMember(Order = 18)]
        public string ConnectionAssemblyName { get; set; }

        [DataMember(Order = 19)]
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
                string pathName = null;
                foreach (var path in Connections.SearchPaths())
                {
                    var filePath = Path.Combine(path.path, ConnectionAssemblyName);

                    if (System.IO.File.Exists(filePath))
                    {
                        pathName = filePath;
                    }
                }

                if (pathName == null)
                {
                    throw new ConnectionException($"The assembly {ConnectionAssemblyName} could not be found.");
                }
                
                // var location = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                // if (string.IsNullOrEmpty(location))
                // {
                //     throw new ConnectionNotFoundException($"The assembly {ConnectionAssemblyName} was not found.");
                // }
                //
                // var pathName = Path.Combine(location, ConnectionAssemblyName);
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