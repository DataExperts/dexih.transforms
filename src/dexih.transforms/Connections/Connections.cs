using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Dexih.Utils.CopyProperties;


namespace dexih.transforms
{
    public class Connections
    {
         public static ConnectionReference GetConnection(string className, string assemblyName = null)
        {
            Type type;

            if (string.IsNullOrEmpty(assemblyName))
            {
                type = Assembly.GetExecutingAssembly().GetType(className);
            }
            else
            {
                var pathName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), assemblyName);
                var assembly = Assembly.LoadFile(pathName);

                if (assembly == null)
                {
                    throw new ConnectionNotFoundException($"The assembly {assemblyName} was not found.");
                }
                type = assembly.GetType(className);
            }

            if (type == null)
            {
                throw new ConnectionNotFoundException($"The type {className} was not found.");
            }

            var connection = GetConnection(type);
            connection.ConnectionClassName = className;
            connection.ConnectionAssemblyName = assemblyName;

            return connection;
        }

        public static ConnectionReference GetConnection(Type type)
        {
            var attribute = type.GetCustomAttribute<ConnectionAttribute>();
            if (attribute != null)
            {
                var connection = attribute.CloneProperties<ConnectionReference>();
                connection.ConnectionClassName = type.FullName;
                connection.ConnectionAssemblyName = type.Assembly.FullName;
                return connection;
            }

            return null;
        }

        public static List<ConnectionReference> GetAllConnections()
        {
            var path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var files = Directory.GetFiles(path, "dexih.connections.*.dll");

            var pluginsPath = Path.Combine(path, "plugins", "connections");
            if (Directory.Exists(pluginsPath))
            {
                files = files.Concat(Directory.GetFiles(pluginsPath, "*.dll")).ToArray();
            }
            
            var connections = new List<ConnectionReference>();

            foreach (var file in Directory.GetFiles(path, "dexih.connections.*.dll"))
            {
                var assembly = Assembly.LoadFile(file);
                foreach (var type in assembly.GetTypes())
                {
                    var connection = GetConnection(type);
                    if (connection != null)
                    {
                        connection.ConnectionAssemblyName = file.Substring(path.Length+1);
                        connections.Add(connection);
                    }
                }
            }
            
            return connections;
        }
    }
}