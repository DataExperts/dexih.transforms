using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using dexih.transforms.Exceptions;
using Dexih.Utils.CopyProperties;


namespace dexih.transforms
{
    public static class Connections
    {
        public static (string path, string pattern)[] SearchPaths()
        {
            return new[]
            {
                (Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "dexih.connections.*.dll"),
                (Path.Combine(Directory.GetCurrentDirectory(), "plugins", "standard"), "dexih.connections.*.dll"),
                (Path.Combine(Directory.GetCurrentDirectory(), "plugins", "connections"), "*.dll")
            };
        }
         
        public static ConnectionReference GetConnection(string className, string assemblyName = null)
        {
            Type type = null;

            if (string.IsNullOrEmpty(className))
            {
                throw new ConnectionException("There is no connection type specified.");
            }

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
                throw new ConnectionNotFoundException($"The type {className} was not found in assembly {assemblyName}.");
            }

            var connection = GetConnection(type);
            connection.ConnectionClassName = className;
            connection.ConnectionAssemblyName = assemblyName;

            return connection;
        }

        public static ConnectionReference GetConnection(Type type)
        {
            // ConnectionFlatFile doesn't have attribute as it's only used when importing flat files into non-flatfile connections
            // so return a dummy ConnectionReference.
            if (type == typeof(ConnectionFlatFile))
            {
                return new ConnectionReference()
                {
                    ConnectionCategory = Connection.EConnectionCategory.File,
                    AllowsFlatFiles = true,
                };
            }
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
            var connections = new List<ConnectionReference>();

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
                            var connection = GetConnection(type);
                            if (connection != null)
                            {
                                connection.ConnectionAssemblyName = assemblyName;
                                connections.Add(connection);
                            }
                        }
                    }
                }
            }
            
            return connections;
        }
    }
}