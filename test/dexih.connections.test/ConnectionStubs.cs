using System;
using System.IO;
using dexih.connections.azure;
using dexih.connections.db2;
using dexih.connections.dexih;
using dexih.connections.excel;
using dexih.connections.flatfile;
using dexih.connections.ftp;
using dexih.connections.mysql;
using dexih.connections.oracle;
using dexih.connections.postgressql;
using dexih.connections.sftp;
using dexih.connections.sql;
using dexih.connections.sqlserver;
using dexih.connections.webservice;
using dexih.connections.webservice.restful;
using dexih.transforms;

namespace dexih.connections.test
{
    /// <summary>
    /// used to create new connections
    /// </summary>
    public static class ConnectionStubs
    {
        public enum EConnectionType
        {
            AzureTable, AzureFlatFile, Db2, Dexih, Excel, FlatFile, Ftp, MySql, Oracle, Postgres, Sftp, SqlServer, Restful, Memory
        }

        public static Connection CreateConnectionStub(EConnectionType connectionType)
        {
            
            switch(connectionType)
            {
                case EConnectionType.AzureTable:
                    return new ConnectionAzureTable
                    {
                        //Name = "Test Connection",
                        //ServerName = Convert.ToString(Helpers.AppSettings["Azure:ServerName"]),
                        //UserName = Convert.ToString(Helpers.AppSettings["Azure:UserName"]),
                        //Password = Convert.ToString(Helpers.AppSettings["Azure:Password"]),
                        UseConnectionString = true,
                        ConnectionString = "UseDevelopmentStorage=true"
                    };
                case EConnectionType.AzureFlatFile:
                    var ConnectionString = Convert.ToString(Configuration.AppSettings["FlatFileAzure:ConnectionString"]);
                    if (string.IsNullOrEmpty(ConnectionString))
                    {
                        return new ConnectionFlatFileAzureFile
                        {
                            Name = "Test Connection",
                            UseConnectionString = false
                        };
                    }

                    return new ConnectionFlatFileAzureFile
                    {
                        Name = "Test Connection",
                        ConnectionString = ConnectionString,
                        UseConnectionString = true
                    };
                case EConnectionType.Db2:
                    return new ConnectionDB2
                    {
                        Name = "Test Connection",
                        UseWindowsAuth = false,
                        Server = Configuration.AppSettings["DB2:ServerName"],
                        Username = Configuration.AppSettings["DB2:UserName"],
                        Password = Configuration.AppSettings["DB2:Password"]
                    };
                case EConnectionType.Dexih:
                    return new ConnectionDexih();
                case EConnectionType.Excel:
                    var serverName = Convert.ToString(Configuration.AppSettings["Excel:ServerName"]);
                    if (serverName == "")
                        return null;

                    if (!Directory.Exists(serverName))
                    {
                        Directory.CreateDirectory(serverName);
                    }

                    return new ConnectionExcel
                    {
                        Name = "Test Connection",
                        Server = serverName
                    };
                case EConnectionType.FlatFile:
                    return new ConnectionFlatFileLocal
                    {
                        Name = "Test Connection",
                        Server = Convert.ToString(Configuration.AppSettings["FlatFileLocal:ServerName"])
                    };
                case EConnectionType.Ftp:
                    return new ConnectionFlatFileFtp
                    {
                        Name = "Test Connection",
                        Server = Convert.ToString(Configuration.AppSettings["FlatFileFtp:ServerName"]),
                        Username = Convert.ToString(Configuration.AppSettings["FlatFileFtp:UserName"]),
                        Password =  Convert.ToString(Configuration.AppSettings["FlatFileFtp:Password"])
                    };                
                case EConnectionType.MySql:
                    return new ConnectionMySql
                    {
                        Name = "Test Connection",
                        UseWindowsAuth = false,
                        Server = Configuration.AppSettings["MySql:ServerName"],
                        Username = Configuration.AppSettings["MySql:UserName"],
                        Password = Configuration.AppSettings["MySql:Password"]
                    };
                case EConnectionType.Oracle:
                    return new ConnectionOracle
                    {
                        Name = "Test Connection",
                        UseWindowsAuth = false,
                        Server = Configuration.AppSettings["Oracle:ServerName"],
                        Username = Configuration.AppSettings["Oracle:UserName"],
                        Password = Configuration.AppSettings["Oracle:Password"]
                    };
                case EConnectionType.Postgres:
                    return new ConnectionPostgreSql
                    {
                        Name = "Test Connection",
                        UseWindowsAuth = Convert.ToBoolean(Configuration.AppSettings["PostgreSql:NTAuthentication"]),
                        Username = Convert.ToString(Configuration.AppSettings["PostgreSql:UserName"]),
                        Password = Convert.ToString(Configuration.AppSettings["PostgreSql:Password"]),
                        Server = Convert.ToString(Configuration.AppSettings["PostgreSql:ServerName"])
                    };
                case EConnectionType.Sftp:
                    return new ConnectionFlatFileSftp
                    {
                        Name = "Test Connection",
                        Server = Convert.ToString(Configuration.AppSettings["FlatFileSftp:ServerName"]),
                        Username = Convert.ToString(Configuration.AppSettings["FlatFileSftp:UserName"]),
                        Password =  Convert.ToString(Configuration.AppSettings["FlatFileSftp:Password"])
                    };
                case EConnectionType.SqlServer:
                    return new ConnectionSqlServer
                    {
                        Name = "Test Connection",
                        UseWindowsAuth = Convert.ToBoolean(Configuration.AppSettings["SqlServer:NTAuthentication"]),
                        Username = Convert.ToString(Configuration.AppSettings["SqlServer:UserName"]),
                        Password = Convert.ToString(Configuration.AppSettings["SqlServer:Password"]),
                        Server = Convert.ToString(Configuration.AppSettings["SqlServer:ServerName"])
                    };
                case EConnectionType.Restful:
                    return new ConnectionRestful
                    {
                        Server = "https://httpbin.org",
                        DefaultDatabase = ""
                    };
                case EConnectionType.Memory:
                    return new ConnectionMemory();
                default:
                    throw new ArgumentOutOfRangeException(nameof(connectionType), connectionType, null);
            }
            
            
        }
    }
}