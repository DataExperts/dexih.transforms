using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.connections.test
{
    public static class Helpers
    {
        public static IConfigurationSection AppSettings { get; set; }

        static Helpers()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddUserSecrets()
                .AddEnvironmentVariables();

            var Configuration = builder.Build();
            AppSettings = Configuration.GetSection("AppSettings");

        }

        public static string EncryptionKey()
        {
            return AppSettings["EncryptionKey"];
        }

        public static string TestExportsPath()
        {
            //return "C:\\Users\\Gary\\OneDrive\\Information Hub\\tests";
            //return "Z:\\OneDrive\\Information Hub\\tests";

            return Directory.GetCurrentDirectory() + "\\testfiles";
        }
    }
}
