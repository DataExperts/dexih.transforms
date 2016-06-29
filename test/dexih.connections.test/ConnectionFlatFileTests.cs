using dexih.connections.test;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.test
{
    public class ConnectionAzureTests
    {
        public ConnectionFlatFileLocal GetLocalConnection()
        {
            var connection  = new ConnectionFlatFileLocal()
            {
                Name = "Test Connection",
                ServerName = Convert.ToString(Helpers.AppSettings["FlatFileLocal:ServerName"]),
            };
            return connection;
        }

        public ConnectionFlatFileAzureFile GetAzureConnection()
        {
            var connection = new ConnectionFlatFileAzureFile()
            {
                Name = "Test Connection",
                ConnectionString = Convert.ToString(Helpers.AppSettings["FlatFileAzure:ConnectionString"]),
                UseConnectionString = true
            };
            return connection;
        }


        [Fact]
        public void FlatFileLocal_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            new UnitTests().Unit(GetLocalConnection(), database);
        }

        //[Fact]
        //public void FlatFileAzure_Basic()
        //{
        //    string database = "test" + Guid.NewGuid().ToString().Replace('-', 'a').Substring(1, 10);

        //    new UnitTests().Unit(GetAzureConnection(), database);
        //}
    }
}
