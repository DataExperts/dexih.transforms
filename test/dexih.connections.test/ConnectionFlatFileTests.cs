using dexih.connections.test;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.test
{
    public class ConnectionAzureFlatFileTests
    {
        public ConnectionFlatFileLocal GetLocalConnection()
        {
            var ServerName = Convert.ToString(Helpers.AppSettings["FlatFileLocal:ServerName"]);
            if (ServerName == "")
                return null;

            var connection  = new ConnectionFlatFileLocal()
            {
                Name = "Test Connection",
                ServerName = ServerName,
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

        [Fact]
        public void FlatFileAzure_Basic()
        {
            string database = "test" + Guid.NewGuid().ToString().Replace('-', 'a').Substring(1, 10);
            var con = GetAzureConnection();

            if(con != null)
                new UnitTests().Unit(con, database);
        }
    }
}
