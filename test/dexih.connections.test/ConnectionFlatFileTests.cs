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
        public ConnectionFlatFileLocal GetConnection()
        {
            var connection  = new ConnectionFlatFileLocal()
            {
                Name = "Test Connection",
                ServerName = Convert.ToString(Helpers.AppSettings["FlatFileLocal:ServerName"]),
            };
            return connection;
        }

        [Fact]
        public void FlatFileLocal_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            new UnitTests().Unit(GetConnection(), database);
        }

    }
}
