using dexih.connections.test;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.azure.tests
{
    public class ConnectionAzureFlatFileTests
    {
        public ConnectionFlatFileAzureFile GetAzureConnection()
        {
            var ConnectionString = Convert.ToString(Configuration.AppSettings["FlatFileAzure:ConnectionString"]);
            if (string.IsNullOrEmpty(ConnectionString))
            {
                var connection2 = new ConnectionFlatFileAzureFile()
                {
                    Name = "Test Connection",
                    UseConnectionString = false
                };
                return connection2;
            }

            var connection = new ConnectionFlatFileAzureFile()
            {
                Name = "Test Connection",
                ConnectionString = ConnectionString,
                UseConnectionString = true
            };
            return connection;
        }

        [Fact]
        public async Task FlatFileAzure_Basic()
        {
            string database = "test" + Guid.NewGuid().ToString().Replace('-', 'a').Substring(1, 10);
            var con = GetAzureConnection();

            Assert.NotNull(con);

            await new UnitTests().Unit(con, database);
        }

    }
}
