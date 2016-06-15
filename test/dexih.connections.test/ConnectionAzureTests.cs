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
        public ConnectionAzure GetConnection()
        {
            return new ConnectionAzure()
            {
                //Name = "Test Connection",
                //ServerName = Convert.ToString(Helpers.AppSettings["Azure:ServerName"]),
                //UserName = Convert.ToString(Helpers.AppSettings["Azure:UserName"]),
                //Password = Convert.ToString(Helpers.AppSettings["Azure:Password"]),
                UseConnectionString = true,
                ConnectionString = "UseDevelopmentStorage=true"
            };


        }

        [Fact]
        public void TestAzure_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            CommonTests.UnitTests(GetConnection(), database, false);
        }

        [Fact]
        public void TestAzure_PerformanceTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            CommonTests.PerformanceTests(GetConnection(), database);
        }

    }
}
