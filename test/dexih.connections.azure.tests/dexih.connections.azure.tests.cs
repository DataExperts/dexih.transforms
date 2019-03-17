using dexih.connections.test;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.azure
{
    public class ConnectionAzureTests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionAzureTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionAzureTable GetConnection()
        {
            return new ConnectionAzureTable()
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
        public async Task Azure_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests().Unit(GetConnection(), database);
        }

        [Fact]
        public async Task Azure_Transform()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task Azure_Performance()
        {
            await new PerformanceTests(_output).Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 100);
        }



    }
}
