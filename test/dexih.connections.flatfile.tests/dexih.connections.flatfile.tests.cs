using dexih.connections.test;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.flatfile
{
    public class ConnectionLocalFlatFileTests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionLocalFlatFileTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionFlatFileLocal GetLocalConnection()
        {
            var ServerName = Convert.ToString(Configuration.AppSettings["FlatFileLocal:ServerName"]);
            if (ServerName == "")
                return null;

            var connection  = new ConnectionFlatFileLocal()
            {
                Name = "Test Connection",
                Server = ServerName,
            };
            return connection;
        }

        [Fact]
        public async Task FlatFileLocal_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests(_output).Unit(GetLocalConnection(), database);
        }
    }
}
