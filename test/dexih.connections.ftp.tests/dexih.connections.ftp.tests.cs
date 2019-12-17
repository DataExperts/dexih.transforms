using dexih.connections.test;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.ftp
{
    public class ConnectionFtpFlatFileTests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionFtpFlatFileTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionFlatFileFtp GetConnection()
        {
            var serverName = Convert.ToString(Configuration.AppSettings["FlatFileFtp:ServerName"]);
            var userName = Convert.ToString(Configuration.AppSettings["FlatFileFtp:UserName"]);
            var password = Convert.ToString(Configuration.AppSettings["FlatFileFtp:Password"]);
            if (serverName == "")
                return null;

            var connection  = new ConnectionFlatFileFtp()
            {
                Name = "Test Connection",
                Server = serverName,
                Username = userName,
                Password =  password
            };
            return connection;
        }

        [Fact]
        public async Task FlatFileFtp_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests(_output).Unit(GetConnection(), database);
        }
    }
}
