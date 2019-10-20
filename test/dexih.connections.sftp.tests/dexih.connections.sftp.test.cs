using dexih.connections.test;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sftp
{
    public class ConnectionSftpFlatFileTests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionSftpFlatFileTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionFlatFileSftp GetConnection()
        {
            var serverName = Convert.ToString(Configuration.AppSettings["FlatFileSftp:ServerName"]);
            var userName = Convert.ToString(Configuration.AppSettings["FlatFileSftp:UserName"]);
            var password = Convert.ToString(Configuration.AppSettings["FlatFileSftp:Password"]);
            if (serverName == "")
                return null;

            var connection  = new ConnectionFlatFileSftp()
            {
                Name = "Test Connection",
                Server = serverName,
                Username = userName,
                Password =  password
            };
            return connection;
        }

        [Fact]
        public async Task FlatFileLocal_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests(_output).Unit(GetConnection(), database);
        }
    }
}
