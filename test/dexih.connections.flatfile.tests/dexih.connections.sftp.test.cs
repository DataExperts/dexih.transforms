using dexih.connections.test;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.sftp
{
    public class ConnectionSftpFlatFileTests
    {
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

            await new UnitTests().Unit(GetConnection(), database);
        }
    }
}
