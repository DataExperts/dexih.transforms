using dexih.connections.test;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.excel
{
    public class ConnectionExcelTests
    {
        public ConnectionExcel GetLocalConnection()
        {
            var serverName = Convert.ToString(Configuration.AppSettings["Excel:ServerName"]);
            if (serverName == "")
                return null;

            if (!Directory.Exists(serverName))
            {
                Directory.CreateDirectory(serverName);
            }

            var connection  = new ConnectionExcel()
            {
                Name = "Test Connection",
                Server = serverName,
            };
            return connection;
        }

        [Fact]
        public async Task Excel_Basic()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new UnitTests().Unit(GetLocalConnection(), database);
        }
        
        [Fact]
        public async Task Excel_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetLocalConnection(), database);
        }

        [Fact]
        public async Task Excel_PerformanceTests()
        {
            await new PerformanceTests().Performance(GetLocalConnection(), "Test-" + Guid.NewGuid().ToString(), 1000);
        }
        
        [Fact]
        public async Task Excel_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetLocalConnection();

            await new SqlReaderTests().Unit(connection, database);
        }

    }
}
