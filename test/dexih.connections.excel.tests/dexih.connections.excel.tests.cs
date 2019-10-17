using dexih.connections.test;
using System;
using System.IO;
using System.Threading.Tasks;
using dexih.transforms;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.excel
{
    public class ConnectionExcelTests
    {        
        
        private readonly ITestOutputHelper _output;

        public ConnectionExcelTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
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
            await new PerformanceTests(_output).Performance(GetLocalConnection(), "Test-" + Guid.NewGuid().ToString(), 1000);
        }
        
        [Fact]
        public async Task Excel_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetLocalConnection();

            await new SqlReaderTests(_output).Unit(connection, database);
        }
        
        [Theory]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, false)]
        public async Task Excel_ParentChild_Write(bool useDbAutoIncrement, TransformDelta.EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetLocalConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }

    }
}
