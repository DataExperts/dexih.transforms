using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.db2;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sql
{
    [Collection("SqlTest")]
    public class ConnectionDB2Tests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionDB2Tests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionDB2 GetConnection()
        {
            return new ConnectionDB2()
            {
                Name = "Test Connection",
                UseWindowsAuth = false,
                Server = Configuration.AppSettings["DB2:ServerName"].ToString(),
                Username = Configuration.AppSettings["DB2:UserName"].ToString(),
                Password = Configuration.AppSettings["DB2:Password"].ToString()
            };
        }

        [Fact]
        public async Task DB2_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task DB2_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task DB2_PerformanceTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            await new PerformanceTests(_output).Performance(GetConnection(), database, 10000);
        }
        
        [Fact]
        public async Task DB2_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
        
        [Fact]
        public async Task DB2_SqlReader()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();

            await new SqlReaderTests().Unit(connection, database);
        }
    }
}
