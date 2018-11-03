using dexih.connections.test;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sql
{
    [Collection("SqlTest")]
    public class ConnectionOracleTests
    {        
        private readonly ITestOutputHelper _output;

        public ConnectionOracleTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionOracle GetConnection()
        {
            return new ConnectionOracle()
            {
                Name = "Test Connection",
                UseWindowsAuth = false,
                Server = Configuration.AppSettings["Oracle:ServerName"].ToString(),
                Username = Configuration.AppSettings["Oracle:UserName"].ToString(),
                Password = Configuration.AppSettings["Oracle:Password"].ToString()
            };
        }

        [Fact]
        public async Task TestOracle_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task TestOracle_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task TestOracle_PerformanceTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            await new PerformanceTests(_output).Performance(GetConnection(), database, 10000);
        }
        
        [Fact]
        public async Task TestOracle_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
        
        [Fact]
        public async Task TestOracle_SqlReader()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();

            await new SqlReaderTests().Unit(connection, database);
        }
    }
}
