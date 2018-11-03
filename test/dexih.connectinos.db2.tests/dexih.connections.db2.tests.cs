using dexih.connections.test;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.sql
{
    [Collection("SqlTest")]
    public class ConnectionOracleTests
    {
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
            await new PerformanceTests().Performance(GetConnection(), database, 10000);
        }
        
        [Fact]
        public async Task TestOracle_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);

            await new PerformanceTests().PerformanceTransformWriter(GetConnection(), database, 100000);
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
