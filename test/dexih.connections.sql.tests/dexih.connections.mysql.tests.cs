using dexih.connections.test;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.sql
{
    public class ConnectionMySqlTests
    {
        public ConnectionMySql GetConnection()
        {
            return new ConnectionMySql()
            {
                Name = "Test Connection",
                UseWindowsAuth = false,
                Server = Configuration.AppSettings["MySql:ServerName"].ToString(),
                Username = Configuration.AppSettings["MySql:UserName"].ToString(),
                Password = Configuration.AppSettings["MySql:Password"].ToString()
            };
        }

        [Fact]
        public async Task TestMySql_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();
            ConnectionMySql connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task TestMySql_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task TestMySql_PerformanceTests()
        {
            await new PerformanceTests().Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 10000);
        }
        
        [Fact]
        public async Task TestMySql_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests().PerformanceTransformWriter(GetConnection(), database, 100000);
        }
    }
}
