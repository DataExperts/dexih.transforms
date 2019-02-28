using dexih.connections.test;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sql
{
    [Collection("SqlTest")]
    public class ConnectionSqliteTests
    {
                
        private readonly ITestOutputHelper _output;

        public ConnectionSqliteTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionSqlite GetConnection()
        {
            return new ConnectionSqlite()
            {
                Name = "Test Connection",
                UseWindowsAuth = Convert.ToBoolean(Configuration.AppSettings["Sqlite:NTAuthentication"]),
                Server = Configuration.AppSettings["Sqlite:ServerName"],
                DefaultDatabase = "Test-" + Guid.NewGuid()
            };
        }

        [Fact]
        public async Task TestSqlite_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();
            ConnectionSqlite connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task TestSqlite_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task TestSqlite_PerformanceTests()
        {
            await new PerformanceTests(_output).Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 50000);
        }
        
        [Fact]
        public async Task TestSqlite_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new SqlReaderTests().Unit(connection, database);
        }

        [Fact]
        public async Task TestSqlite_Transaction_Writer()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTransactional().Unit(connection, database);
        }
        
        [Fact]
        public async Task TestSqlite_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
    }
}
