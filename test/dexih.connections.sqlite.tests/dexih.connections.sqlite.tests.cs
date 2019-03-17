using dexih.connections.test;
using System;
using System.ComponentModel;
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
        public async Task Sqlite_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();
            ConnectionSqlite connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task Sqlite_Transform()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task Sqlite_Performance()
        {
            await new PerformanceTests(_output).Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 50000);
        }
        
        [Fact]
        public async Task Sqlite_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new SqlReaderTests().Unit(connection, database);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task Sqlite_ParentChild_Write(bool useDbAutoIncrement, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTarget().ParentChild_Write(connection, database, useDbAutoIncrement, useTransaction);
        }
        
        [Fact]
        public async Task Sqlite_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
    }
}
