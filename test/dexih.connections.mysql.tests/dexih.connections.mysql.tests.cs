using dexih.connections.test;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sql
{
    [Collection("SqlTest")]
    public class ConnectionMySqlTests
    {
                
        private readonly ITestOutputHelper _output;

        public ConnectionMySqlTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
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
        public async Task MySql_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();
            ConnectionMySql connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task MySql_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task MySql_PerformanceTests()
        {
            await new PerformanceTests(_output).Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 10000);
        }
        
        [Fact]
        public async Task MySql_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
        
        [Fact]
        public async Task MySql_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new SqlReaderTests().Unit(connection, database);
        }
        
        [Fact]
        public async Task MySql_Transaction_Writer()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTarget().ParentChild_Write(connection, database, false, true);
        }
        
        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task MySql_ParentChild_Write(bool useDbAutoIncrement, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTarget().ParentChild_Write(connection, database, useDbAutoIncrement, useTransaction);
        }
    }
}
