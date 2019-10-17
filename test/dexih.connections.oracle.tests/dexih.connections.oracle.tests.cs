using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.oracle;
using dexih.transforms;
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
        public async Task Oracle_Basic()
        {
            string database = "Test" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task Oracle_TransformTests()
        {
            string database = "Test" + Guid.NewGuid().ToString().Substring(0,8);

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task Oracle_PerformanceTests()
        {
            string database = "Test" + Guid.NewGuid().ToString().Substring(0,8);
            await new PerformanceTests(_output).Performance(GetConnection(), database, 10000);
        }
        
        [Fact]
        public async Task Oracle_TransformWriter()
        {
            string database = "Test" + Guid.NewGuid().ToString().Substring(0,8);

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
        
        [Fact]
        public async Task Oracle_SqlReader()
        {
            string database = "Test" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();

            await new SqlReaderTests(_output).Unit(connection, database);
        }
        
        [Theory]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, true)]
        [InlineData(true, TransformDelta.EUpdateStrategy.Reload, true)]
        public async Task Oracle_ParentChild_Write(bool useDbAutoIncrement, TransformDelta.EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString().Substring(0,8);
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }
    }
}
