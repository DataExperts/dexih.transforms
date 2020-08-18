using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.db2;
using dexih.transforms;
using dexih.transforms.tests;
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
            await new UnitTests(_output).Unit(connection, database);
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

            await new SqlReaderTests(_output).Unit(connection, database);
        }
        
        [Theory]
        [InlineData(false, EUpdateStrategy.Reload, false)]
        [InlineData(false, EUpdateStrategy.AppendUpdateDelete, false)]
        [InlineData(false, EUpdateStrategy.Reload, true)]
        [InlineData(true, EUpdateStrategy.Reload, true)]
        public async Task ParentChild_Write(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }

        [Theory]
        [InlineData(false, EUpdateStrategy.Reload, false)]
        [InlineData(false, EUpdateStrategy.AppendUpdateDelete, false)]
        [InlineData(false, EUpdateStrategy.Reload, true)]
        [InlineData(true, EUpdateStrategy.Reload, true)]
        public async Task ParentChild_Write_Large(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write_Large(connection, 1000, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }
        
        [Fact]
        public async Task Sqlite_SelectQuery()
        {
            var database = "Test-" + Guid.NewGuid();
            var connection = GetConnection();

            await new SelectQueryTests(_output).SelectQuery(connection, database);
        }

        [Theory]
        [InlineData(EJoinStrategy.Auto, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Database, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Sorted, EJoinStrategy.Sorted)]
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task Sqlite_Join(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinDatabase(connection, joinStrategy, usedJoinStrategy);
        }
        
        
        [Theory]
        [InlineData(EJoinStrategy.Auto, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Database, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Sorted, EJoinStrategy.Sorted)]
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task Sqlite_JoinDatabaseFilterNull(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinDatabaseJoinMissingException(connection, joinStrategy, usedJoinStrategy);
        }
    }
}
