using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.sqlite;
using dexih.transforms;
using dexih.transforms.tests;
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
            string database = "Test-" + Guid.NewGuid();
            ConnectionSqlite connection = GetConnection();
            await new UnitTests(_output).Unit(connection, database);
        }

        [Fact]
        public async Task Sqlite_Transform()
        {
            string database = "Test-" + Guid.NewGuid();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task Sqlite_Performance()
        {
            await new PerformanceTests(_output).Performance(GetConnection(), "Test-" + Guid.NewGuid(), 50000);
        }
        
        [Fact]
        public async Task Sqlite_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid();
            var connection = GetConnection();

            await new SqlReaderTests(_output).Unit(connection, database);
        }

        [Theory]
        [InlineData(false, EUpdateStrategy.Reload, false)]
        [InlineData(false, EUpdateStrategy.Reload, true)]
        [InlineData(true, EUpdateStrategy.Reload, true)]
        public async Task Sqlite_ParentChild_Write(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }
        
        [Fact]
        public async Task Sqlite_TransformWriter()
        {
            var database = "Test-" + Guid.NewGuid();

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }
        
        [Theory]
        [InlineData(false, EUpdateStrategy.Reload, false)]
        [InlineData(false, EUpdateStrategy.AppendUpdateDelete, false)]
        [InlineData(false, EUpdateStrategy.Reload, true)]
        [InlineData(true, EUpdateStrategy.Reload, true)]
        public async Task Sqlite_ParentChild_Write_Large(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid();
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
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task Sqlite_JoinTwoTablesDatabase(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinTwoTablesDatabase(connection, joinStrategy, usedJoinStrategy);
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
        
        [Theory]
        [InlineData(EJoinStrategy.Auto, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Database, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Sorted, EJoinStrategy.Sorted)]
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task Sqlite_JoinAndGroupDatabase(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinAndGroupDatabase(connection, joinStrategy, usedJoinStrategy);
        }
    }
}
