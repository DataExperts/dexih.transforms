using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.sqlserver;
using dexih.transforms;
using dexih.transforms.tests;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sql.sqlserver
{
    [Collection("SqlTest")]
    public class ConnectionSqlTests
    {        
        private readonly ITestOutputHelper _output;

        public ConnectionSqlTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionSql GetConnection()
        {
            var connection = new ConnectionSqlServer()
            {
                Name = "Test Connection",
                UseWindowsAuth = Convert.ToBoolean(Configuration.AppSettings["SqlServer:NTAuthentication"]),
                Username = Convert.ToString(Configuration.AppSettings["SqlServer:UserName"]),
                Password = Convert.ToString(Configuration.AppSettings["SqlServer:Password"]),
                Server = Convert.ToString(Configuration.AppSettings["SqlServer:ServerName"]),
            };
            this._output.WriteLine($"Server: {connection.Server}, User: {connection.Username}, Password: {connection.Password}, UseWindowsAuth: {connection.UseWindowsAuth}, UseConnectionString: {connection.UseConnectionString}.");

            return connection;
        }

        [Fact]
        public async Task SqlServer_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests(_output).Unit(GetConnection(), database);
        }

        [Fact]
        public async Task SqlServer_Performance()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests(_output).Performance(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task SqlServer_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task SqlServer_Transform()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }
        
        [Fact]
        public async Task SqlServer_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new SqlReaderTests(_output).Unit(connection, database);
        }
        
        [Theory]
        [InlineData(false, EUpdateStrategy.Reload, false)]
        [InlineData(false, EUpdateStrategy.Reload, true)]
        [InlineData(true, EUpdateStrategy.Reload, true)]
        public async Task SqlServer_ParentChild_Write(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }
        
        [Fact]
        public async Task SqlServer_SelectQuery()
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
        public async Task SqlServer_Join(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinDatabase(connection, joinStrategy, usedJoinStrategy);
        }
        
        [Theory]
        [InlineData(EJoinStrategy.Auto, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Database, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task SqlServer_JoinTwoTablesDatabase(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinTwoTablesDatabase(connection, joinStrategy, usedJoinStrategy);
        }
        
        [Theory]
        [InlineData(EJoinStrategy.Auto, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Database, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Sorted, EJoinStrategy.Sorted)]
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task SqlServer_JoinDatabaseFilterNull(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinDatabaseJoinMissingException(connection, joinStrategy, usedJoinStrategy);
        }
        
        [Theory]
        [InlineData(EJoinStrategy.Auto, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Database, EJoinStrategy.Database)]
        [InlineData(EJoinStrategy.Sorted, EJoinStrategy.Sorted)]
        [InlineData(EJoinStrategy.Hash, EJoinStrategy.Hash)]
        public async Task SqlServer_JoinAndGroupDatabase(EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var connection = GetConnection();
            await new TransformJoinDbTests().JoinAndGroupDatabase(connection, joinStrategy, usedJoinStrategy);
        }


    }
}
