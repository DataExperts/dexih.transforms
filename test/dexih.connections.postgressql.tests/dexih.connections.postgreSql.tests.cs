using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.postgressql;
using dexih.transforms;
using dexih.transforms.tests;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.sql.npgsql
{
    [Collection("SqlTest")]
    public class ConnectionPostgreSqlTests
    {
                
        private readonly ITestOutputHelper _output;

        public ConnectionPostgreSqlTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionPostgreSql GetConnection()
        {
            return new ConnectionPostgreSql()
            {
                Name = "Test Connection",
                UseWindowsAuth = Convert.ToBoolean(Configuration.AppSettings["PostgreSql:NTAuthentication"]),
                Username = Convert.ToString(Configuration.AppSettings["PostgreSql:UserName"]),
                Password = Convert.ToString(Configuration.AppSettings["PostgreSql:Password"]),
                Server = Convert.ToString(Configuration.AppSettings["PostgreSql:ServerName"]),
            };
        }

        [Fact]
        public async Task Postgres_Basic()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new UnitTests(_output).Unit(GetConnection(), database);
        }

        [Fact]
        public async Task Postgres_Performance()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new PerformanceTests(_output).Performance(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task Postgres_TransformWriter()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task Postgres_Transform()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new TransformTests().Transform(GetConnection(), database);
        }

        //[Fact]
        //public void Postgres_Specific_Unit()
        //{
        //    var connection = new ConnectionPostgreSql();

        //    //test delimiter
        //    Assert.Equal("\"table\"", connection.AddDelimiter("table"));
        //    Assert.Equal("\"table\"", connection.AddDelimiter("\"table\""));
        //    Assert.Equal("\"table\".\"schema\"", connection.AddDelimiter("\"table\".\"schema\""));
        //}
        
        [Fact]
        public async Task Postgres_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new SqlReaderTests(_output).Unit(connection, database);
        }
        
        [Theory]
        [InlineData(false, EUpdateStrategy.Reload, false)]
        [InlineData(false, EUpdateStrategy.AppendUpdateDelete, false)]
        [InlineData(false, EUpdateStrategy.Reload, true)]
        [InlineData(true, EUpdateStrategy.Reload, true)]
        public async Task Postgres_ParentChild_Write(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
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
        public async Task Postgres_ParentChild_Write_Large(bool useDbAutoIncrement, EUpdateStrategy updateStrategy, bool useTransaction)
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
