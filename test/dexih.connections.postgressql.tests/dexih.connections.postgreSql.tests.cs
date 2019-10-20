using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.postgressql;
using dexih.transforms;
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
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.AppendUpdateDelete, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, true)]
        [InlineData(true, TransformDelta.EUpdateStrategy.Reload, true)]
        public async Task Postgres_ParentChild_Write(bool useDbAutoIncrement, TransformDelta.EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }

        [Theory]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.AppendUpdateDelete, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, true)]
        [InlineData(true, TransformDelta.EUpdateStrategy.Reload, true)]
        public async Task Postgres_ParentChild_Write_Large(bool useDbAutoIncrement, TransformDelta.EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write_Large(connection, 1000, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }

        
        //[Fact]
        //public async Task SalesDetail()
        //{
        //    ConnectionSqlServer connection = new ConnectionSqlServer()
        //    {
        //        NtAuthentication = true,
        //        ServerName = "(localdb)\\v11.0",
        //        DefaultDatabase = "MyAdventureWorks"
        //    };

        //    var tableResult = await connection.GetSourceTableInfo("\"Sales\".\"SalesOrderDetail\"", null);
        //    Assert.True(tableResult.Success);

        //    Table salesOrder = tableResult.Value;

        //    string database = "Test-" + Guid.NewGuid().ToString();
        //    Connection targetConnection = GetConnection();
        //    var returnValue = await targetConnection.CreateDatabase(database);
        //    Assert.True(returnValue.Success, "New Database - Message:" + returnValue.Message);

        //    var targetTable = salesOrder.Copy();
        //    targetTable.AddAuditColumns();
        //    targetTable.TableName = "TargetTable";
        //    await targetConnection.CreateTable(targetTable);
        //    Transform targetTransform = targetConnection.GetTransformReader(targetTable);

        //    //count rows using reader
        //    Transform transform = connection.GetTransformReader(salesOrder);
        //    transform = new TransformMapping(transform, true, null, null);
        //    transform = new TransformValidation(transform, null, true);
        //    transform = new TransformDelta(transform, targetTransform, TransformDelta.EUpdateStrategy.AppendUpdate, 1, 1);

        //    TransformWriter writer = new TransformWriter();
        //    TransformWriterResult writerResult = new TransformWriterResult();
        //    var result = await writer.WriteAllRecords(writerResult, transform, targetTable, connection, null, null, CancellationToken.None);

        //    Assert.Equal(121317, writerResult.RowsCreated);
        //}
    }
}
