using dexih.connections.test;
using System;
using System.Threading.Tasks;
using dexih.connections.sqlserver;
using dexih.transforms;
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

        //[Fact]
        //public void TestSqlServer_Specific_Unit()
        //{
        //    ConnectionSqlServer connection = new ConnectionSqlServer();

        //    //test delimiter
        //    Assert.Equal("\"table\"", connection.AddDelimiter("table"));
        //    Assert.Equal("\"table\"", connection.AddDelimiter("\"table\""));
        //    Assert.Equal("\"table\".\"schema\"", connection.AddDelimiter("\"table\".\"schema\""));
        //}

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
        //    transform = new TransformDelta(transform, targetTransform, EUpdateStrategy.AppendUpdate, 1, 1);

        //    TransformWriter writer = new TransformWriter();
        //    TransformWriterResult writerResult = new TransformWriterResult();
        //    var result = await writer.WriteAllRecords(writerResult, transform, targetTable, connection, null, null, CancellationToken.None);

        //    Assert.Equal(121317, writerResult.RowsCreated);
        //}
    }
}
