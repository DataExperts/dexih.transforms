using dexih.connections.test;
using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.sql.npgsql
{
    public class ConnectionNpgsqlTests
    {
        public ConnectionNpgSql GetConnection()
        {
            return new ConnectionNpgSql()
            {
                Name = "Test Connection",
                Ntauth = Convert.ToBoolean(Configuration.AppSettings["Npgsql:NTAuthentication"]),
                Username = Convert.ToString(Configuration.AppSettings["Npgsql:UserName"]),
                Password = Convert.ToString(Configuration.AppSettings["Npgsql:Password"]),
                Server = Convert.ToString(Configuration.AppSettings["Npgsql:ServerName"]),
            };
        }

        [Fact]
        public async Task Npgsql_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests().Unit(GetConnection(), database);
        }

        [Fact]
        public async Task Npgsql_Performance()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests().Performance(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task Npgsql_TransformWriter()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests().PerformanceTransformWriter(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task Npgsql_Transform()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public void Npgsql_Specific_Unit()
        {
            ConnectionSqlServer connection = new ConnectionSqlServer();

            //test delimiter
            Assert.Equal("\"table\"", connection.AddDelimiter("table"));
            Assert.Equal("\"table\"", connection.AddDelimiter("\"table\""));
            Assert.Equal("\"table\".\"schema\"", connection.AddDelimiter("\"table\".\"schema\""));
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
