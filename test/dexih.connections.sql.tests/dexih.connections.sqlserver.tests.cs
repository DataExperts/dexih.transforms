using dexih.connections.test;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.sql.sqlserver
{
    public class ConnectionSqlTests
    {
        public ConnectionSql GetConnection()
        {
            return new ConnectionSqlServer()
            {
                Name = "Test Connection",
                NtAuthentication = Convert.ToBoolean(Configuration.AppSettings["SqlServer:NTAuthentication"]),
                UserName = Convert.ToString(Configuration.AppSettings["SqlServer:UserName"]),
                Password = Convert.ToString(Configuration.AppSettings["SqlServer:Password"]),
                ServerName = Convert.ToString(Configuration.AppSettings["SqlServer:ServerName"]),
            };
        }

        [Fact]
        public async Task SqlServer_Basic()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new UnitTests().Unit(GetConnection(), database);
        }

        [Fact]
        public async Task SqlServer_Performance()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new PerformanceTests().Performance(GetConnection(), database, 1000);
        }

        [Fact]
        public async Task SqlServer_Transform()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public void TestSqlServer_Specific_Unit()
        {
            ConnectionSqlServer connection = new ConnectionSqlServer();

            //test delimiter
            Assert.Equal("\"table\"", connection.AddDelimiter("table"));
            Assert.Equal("\"table\"", connection.AddDelimiter("\"table\""));
            Assert.Equal("\"table\".\"schema\"", connection.AddDelimiter("\"table\".\"schema\""));
        }
    }
}
