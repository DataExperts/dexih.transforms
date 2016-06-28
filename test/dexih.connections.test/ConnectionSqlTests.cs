using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.test
{
    public class ConnectionSqlTests
    {
        public ConnectionSql GetConnection()
        {
            return new ConnectionSqlServer()
            {
                Name = "Test Connection",
                NtAuthentication = Convert.ToBoolean(Helpers.AppSettings["SqlServer:NTAuthentication"]),
                UserName = Convert.ToString(Helpers.AppSettings["SqlServer:UserName"]),
                Password = Convert.ToString(Helpers.AppSettings["SqlServer:Password"]),
                ServerName = Convert.ToString(Helpers.AppSettings["SqlServer:ServerName"]),
            };
        }

        [Fact]
        public void TestSqlServer_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            new UnitTests().Unit(GetConnection(), database);
        }

        [Fact]
        public void TestSqlServer_PerformanceTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            new PerformanceTests().Performance(GetConnection(), database, 1000);
        }

        [Fact]
        public void TestSqlServer_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            new TransformTests().Transform(GetConnection(), database);
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
