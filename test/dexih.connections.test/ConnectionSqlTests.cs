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
            return new ConnectionSql()
            {
                Name = "Test Connection",
                NtAuthentication = Convert.ToBoolean(Helpers.AppSettings["SqlServer:NTAuthentication"]),
                ServerName = Convert.ToString(Helpers.AppSettings["SqlServer:ServerName"]),
            };
        }

        [Fact]
        public void TestSqlServer_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            CommonTests.UnitTests(GetConnection(), database);
        }

        [Fact]
        public void TestSqlServer_PerformanceTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            CommonTests.PerformanceTests(GetConnection(), database);
        }
    }
}
