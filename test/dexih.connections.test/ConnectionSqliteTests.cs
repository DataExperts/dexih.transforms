using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.test
{
    public class ConnectionSqliteTests
    {
        public ConnectionSqlite GetConnection()
        {
            return new ConnectionSqlite()
            {
                Name = "Test Connection",
                NtAuthentication = Convert.ToBoolean( Helpers.AppSettings["Sqlite:NTAuthentication"]),
                ServerName = Helpers.AppSettings["Sqlite:ServerName"].ToString()
            };
        }

        [Fact]
        public void TestSqlite_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

             CommonTests.UnitTests(GetConnection(), database);
        }

        [Fact]
        public void TestSqlite_PerformanceTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            CommonTests.PerformanceTests(GetConnection(), database);
        }
    }
}
