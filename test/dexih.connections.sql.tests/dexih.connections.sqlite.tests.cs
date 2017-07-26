using dexih.connections.test;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.connections.sql
{
    public class ConnectionSqliteTests
    {
        public ConnectionSqlite GetConnection()
        {
            return new ConnectionSqlite()
            {
                Name = "Test Connection",
                UseWindowsAuth = Convert.ToBoolean(Configuration.AppSettings["Sqlite:NTAuthentication"]),
                Server = Configuration.AppSettings["Sqlite:ServerName"].ToString()
            };
        }

        [Fact]
        public async Task TestSqlite_BasicTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();
            ConnectionSqlite connection = GetConnection();
            await new UnitTests().Unit(connection, database);
        }

        [Fact]
        public async Task TestSqlite_TransformTests()
        {
            string database = "Test-" + Guid.NewGuid().ToString();

            await new TransformTests().Transform(GetConnection(), database);
        }

        [Fact]
        public async Task TestSqlite_PerformanceTests()
        {
            await new PerformanceTests().Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 10000);
        }
    }
}
