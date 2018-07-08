using dexih.functions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.webservice.restful.tests
{
    /// <summary>
    /// web services test.  These tests use the online web services hosted at https://httpbin.org
    /// and https://jsonplaceholder.typicode.com
    /// </summary>
    public class ConnectionWebServiceTests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionWebServiceTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public async Task WebService_Basic()
        {

            var connection = new ConnectionRestful()
            {
                Server = "https://httpbin.org",
                DefaultDatabase = "",
            };

            var table = await connection.GetSourceTableInfo("get", CancellationToken.None);
            Assert.True(table.Columns.GetOrdinal("args") >= 0);
            Assert.True(table.Columns.GetOrdinal("headers") >= 0);
            // Assert.True(table.Columns.GetOrdinal("headers.Accept") >= 0);
            // Assert.True(table.Columns.GetOrdinal("headers.Connection") >= 0);
            // Assert.True(table.Columns.GetOrdinal("headers.Host") >= 0);
            Assert.True(table.Columns.GetOrdinal("origin") >= 0);
            Assert.True(table.Columns.GetOrdinal("url") >= 0);

            var reader = connection.GetTransformReader(table);
            var openResult = await reader.Open(0, null, CancellationToken.None);
            Assert.True(openResult, "Reader open failed");

            var result = await reader.ReadAsync();
            Assert.True(result, "No rows found.");

            Assert.Equal("https://httpbin.org/get", reader["url"].ToString());

            result = await reader.ReadAsync();
            Assert.False(result, "More rows found.");

        }

        [Fact]
        public async Task WebService_ReadRows()
        {
            var connection = new ConnectionRestful()
            {
                Server = "https://jsonplaceholder.typicode.com",
                DefaultDatabase = ""
            };

            var restFunction = new WebService()
            {
                Name = "users",
                RestfulUri = "users",
            };

            var table = await connection.GetSourceTableInfo(restFunction, CancellationToken.None);
            Assert.True(table.Columns.GetOrdinal("id") >= 0);
            Assert.True(table.Columns.GetOrdinal("name") >= 0);
            Assert.True(table.Columns.GetOrdinal("username") >= 0);
            Assert.True(table.Columns.GetOrdinal("email") >= 0);
            Assert.True(table.Columns.GetOrdinal("address") >= 0);

            var reader = connection.GetTransformReader(table);
            var openResult = await reader.Open(0, null, CancellationToken.None);
            Assert.True(openResult, "Reader open failed");

            var id = 0;
            while(await reader.ReadAsync())
            {
                id++;
                Assert.Equal(id, reader["id"]);
            }

            Assert.Equal(10, id);
       
        }

        [Fact]
        public async Task WebService_Basic_Authenticated()
        {
            var connection = new ConnectionRestful()
            {
                Server = "https://httpbin.org",
                DefaultDatabase = "",
                Username = "user",
                Password = "passwd"
            };

            var restFunction = new WebService()
            {
                Name = "basic-auth",
                RestfulUri = "basic-auth/{user1}/{passwd1}",
            };

            restFunction.AddInputParameter("user1", "user");
            restFunction.AddInputParameter("passwd1", "passwd");
            
            var table = await connection.GetSourceTableInfo(restFunction, CancellationToken.None);
            Assert.True(table.Columns.GetOrdinal("authenticated") >= 0);
            Assert.True(table.Columns.GetOrdinal("user") >= 0);

            var reader = connection.GetTransformReader(table);
            var openResult = await reader.Open(0, null, CancellationToken.None);
            Assert.True(openResult, "Reader open failed");

            var result = await reader.ReadAsync();
            Assert.True(result, "No rows found.");

            Assert.Equal("user", reader["user"].ToString());

            result = await reader.ReadAsync();
            Assert.False(result, "More rows found.");

        }

        // TBD get this authentication working.
        //[Fact]
        //public async Task WebService_MD5_Authenticated()
        //{
        //    var connection = new ConnectionRestful()
        //    {
        //        Server = "https://httpbin.org",
        //        DefaultDatabase = "",
        //        Username = "user",
        //        Password = "passwd"
        //    };

        //    var restFunction = new RestFunction()
        //    {
        //        Name = "digest-auth",
        //        RestfulUri = "digest-auth/auth/{user1}/{passwd1}",
        //    };

        //    restFunction.AddInputParameter("user1", "user");
        //    restFunction.AddInputParameter("passwd1", "passwd");
        //    //restFunction.AddInputParameter("method", "MD5");

        //    var sourceTableResult = await connection.GetSourceTableInfo(restFunction, CancellationToken.None);
        //    Assert.True(sourceTableResult.Success, "GetSourceTableInfo failed: " + sourceTableResult.Message);

        //    var table = sourceTableResult.Value;
        //    Assert.True(table.Columns.GetOrdinal("authenticated") >= 0);
        //    Assert.True(table.Columns.GetOrdinal("user") >= 0);

        //    var reader = connection.GetTransformReader(table);
        //    var openResult = await reader.Open(0, null, CancellationToken.None);
        //    Assert.True(openResult.Success, "Reader open failed: " + openResult.Message);

        //    var result = await reader.ReadAsync();
        //    Assert.True(result, "No rows found.");

        //    Assert.Equal("user", reader["user"].ToString());

        //    result = await reader.ReadAsync();
        //    Assert.False(result, "More rows found.");

        //}
    }
}
