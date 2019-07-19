using dexih.functions;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms.Exceptions;
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

            var reader = await connection.GetTransformReader("get");

            var table = reader.CacheTable;
            Assert.True(table.Columns.GetOrdinal("args") >= 0);
            Assert.True(table.Columns.GetOrdinal("Host") >= 0);
            Assert.True(table.Columns.GetOrdinal("origin") >= 0);
            Assert.True(table.Columns.GetOrdinal("url") >= 0);

            var openResult = await reader.Open();
            Assert.True(openResult, "Reader open failed");
            Assert.True(await reader.ReadAsync());
            Assert.Equal("https://httpbin.org/get", reader["url"]);
            Assert.False(await reader.ReadAsync());
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
                MaxImportLevels = 3
            };

            var table = await connection.GetSourceTableInfo(restFunction, CancellationToken.None);
            Assert.True(table.Columns.GetOrdinal("id") >= 0);
            Assert.True(table.Columns.GetOrdinal("name") >= 0);
            Assert.True(table.Columns.GetOrdinal("username") >= 0);
            Assert.True(table.Columns.GetOrdinal("email") >= 0);
            Assert.True(table.Columns.GetOrdinal("street") >= 0);
            Assert.True(table.Columns.GetOrdinal("suite") >= 0);
            Assert.True(table.Columns.GetOrdinal("city") >= 0);
            Assert.True(table.Columns.GetOrdinal("zipcode") >= 0);
            Assert.True(table.Columns.GetOrdinal("lat") >= 0);
            Assert.True(table.Columns.GetOrdinal("lng") >= 0);
            Assert.True(table.Columns.GetOrdinal("phone") >= 0);
            Assert.True(table.Columns.GetOrdinal("website") >= 0);
            Assert.True(table.Columns.GetOrdinal("company.name") >= 0);
            Assert.True(table.Columns.GetOrdinal("catchPhrase") >= 0);
            Assert.True(table.Columns.GetOrdinal("bs") >= 0);

            var reader = connection.GetTransformReader(table);
            var openResult = await reader.Open();
            Assert.True(openResult, "Reader open failed");

            var id = 0;
            while(await reader.ReadAsync())
            {
                id++;
                Assert.Equal(id, reader["id"]);
            }

            Assert.Equal(10, id);
       
        }

        [Theory]
        [InlineData("https://httpbin.org",  "basic-auth/{userParam}/{passParam}")]
        [InlineData("https://httpbin.org",  "digest-auth/auth/{userParam}/{passParam}")]
        [InlineData("https://httpbin.org",  "digest-auth/auth/{userParam}/{passParam}/sha-256")]
        [InlineData("https://httpbin.org",  "digest-auth/auth/{userParam}/{passParam}/sha-512")]
        public async Task WebService_Authentication(string server, string uri)
        {
            var connection = new ConnectionRestful()
            {
                Server = server,
                DefaultDatabase = "",
                Username = "user",
                Password = "passwd"
            };

            var restFunction = new WebService()
            {
                Name = "auth",
                RestfulUri = uri,
            };

            // call with valid password
            restFunction.AddInputParameter("userParam", "user");
            restFunction.AddInputParameter("passParam", "passwd");
            var table = await connection.GetSourceTableInfo(restFunction, CancellationToken.None);

            Assert.True(table.Columns.GetOrdinal("authenticated") >= 0);
            Assert.True(table.Columns.GetOrdinal("user") >= 0);

            var reader = connection.GetTransformReader(table);
            var openResult = await reader.Open();
            Assert.True(openResult, "Reader open failed");

            var result = await reader.ReadAsync();
            Assert.True(result, "No rows found.");

            Assert.Equal("user", reader["user"].ToString());
            Assert.Equal(true, reader["authenticated"]);

            result = await reader.ReadAsync();
            Assert.False(result, "More rows found.");

            restFunction.Columns.Clear();
            
            // call with invalid password
            restFunction.AddInputParameter("userParam", "user");
            restFunction.AddInputParameter("passParam", "badpassword");
            await Assert.ThrowsAsync<ConnectionException>(async ()  => await connection.GetSourceTableInfo(restFunction, CancellationToken.None));
        }
        
        [Fact]
        public async Task WebService_Authentication_Bearer()
        {
            var connection = new ConnectionRestful()
            {
                Server = "https://httpbin.org",
                DefaultDatabase = "",
                Username = "bearer",
                Password = "user123"
            };

            var restFunction = new WebService()
            {
                Name = "bearer",
                RestfulUri = "bearer",
            };

            // call with valid password
            var table = await connection.GetSourceTableInfo(restFunction, CancellationToken.None);

            Assert.True(table.Columns.GetOrdinal("authenticated") >= 0);
            Assert.True(table.Columns.GetOrdinal("token") >= 0);

            var reader = connection.GetTransformReader(table);
            var openResult = await reader.Open();
            Assert.True(openResult, "Reader open failed");

            var result = await reader.ReadAsync();
            Assert.True(result, "No rows found.");

            Assert.Equal("user123", reader["token"].ToString());
            Assert.Equal(true, reader["authenticated"]);

            result = await reader.ReadAsync();
            Assert.False(result, "More rows found.");
        }
       
    }
}
