using System;
using dexih.functions;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.github;
using dexih.transforms;
using dexih.transforms.File;
using Dexih.Utils.DataType;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.github.tests
{
    /// <summary>
    /// web services test.  These tests use the online web services hosted at https://httpbin.org
    /// and https://jsonplaceholder.typicode.com
    /// </summary>
    public class ConnectionGitHubTests
    {
        private readonly ITestOutputHelper _output;

        public ConnectionGitHubTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        public ConnectionFlatFile GetConnection()
        {
            var connection = new ConnectionGitHubFlatFile
            {
                Server = "DataExperts/ConnectionTest",
                DefaultDatabase = "",
            };

            return connection;
        }

        [Fact]
        public async Task GetFiles()
        {
            var connection = GetConnection();
            
            var tables = await connection.GetFiles(new FlatFile(), EFlatFilePath.None, CancellationToken.None);

            Assert.Single(tables);
            Assert.Equal("sample_data.csv", tables[0].FileName);
            Assert.Equal(new DateTime(2020, 3, 23), tables[0].LastModified.Date);
        }

        [Fact]
        public async Task GetFile()
        {
            var connection = GetConnection();

            var flatFile = new FlatFile()
            {
                Name = "sample_data.csv",
                FileConfiguration = new FileConfiguration(),
                FormatType = ETypeCode.Text,
                Columns = new TableColumns()
                {
                    new TableColumn("row_number", ETypeCode.Int32),
                    new TableColumn("value", ETypeCode.String)
                }
            };

            var reader = connection.GetTransformReader(flatFile, true);
            Assert.True(await reader.Open());
            
            Assert.Equal(2, reader.FieldCount);
            Assert.Equal("row_number", reader.GetName(0));
            Assert.Equal("value", reader.GetName(1));

            var count = 0;
            while (await reader.ReadAsync()) count++;
            
            Assert.Equal(10, count);

        }
        
    }
}
