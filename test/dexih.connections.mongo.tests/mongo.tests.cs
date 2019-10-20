using System;
using System.Threading.Tasks;
using dexih.connections.test;
using dexih.transforms;
using Xunit;
using Xunit.Abstractions;

namespace dexih.connections.mongo.tests
{
    [Collection("SqlTest")]
    public class mongo_tests
    {
        private readonly ITestOutputHelper _output;

        public mongo_tests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public ConnectionMongo GetConnection()
        {
            return new ConnectionMongo()
            {
                Name = "Test Connection",
                Username = Convert.ToString(Configuration.AppSettings["Mongo:UserName"]),
                Password = Convert.ToString(Configuration.AppSettings["Mongo:Password"]),
                Server = Convert.ToString(Configuration.AppSettings["Mongo:ServerName"]),
            };
        }

        [Fact]
        public async Task Mongo_Basic()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new UnitTests(_output).Unit(GetConnection(), database);
        }

        [Fact]
        public async Task Mongo_Performance()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new PerformanceTests(_output).Performance(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task Mongo_TransformWriter()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new PerformanceTests(_output).PerformanceTransformWriter(GetConnection(), database, 100000);
        }

        [Fact]
        public async Task Mongo_Transform()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            await new TransformTests().Transform(GetConnection(), database);
        }

        //[Fact]
        //public void Mongo_Specific_Unit()
        //{
        //    var connection = new ConnectionMongoql();

        //    //test delimiter
        //    Assert.Equal("\"table\"", connection.AddDelimiter("table"));
        //    Assert.Equal("\"table\"", connection.AddDelimiter("\"table\""));
        //    Assert.Equal("\"table\".\"schema\"", connection.AddDelimiter("\"table\".\"schema\""));
        //}
        
        [Fact]
        public async Task Mongo_SqlReader()
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new SqlReaderTests(_output).Unit(connection, database);
        }
        
        [Theory]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.AppendUpdateDelete, false)]
        public async Task Mongo_ParentChild_Write(bool useDbAutoIncrement, TransformDelta.EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write(connection, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }

        [Theory]
        [InlineData(false, TransformDelta.EUpdateStrategy.Reload, false)]
        [InlineData(false, TransformDelta.EUpdateStrategy.AppendUpdateDelete, false)]
        public async Task Mongo_ParentChild_Write_Large(bool useDbAutoIncrement, TransformDelta.EUpdateStrategy updateStrategy, bool useTransaction)
        {
            var database = "Test-" + Guid.NewGuid().ToString();
            var connection = GetConnection();

            await new TransformWriterTargetTests(_output).ParentChild_Write_Large(connection, 1000, database, useDbAutoIncrement, updateStrategy, useTransaction);
        }
    }
}