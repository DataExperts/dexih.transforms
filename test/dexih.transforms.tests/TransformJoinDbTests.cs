using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformJoinDbTests
    {

        public async Task<Transform> GetDbReader(Connection connection, Transform reader)
        {
            var converted = new ReaderConvertDataTypes(connection, reader);
            await converted.Open();
            await connection.CreateTable(reader.CacheTable, true);
            await connection.ExecuteInsertBulk(reader.CacheTable, converted);
            var sourceReader = connection.GetTransformReader(reader.CacheTable);
            return sourceReader;
        }

        public async Task JoinDatabase(Connection connection, EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var database = $"Test-{Guid.NewGuid().ToString().Substring(0,8)}";
            await connection.CreateDatabase(database);

            var source = await GetDbReader(connection, Helpers.CreateSortedTestData());
            var join = await GetDbReader(connection, Helpers.CreateSortedJoinData());

            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, join, mappings, joinStrategy, EDuplicateStrategy.All, EJoinNotFoundStrategy.NullJoin, null, "join");

            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == usedJoinStrategy);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }
        
        /// <summary>
        /// Checks the join transform correctly raises an exception when a duplicate join key exists.
        /// </summary>
        public async Task JoinDatabaseJoinMissingException(Connection connection, EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var database = $"Test-{Guid.NewGuid().ToString().Substring(0,8)}";
            await connection.CreateDatabase(database);

            var source = await GetDbReader(connection, Helpers.CreateSortedTestData());
            var join = await GetDbReader(connection, Helpers.CreateDuplicatesJoinData());
            
            var mappings = new Mappings { new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn") {ReferenceTable = "join_duplicates"}) };
            var transformJoin = new TransformJoin(source, join, mappings, joinStrategy, EDuplicateStrategy.All, EJoinNotFoundStrategy.Abend, null, "join_duplicates");
            Assert.Equal(10, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == usedJoinStrategy);
            await Assert.ThrowsAsync<TransformException>(async () => { while (await transformJoin.ReadAsync()) ; });
        }
    }
}