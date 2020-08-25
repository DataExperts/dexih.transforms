using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
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
            // source.TableAlias = "source";
            join.TableAlias = "sorted_join";
            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, join, mappings, joinStrategy, EDuplicateStrategy.All, EJoinNotFoundStrategy.NullJoin, null, null);

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
        
        public async Task JoinTwoTablesDatabase(Connection connection, EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var database = $"Test-{Guid.NewGuid().ToString().Substring(0,8)}";
            await connection.CreateDatabase(database);

            var parent = await GetDbReader(connection, Helpers.CreateParentTableData());
            var child = await GetDbReader(connection, Helpers.CreateChildTableData());
            var grandChild = await GetDbReader(connection, Helpers.CreateGrandChildTableData());
            
            var mappings = new Mappings {new MapJoin(new TableColumn("child_id"), new TableColumn("child_id"))};
            var transformJoin = new TransformJoin(grandChild, child, mappings, EJoinStrategy.Database, EDuplicateStrategy.All, EJoinNotFoundStrategy.NullJoin, null, null);

            var mappings2 = new Mappings {new MapJoin(new TableColumn("parent_id"), new TableColumn("parent_id"))};
            var transformJoin2 = new TransformJoin(transformJoin, parent, mappings2, joinStrategy, EDuplicateStrategy.All, EJoinNotFoundStrategy.NullJoin, null, null);

            Assert.Equal(8, transformJoin2.FieldCount);

            await transformJoin2.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin2.JoinAlgorithm == usedJoinStrategy);

            var pos = 0;
            var parentName = new TableColumn("name") { ReferenceTable = "parent"};

            await transformJoin2.ReadAsync();
            Assert.Equal($"parent 0", transformJoin2[parentName]);

            await transformJoin2.ReadAsync();
            Assert.Equal($"parent 0", transformJoin2[parentName]);
            
            await transformJoin2.ReadAsync();
            Assert.Equal($"parent 2", transformJoin2[parentName]);
            
            await transformJoin2.ReadAsync();
            Assert.Equal($"parent 3", transformJoin2[parentName]);
            
            Assert.False(await transformJoin2.ReadAsync());
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
            var transformJoin = new TransformJoin(source, join, mappings, joinStrategy, EDuplicateStrategy.All, EJoinNotFoundStrategy.Abend, null, null);
            Assert.Equal(10, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == usedJoinStrategy);
            await Assert.ThrowsAsync<TransformException>(async () => { while (await transformJoin.ReadAsync()) ; });
        }
        
        public async Task JoinAndGroupDatabase(Connection connection, EJoinStrategy joinStrategy, EJoinStrategy usedJoinStrategy)
        {
            var database = $"Test-{Guid.NewGuid().ToString().Substring(0,8)}";
            await connection.CreateDatabase(database);

            var parent = await GetDbReader(connection, Helpers.CreateParentTableData());
            var child = await GetDbReader(connection, Helpers.CreateChildTableData());
            
            var mappings = new Mappings {new MapJoin(new TableColumn("parent_id"), new TableColumn("parent_id"))};
            var transformJoin = new TransformJoin(child, parent, mappings, joinStrategy, EDuplicateStrategy.All, EJoinNotFoundStrategy.NullJoin, null, "child");

            var parentName = new TableColumn("name") {ReferenceTable = "parent"};
            
            var groupMappings = new Mappings(false)
            {
                new MapGroup(parentName),
                new MapAggregate(new TableColumn("child_id"), new TableColumn("child_count", ETypeCode.Int32), EAggregate.Count)
            };
            var group = new TransformGroup(transformJoin, groupMappings);

            await group.Open(1, null, CancellationToken.None);

            Assert.Equal(usedJoinStrategy, transformJoin.JoinAlgorithm);
            Assert.Equal(2, group.FieldCount);

            await group.ReadAsync();
            Assert.Equal($"parent 0", group[parentName]);
            Assert.Equal(2, group["child_count"]);

            await group.ReadAsync();
            Assert.Equal($"parent 2", group[parentName]);
            Assert.Equal(1, group["child_count"]);
            
            await group.ReadAsync();
            Assert.Equal($"parent 3", group[parentName]);
            Assert.Equal(1, group["child_count"]);
        }
    }
}