using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions.Query;
using Xunit;
using DataType = Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformDeltaParentChild
    {
        private async Task InitialLoadTargetTables(Connection targetConnection, TransformDelta.EUpdateStrategy updateStrategy)
        {
            var parentTable = Helpers.CreateParentTable();
            parentTable.AddAuditColumns("parent_key");

            var childTable = Helpers.CreateChildTable();
            childTable.AddAuditColumns("child_key");
            childTable.AddColumn("parent_key", DataType.ETypeCode.Int64);

            var grandChildTable = Helpers.CreateGrandChildTable();
            grandChildTable.AddAuditColumns("grandChild_key");
            grandChildTable.AddColumn("child_key", DataType.ETypeCode.Int64);

            var parentTarget = new TransformWriterTarget(targetConnection, parentTable);
            var childTarget = new TransformWriterTarget(targetConnection, childTable);
            var grandChildTarget = new TransformWriterTarget(targetConnection, grandChildTable);
            parentTarget.Add(childTarget, new[] {"children"});
            parentTarget.Add(grandChildTarget, new[] {"children", "grandChildren"});

            // creates a three level hierarchy parent/child/grandchild.
            var reader = Helpers.CreateParentChildReader();

            await parentTarget.WriteRecordsAsync(reader, updateStrategy);
        }

        private async Task ValidateTargetTables(Connection targetConnection)
        {
            var parentReader = await targetConnection.GetTransformReader("parent");
            var query = new SelectQuery()
            {
                Filters = new Filters("IsCurrent", true),
                Sorts = new Sorts("parent_id")
            };
            parentReader = new TransformQuery(parentReader, query);
            await parentReader.Open();
            
            var parentKeys = new Dictionary<long, int>();

            Assert.True(await parentReader.ReadAsync());
            parentKeys.Add((long) parentReader["parent_key"], (int)parentReader["parent_id"]);
//            Assert.Equal(1L, parentReader["parent_key"]);
            Assert.Equal(0, parentReader["parent_id"]);
            Assert.Equal("parent 0", parentReader["name"]);

            Assert.True(await parentReader.ReadAsync());
            parentKeys.Add((long) parentReader["parent_key"], (int)parentReader["parent_id"]);
            //Assert.Equal(2L, parentReader["parent_key"]);
            Assert.Equal(1, parentReader["parent_id"]);
            Assert.Equal("parent 1", parentReader["name"]);

            Assert.True(await parentReader.ReadAsync());
            parentKeys.Add((long) parentReader["parent_key"], (int)parentReader["parent_id"]);
//            Assert.Equal(3L, parentReader["parent_key"]);
            Assert.Equal(2, parentReader["parent_id"]);
            Assert.Equal("parent 2", parentReader["name"]);

            Assert.True(await parentReader.ReadAsync());
            parentKeys.Add((long) parentReader["parent_key"], (int)parentReader["parent_id"]);
//            Assert.Equal(4L, parentReader["parent_key"]);
            Assert.Equal(3, parentReader["parent_id"]);
            Assert.Equal("parent 3", parentReader["name"]);

            Assert.False(await parentReader.ReadAsync());

            var childReader = await targetConnection.GetTransformReader("child");
            var childQuery = new SelectQuery()
            {
                Filters = new Filters("IsCurrent", true),
                Sorts = new Sorts("child_id")
            };
            childReader = new TransformQuery(childReader, childQuery);
            await childReader.Open();

            var childKeys = new Dictionary<long, int>();
            
            Assert.True(await childReader.ReadAsync());
            childKeys.Add((long) childReader["child_key"], (int)childReader["child_id"]);
            Assert.Equal(0, parentKeys[(long)childReader["parent_key"]]);
            Assert.Equal(0, childReader["parent_id"]);
            Assert.Equal(0, childReader["child_id"]);
            Assert.Equal("child 00", childReader["name"]);

            Assert.True(await childReader.ReadAsync());
            childKeys.Add((long) childReader["child_key"], (int)childReader["child_id"]);
            Assert.Equal(0, parentKeys[(long)childReader["parent_key"]]);
            Assert.Equal(0, childReader["parent_id"]);
            Assert.Equal(1, childReader["child_id"]);
            Assert.Equal("child 01", childReader["name"]);

            Assert.True(await childReader.ReadAsync());
            childKeys.Add((long) childReader["child_key"], (int)childReader["child_id"]);
            Assert.Equal(2, parentKeys[(long)childReader["parent_key"]]);
            Assert.Equal(2, childReader["parent_id"]);
            Assert.Equal(20, childReader["child_id"]);
            Assert.Equal("child 20", childReader["name"]);

            Assert.True(await childReader.ReadAsync());
            childKeys.Add((long) childReader["child_key"], (int)childReader["child_id"]);
            Assert.Equal(3, parentKeys[(long)childReader["parent_key"]]);
            Assert.Equal(3, childReader["parent_id"]);
            Assert.Equal(30, childReader["child_id"]);
            Assert.Equal("child 30", childReader["name"]);

            Assert.False(await childReader.ReadAsync());

            var grandChildReader = await targetConnection.GetTransformReader("grandChild");
            var grandChildQuery = new SelectQuery()
            {
                Filters = new Filters("IsCurrent", true),
                Sorts = new Sorts("grandChild_id")
            };
            grandChildReader = new TransformQuery(grandChildReader, grandChildQuery);
            await grandChildReader.Open();

            var grandChildKeys = new Dictionary<long, int>();

            Assert.True(await grandChildReader.ReadAsync());
            Assert.Equal(0, childKeys[(long)grandChildReader["child_key"]]);
            Assert.Equal(0, grandChildReader["child_id"]);
            grandChildKeys.Add((long) grandChildReader["grandChild_key"], (int)grandChildReader["grandChild_id"]);
            Assert.Equal(0, grandChildReader["grandChild_id"]);
            Assert.Equal("grandChild 000", grandChildReader["name"]);

            Assert.True(await grandChildReader.ReadAsync());
            Assert.Equal(0, childKeys[(long)grandChildReader["child_key"]]);
            Assert.Equal(0, grandChildReader["child_id"]);
            grandChildKeys.Add((long) grandChildReader["grandChild_key"], (int)grandChildReader["grandChild_id"]);
            Assert.Equal(1, grandChildReader["grandChild_id"]);
            Assert.Equal("grandChild 001", grandChildReader["name"]);

            Assert.True(await grandChildReader.ReadAsync());
            Assert.Equal(20, childKeys[(long)grandChildReader["child_key"]]);
            Assert.Equal(20, grandChildReader["child_id"]);
            grandChildKeys.Add((long) grandChildReader["grandChild_key"], (int)grandChildReader["grandChild_id"]);
            Assert.Equal(200, grandChildReader["grandChild_id"]);
            Assert.Equal("grandChild 200", grandChildReader["name"]);

            Assert.True(await grandChildReader.ReadAsync());
            Assert.Equal(30, childKeys[(long)grandChildReader["child_key"]]);
            Assert.Equal(30, grandChildReader["child_id"]);
            grandChildKeys.Add((long) grandChildReader["grandChild_key"], (int)grandChildReader["grandChild_id"]);
            Assert.Equal(300, grandChildReader["grandChild_id"]);
            Assert.Equal("grandChild 300", grandChildReader["name"]);
        }
        
        [Theory]
        [InlineData(TransformDelta.EUpdateStrategy.Append)]
        [InlineData(TransformDelta.EUpdateStrategy.Reload)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdate)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdateDelete)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdatePreserve)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve)]
        public async Task Load_Empty_targets(TransformDelta.EUpdateStrategy updateStrategy)
        {
            var targetConnection = new ConnectionMemory();
            await InitialLoadTargetTables(targetConnection, updateStrategy);
            await ValidateTargetTables(targetConnection);
        }

        [Theory]
        [InlineData(TransformDelta.EUpdateStrategy.Reload)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdate)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdateDelete)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdatePreserve)]
        [InlineData(TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve)]
        public async Task Modify_Parent_Record(TransformDelta.EUpdateStrategy updateStrategy)
        {
            var targetConnection = new ConnectionMemory();
            await InitialLoadTargetTables(targetConnection, updateStrategy);
            
            // modify a record in the target table, and then reload.
            var query = new UpdateQuery("name", "update", "parent_key", 1);
            await targetConnection.ExecuteUpdate("parent", query);

            await InitialLoadTargetTables(targetConnection, updateStrategy);
            await ValidateTargetTables(targetConnection);
        }
    }
}