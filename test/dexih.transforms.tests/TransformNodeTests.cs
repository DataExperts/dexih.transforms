using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Parameter;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformNodeTests
    {
        [Fact]
        public async Task NodeParentChildTest()
        {
            var source = Helpers.CreateParentChildReader();

            Assert.Equal("parent_id", source.CacheTable.Columns[1].Name);
            Assert.Equal("name", source.CacheTable.Columns[2].Name);

            var children = source.CacheTable.Columns["children"].ChildColumns;
            Assert.Equal("parent_id", children[1].Name);
            Assert.Equal("child_id", children[2].Name);
            Assert.Equal("name", children[3].Name);

            var grandChildren = children[0].ChildColumns;
            Assert.Equal("child_id", grandChildren[0].Name);
            Assert.Equal("grandChild_id", grandChildren[1].Name);
            Assert.Equal("name", grandChildren[2].Name);

            Assert.True(await source.Open());
            
            Assert.True(await source.ReadAsync());
            Assert.Equal(0, source["parent_id"]);
            Assert.Equal("parent 0", source["name"]);

            var childTransform = (Transform) source["children"];
            Assert.True(await childTransform.ReadAsync());
            Assert.Equal(0, childTransform["parent_id"]);
            Assert.Equal(0, childTransform["child_id"]);
            Assert.Equal("child 00", childTransform["name"]);

            var grandChildTransform = (Transform) childTransform["grandChildren"];
            Assert.True(await grandChildTransform.ReadAsync());
            Assert.Equal(0, grandChildTransform["child_id"]);
            Assert.Equal(0, grandChildTransform["grandChild_id"]);
            Assert.Equal("grandChild 000", grandChildTransform["name"]);

            Assert.True(await grandChildTransform.ReadAsync());
            Assert.Equal(0, grandChildTransform["child_id"]);
            Assert.Equal(1, grandChildTransform["grandChild_id"]);
            Assert.Equal("grandChild 001", grandChildTransform["name"]);
            
            Assert.False(await grandChildTransform.ReadAsync());

            Assert.True(await childTransform.ReadAsync());
            Assert.Equal(0, childTransform["parent_id"]);
            Assert.Equal(1, childTransform["child_id"]);
            Assert.Equal("child 01", childTransform["name"]);

            grandChildTransform = (Transform) childTransform["grandChildren"];
            Assert.False(await grandChildTransform.ReadAsync());
            
            Assert.False(await childTransform.ReadAsync());
            
            Assert.True(await source.ReadAsync());
            Assert.Equal(1, source["parent_id"]);
            Assert.Equal("parent 1", source["name"]);
            
            childTransform = (Transform) source["children"];
            Assert.False(await childTransform.ReadAsync());
            
            Assert.True(await source.ReadAsync());
            Assert.Equal(2, source["parent_id"]);
            Assert.Equal("parent 2", source["name"]);
            
            childTransform = (Transform) source["children"];
            Assert.True(await childTransform.ReadAsync());
            Assert.Equal(2, childTransform["parent_id"]);
            Assert.Equal(20, childTransform["child_id"]);
            Assert.Equal("child 20", childTransform["name"]);
        }
        
        [Fact]
        public async Task UnGroupChapterTest()
        {
            var source = Helpers.CreateParentChildReader();
            
            var mapping = new MapUnGroup(source.CacheTable["children"]);
            var mappings = new Mappings(true) {mapping};
            var flatten = new TransformRows(source, mappings) {};

            await flatten.Open(0, null, CancellationToken.None);
            
            Assert.Equal("children.parent_id", flatten.CacheTable.Columns[1].Name);
            Assert.Equal("children.child_id", flatten.CacheTable.Columns[2].Name);
            Assert.Equal("children.name", flatten.CacheTable.Columns[3].Name);
            Assert.Equal("parent_id", flatten.CacheTable.Columns[5].Name);
            Assert.Equal("name", flatten.CacheTable.Columns[6].Name);

            await flatten.ReadAsync();
            Assert.Equal(0, flatten["parent_id"]);
            Assert.Equal("parent 0", flatten["name"]);
            Assert.Equal(0, flatten["children.parent_id"]);
            Assert.Equal(0, flatten["children.child_id"]);
            Assert.Equal("child 00", flatten["children.name"]);
            
            await flatten.ReadAsync();
            Assert.Equal(0, flatten["parent_id"]);
            Assert.Equal("parent 0", flatten["name"]);
            Assert.Equal(0, flatten["children.parent_id"]);
            Assert.Equal(1, flatten["children.child_id"]);
            Assert.Equal("child 01", flatten["children.name"]);

            await flatten.ReadAsync();
            Assert.Equal(1, flatten["parent_id"]);
            Assert.Equal("parent 1", flatten["name"]);
            Assert.Equal(null, flatten["children.parent_id"]);
            Assert.Equal(null, flatten["children.child_id"]);
            Assert.Equal(null, flatten["children.name"]);

            await flatten.ReadAsync();
            Assert.Equal(2, flatten["parent_id"]);
            Assert.Equal("parent 2", flatten["name"]);
            Assert.Equal(2, flatten["children.parent_id"]);
            Assert.Equal(20, flatten["children.child_id"]);
            Assert.Equal("child 20", flatten["children.name"]);

            await flatten.ReadAsync();
            Assert.Equal(3, flatten["parent_id"]);
            Assert.Equal("parent 3", flatten["name"]);
            Assert.Equal(3, flatten["children.parent_id"]);
            Assert.Equal(30, flatten["children.child_id"]);
            Assert.Equal("child 30", flatten["children.name"]);

            Assert.False(await flatten.ReadAsync());

        }

        [Fact]
        public async Task NodeMappingTest()
        {
            var sourceReader = Helpers.CreateParentChildReader();
            
            var nodeMappings = new Mappings();
            var function = new TransformFunction(new Func<string, string, string>((parent, child) => parent + "-" + child), typeof(string), null, null);
            var parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterColumn("parent.name", DataType.ETypeCode.String),
                    new ParameterColumn("name", DataType.ETypeCode.String),
                },
                ReturnParameters =  new List<Parameter> { new ParameterOutputColumn("parent_child", DataType.ETypeCode.String)}
            };   
            nodeMappings.Add(new MapFunction(function, parameters, MapFunction.EFunctionCaching.NoCache));

            var mapping = new TransformMapping();
            var mappingTransform = mapping.CreateNodeMapping(sourceReader, null, nodeMappings,
                new TableColumn[] {new TableColumn("children")});
            
//            var childrenTable = sourceReader.CacheTable["children"];
//            
//            var mapNode = new MapNode(childrenTable, sourceReader.CacheTable);
//            var nodeTransform = mapNode.Transform;
//            var nodeMapping = new TransformMapping(nodeTransform, nodeMappings);
//            mapNode.OutputTransform = nodeMapping;
//
//            var mappings = new Mappings {mapNode};
//
//            var mapping = new TransformMapping(sourceReader, mappings);

            await mappingTransform.Open(0, null, CancellationToken.None);

            Assert.True(await mappingTransform.ReadAsync());
            Assert.Equal(0, mappingTransform["parent_id"]);
            Assert.Equal("parent 0", mappingTransform["name"]);

            var transform = (Transform) mappingTransform["children"];
            Assert.True(await transform.ReadAsync());
            Assert.Equal("parent 0-child 00", transform["parent_child"]);
            Assert.True(await transform.ReadAsync());
            Assert.Equal("parent 0-child 01", transform["parent_child"]);
            Assert.False(await transform.ReadAsync());
            
            Assert.True(await mappingTransform.ReadAsync());
            Assert.Equal(1, mappingTransform["parent_id"]);
            Assert.Equal("parent 1", mappingTransform["name"]);
            Assert.False(await transform.ReadAsync());

            Assert.True(await mappingTransform.ReadAsync());
            Assert.Equal(2, mappingTransform["parent_id"]);
            Assert.Equal("parent 2", mappingTransform["name"]);
            Assert.True(await transform.ReadAsync());
            Assert.Equal("parent 2-child 20", transform["parent_child"]);
            Assert.False(await transform.ReadAsync());

            Assert.True(await mappingTransform.ReadAsync());
            Assert.Equal(3, mappingTransform["parent_id"]);
            Assert.Equal("parent 3", mappingTransform["name"]);
            Assert.True(await transform.ReadAsync());
            Assert.Equal("parent 3-child 30", transform["parent_child"]);
            Assert.False(await transform.ReadAsync());

            Assert.False(await mappingTransform.ReadAsync());


        }
    }
}