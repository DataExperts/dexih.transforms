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
    public class TransformChapterTests
    {
        [Fact]
        public async Task FlattenChapterTest()
        {
            var source = Helpers.CreateParentChildReader();

            var path = new [] { source.CacheTable["array"] };
            var flatten = new TransformFlattenNode(source, null, path, 1);

            await flatten.Open(0, null, CancellationToken.None);
            
            Assert.Equal("parent_id", flatten.CacheTable.Columns[0].Name);
            Assert.Equal("name", flatten.CacheTable.Columns[1].Name);
            Assert.Equal("parent_id", flatten.CacheTable.Columns[2].Name);
            Assert.Equal("child_id", flatten.CacheTable.Columns[3].Name);
            Assert.Equal("name", flatten.CacheTable.Columns[4].Name);

            await flatten.ReadAsync();
            Assert.Equal(0, flatten["parent_id"]);
            Assert.Equal("parent 0", flatten["name"]);
            Assert.Equal(0, flatten["array.parent_id"]);
            Assert.Equal(0, flatten["child_id"]);
            Assert.Equal("child 00", flatten["array.name"]);
            
            await flatten.ReadAsync();
            Assert.Equal(0, flatten["parent_id"]);
            Assert.Equal("parent 0", flatten["name"]);
            Assert.Equal(0, flatten["array.parent_id"]);
            Assert.Equal(1, flatten["child_id"]);
            Assert.Equal("child 01", flatten["array.name"]);

            await flatten.ReadAsync();
            Assert.Equal(1, flatten["parent_id"]);
            Assert.Equal("parent 1", flatten["name"]);
            Assert.Equal(null, flatten["array.parent_id"]);
            Assert.Equal(null, flatten["child_id"]);
            Assert.Equal(null, flatten["array.name"]);

            await flatten.ReadAsync();
            Assert.Equal(2, flatten["parent_id"]);
            Assert.Equal("parent 2", flatten["name"]);
            Assert.Equal(2, flatten["array.parent_id"]);
            Assert.Equal(20, flatten["child_id"]);
            Assert.Equal("child 20", flatten["array.name"]);

            await flatten.ReadAsync();
            Assert.Equal(3, flatten["parent_id"]);
            Assert.Equal("parent 3", flatten["name"]);
            Assert.Equal(3, flatten["array.parent_id"]);
            Assert.Equal(30, flatten["child_id"]);
            Assert.Equal("child 30", flatten["array.name"]);

            Assert.False(await flatten.ReadAsync());

        }

        [Fact]
        public async Task ChapterMappingTest()
        {
            var source = Helpers.CreateParentChildReader();
            
            var nodeMappings = new Mappings();
            var function = new TransformFunction(new Func<string, string, string>((parent, child) => parent + "-" + child), typeof(string), null, null);
            var parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterColumn("name", DataType.ETypeCode.String),
                    new ParameterColumn("parent.name", DataType.ETypeCode.String),
                },
                ReturnParameters =  new List<Parameter> { new ParameterOutputColumn("CustomFunction", DataType.ETypeCode.String)}
            };   
            nodeMappings.Add(new MapFunction(function, parameters, MapFunction.EFunctionCaching.NoCache));

            var mapNode = new MapNode(new TableColumn("array", DataType.ETypeCode.Node));
            var nodeTransform = mapNode.Transform;
            var nodeMapping = new TransformMapping(nodeTransform, nodeMappings);
            mapNode.OutputTransform = nodeMapping;

            var mappings = new Mappings();
            mappings.Add(mapNode);
            
            var mapping = new TransformMapping(source, mappings);

            await mapping.Open(0, null, CancellationToken.None);

            await mapping.ReadAsync();

            Assert.Equal(0, mapping[0]);



        }
    }
}