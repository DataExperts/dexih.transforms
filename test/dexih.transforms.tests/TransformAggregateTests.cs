using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformAggregateTests
    {
          [Fact]
        public async Task Group_ParentChild_Flatten()
        {
            var table = new Table("parent-child", 0, 
                new TableColumn("child", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("parent", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey)
            );
            
            table.AddRow("EMP1", "MGR1");
            table.AddRow("EMP2", "MGR1");
            table.AddRow("EMP3", "MGR2");
            table.AddRow("MGR2", "MGR1");
            table.AddRow("EMP4", "MGR2");
            table.AddRow("EMP5", "EMP4");
            table.AddRow("EMP6", "EMP5");
                
            var source = new ReaderMemory(table, null);
            source.Reset();
            
            var expected = new[]
            {
                new object[] {"MGR1", 0, "MGR1", null, null, null, null},
                new object[] {"EMP1", 1, "MGR1", "EMP1", null, null, null},
                new object[] {"EMP2", 1, "MGR1", "EMP2", null, null, null},
                new object[] {"EMP3", 2, "MGR1", "MGR2", "EMP3", null, null},
                new object[] {"MGR2", 1, "MGR1", "MGR2", null, null, null},
                new object[] {"EMP4", 2, "MGR1", "MGR2", "EMP4", null, null},
                new object[] {"EMP5", 3, "MGR1", "MGR2", "EMP4", "EMP5", null},
                new object[] {"EMP6", 4, "MGR1", "MGR2", "EMP4", "EMP5", "EMP6"},
            };

            var func = Functions.GetFunction(typeof(HierarchyFunctions).FullName, nameof(HierarchyFunctions.FlattenParentChild), Helpers.BuiltInAssembly).GetTransformFunction(typeof(string));
            
            var parameters = new Parameters
            {
                Inputs = new Parameter []
                {
                    new ParameterColumn("child", table["child"]),
                    new ParameterColumn("parent", table["parent"]),
                },
                ResultInputs = new Parameter []
                {
                     new ParameterValue("maxDepth", DataType.ETypeCode.Int32, 4 ), 
                },
                ResultOutputs = new Parameter []
                {
                    new ParameterOutputColumn("leafValue", DataType.ETypeCode.String),
                    new ParameterOutputColumn("depth", DataType.ETypeCode.Int32),
                    new ParameterArray("levels", DataType.ETypeCode.String, 1, new List<Parameter>
                    {
                        new ParameterOutputColumn("level1", DataType.ETypeCode.String),
                        new ParameterOutputColumn("level2", DataType.ETypeCode.String),
                        new ParameterOutputColumn("level3", DataType.ETypeCode.String),
                        new ParameterOutputColumn("level4", DataType.ETypeCode.String),
                        new ParameterOutputColumn("level5", DataType.ETypeCode.String),
                    }), 
                }
            };

            //            var aggregates = new List<TransformFunction>();
            
            var mappings = new Mappings(false);
            mappings.Add(new MapFunction(func, parameters, EFunctionCaching.NoCache));
            var transformAggregate = new TransformAggregate(source, mappings);
            await transformAggregate.Open(0, null, CancellationToken.None);
            
            Assert.Equal(7, transformAggregate.FieldCount);

            var counter = 0;
            while (await transformAggregate.ReadAsync())
            {
                Assert.Equal(expected[counter++], transformAggregate.CurrentRow);
            }
            Assert.Equal(expected.Length, counter);
        }
        
        [Fact]
        public async Task Group_PercentTotal_Rank()
        {
            var table = new Table("parent-child", 0, 
                new TableColumn("group", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("value", DataType.ETypeCode.Double, TableColumn.EDeltaType.NaturalKey)
            );
            
            table.AddRow("GROUP1", 1);
            table.AddRow("GROUP1", 2);
            table.AddRow("GROUP1", 7);
            table.AddRow("GROUP2", 6);
            table.AddRow("GROUP2", 2);
            table.AddRow("GROUP2", 2);
                
            var source = new ReaderMemory(table, null);
            source.Reset();
            
            var expected = new[]
            {
                new object[] {"GROUP1", 10d, 1},
                new object[] {"GROUP1", 20d, 2},
                new object[] {"GROUP1", 70d, 3},
                new object[] {"GROUP2", 60d, 3},
                new object[] {"GROUP2", 20d, 1},
                new object[] {"GROUP2", 20d, 1},
            };

            var percentTotal = new MapFunction(
                Functions.GetFunction(typeof(AggregateFunctions<>).FullName, nameof(AggregateFunctions<double>.PercentTotal), Helpers.BuiltInAssembly).GetTransformFunction(typeof(double)),
                new Parameters
                {
                    Inputs = new Parameter []
                    {
                        new ParameterColumn("value", table["value"]),
                    },
                    ResultInputs = new Parameter []
                    {
                        new ParameterValue("percentFormat", DataType.ETypeCode.Enum, AggregateFunctions<double>.EPercentFormat.AsPercent), 
                    },
                    ResultReturnParameters = new Parameter[]
                    {
                        new ParameterOutputColumn("percent", DataType.ETypeCode.Double)
                        
                    },
                }, EFunctionCaching.NoCache
            );

            var rank = new MapFunction(
                Functions.GetFunction(typeof(AggregateFunctions<>).FullName, nameof(AggregateFunctions<string>.Rank), Helpers.BuiltInAssembly).GetTransformFunction(typeof(double)),
                new Parameters
                {
                    Inputs = new Parameter []
                    {
                        new ParameterArray("values", DataType.ETypeCode.Double, 1, new List<Parameter>
                        { 
                            new ParameterColumn("value", table["value"])
                        })
                    },
                    ResultInputs = new Parameter []
                    {
                        new ParameterValue("direction", DataType.ETypeCode.Enum, Sort.EDirection.Ascending), 
                    },
                    ResultReturnParameters = new Parameter[]
                    {
                        new ParameterOutputColumn("rank", DataType.ETypeCode.Int32)
                    },
                }, EFunctionCaching.NoCache   
            );

            var mappings = new Mappings(false)
            {
                new MapGroup(table.Columns[0]),
                percentTotal, 
                rank, 
            };

            var transformAggregate = new TransformAggregate(source, mappings);
            await transformAggregate.Open(0, null, CancellationToken.None);

            Assert.Equal(3, transformAggregate.FieldCount);

            var counter = 0;
            while (await transformAggregate.ReadAsync())
            {
                Assert.Equal(expected[counter++], transformAggregate.CurrentRow);
            }
            Assert.Equal(expected.Length, counter);
        }
    }
}