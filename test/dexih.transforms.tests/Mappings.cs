using System;
using System.Collections.Generic;
using System.Threading;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.functions.tests
{
    public class MappingsTests
    {
        static FunctionVariables _functionVariables = new FunctionVariables();
        
        [Fact]
        public async void Mapping_Column()
        {
            var inputColumn = new TableColumn("input");
            var outputColumn = new TableColumn("input");
            var inputRow = new object[] {"field1"};
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            var outputTable = new Table("output");
            
            // map a value
            var mapColumn = (Mapping) new MapColumn("123", outputColumn);
            mapColumn.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            await mapColumn.ProcessInputRowAsync(_functionVariables, inputRow);
            mapColumn.MapOutputRow(outputRow);

            Assert.Equal("123", outputRow[0]);
            
            mapColumn = new MapColumn(inputColumn, outputColumn);
            mapColumn.InitializeColumns(inputTable);
            mapColumn.AddOutputColumns(outputTable);
            await mapColumn.ProcessInputRowAsync(_functionVariables, inputRow);
            mapColumn.MapOutputRow(outputRow);
            
            Assert.Equal("field1", outputRow[0]);
        }
        
        [Fact]
        public async void Mapping_Group()
        {
            var inputColumn = new TableColumn("input", ETypeCode.String);
            var outputColumn = new TableColumn("input", ETypeCode.String);
            var inputRow = new object[] {"field1"};
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            var outputTable = new Table("output");
            
            // map a value
            var mapColumn = new MapGroup("123", outputColumn);
            mapColumn.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            await mapColumn.ProcessInputRowAsync(_functionVariables, inputRow);
            mapColumn.MapOutputRow(outputRow);

            Assert.Equal("123", outputRow[0]);
            
            mapColumn = new MapGroup(inputColumn, outputColumn);
            mapColumn.InitializeColumns(inputTable);
            mapColumn.AddOutputColumns(outputTable);
            await mapColumn.ProcessInputRowAsync(_functionVariables, inputRow);
            mapColumn.MapOutputRow(outputRow);
            
            Assert.Equal("field1", outputRow[0]);
        }
        
        [Fact]
        public async void Mapping_Filter()
        {
            var inputColumn1 = new TableColumn("input1", ETypeCode.String);
            var inputColumn2 = new TableColumn("input2", ETypeCode.String);
            var inputColumn3 = new TableColumn("input3", ETypeCode.String);
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn1);
            inputTable.Columns.Add(inputColumn2);
            inputTable.Columns.Add(inputColumn3);

            var inputRow = new object[] {"val1", "val1", "not val1"};
            
            // test filter
            var mapFilter = new MapFilter(inputColumn1, "val1");
            mapFilter.InitializeColumns(inputTable);
            Assert.True(await mapFilter.ProcessInputRowAsync(_functionVariables, inputRow));
            
            mapFilter = new MapFilter(inputColumn1, "not val1");
            mapFilter.InitializeColumns(inputTable);
            Assert.False(await mapFilter.ProcessInputRowAsync(_functionVariables, inputRow));

            mapFilter = new MapFilter(inputColumn1, inputColumn2);
            mapFilter.InitializeColumns(inputTable);
            Assert.True(await mapFilter.ProcessInputRowAsync(_functionVariables, inputRow));

            mapFilter = new MapFilter(inputColumn1, inputColumn3);
            mapFilter.InitializeColumns(inputTable);
            Assert.False(await mapFilter.ProcessInputRowAsync(_functionVariables, inputRow));

        }
        
        [Fact]
        public async void Mapping_Join()
        {
            var inputColumn1 = new TableColumn("input1", ETypeCode.String);
            var inputColumn2 = new TableColumn("input2", ETypeCode.String);
            var inputColumn3 = new TableColumn("input3", ETypeCode.String);
            var inputTable = new Table("input");
            var joinTable = new Table("join");
            inputTable.Columns.Add(inputColumn1);
            joinTable.Columns.Add(inputColumn2);
            joinTable.Columns.Add(inputColumn3);

            var inputRow = new object[] {"val1"};
            var joinRow = new object[] {"val1", "not val1"};
            
//            // test filter
//            var mapJoin = new MapJoin(inputColumn1, "val1");
//            mapJoin.InitializeInputOrdinals(inputTable, joinTable);
//            Assert.True(mapJoin.ProcessInputRow(inputRow, joinRow));
//            
//            mapJoin = new MapJoin(inputColumn1, "not val1");
//            mapJoin.InitializeInputOrdinals(inputTable, joinTable);
//            Assert.False(mapJoin.ProcessInputRow(inputRow, joinRow));

            var mapJoin = new MapJoin(inputColumn1, inputColumn2);
            mapJoin.InitializeColumns(inputTable, joinTable);
            Assert.True( await mapJoin.ProcessInputRowAsync(_functionVariables, inputRow, joinRow));

            mapJoin = new MapJoin(inputColumn1, inputColumn3);
            mapJoin.InitializeColumns(inputTable, joinTable);
            Assert.False( await mapJoin.ProcessInputRowAsync(_functionVariables, inputRow, joinRow));

        }
        
        [Fact]
        public async void Mapping_Aggregate()
        {
            var inputColumn = new TableColumn("input", ETypeCode.Int32);
            var outputColumn = new TableColumn("output", ETypeCode.Int32);
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            var outputTable = new Table("output");
            
            // map a value
            var mapAggregate = new MapAggregate(inputColumn, outputColumn, EAggregate.Sum);
            mapAggregate.AddOutputColumns(outputTable);

            //run twice to ensure reset works.
            for (var i = 0; i < 2; i++)
            {
                await mapAggregate.ProcessInputRowAsync(_functionVariables, new object[] {1});
                await mapAggregate.ProcessInputRowAsync(_functionVariables, new object[] {2});
                await mapAggregate.ProcessInputRowAsync(_functionVariables, new object[] {3});
                var outputRow = new object[1];
                await mapAggregate.ProcessResultRowAsync(new FunctionVariables(), outputRow, EFunctionType.Aggregate, CancellationToken.None);
                Assert.Equal(6, outputRow[0]);
                mapAggregate.Reset(EFunctionType.Aggregate);
            }
        }

        [Fact]
        public async void Mapping_Function()
        {
            var inputColumn1 = new TableColumn("input1", ETypeCode.String);
            var inputColumn2 = new TableColumn("input2", ETypeCode.String);
            var outputColumn = new TableColumn("output");
            var inputRow = new object[] {"aaa", "bbb"};
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn1);
            inputTable.Columns.Add(inputColumn2);
            
            var outputTable = new Table("output");

            string Concat(string[] a) => string.Concat(a);
            var transformFunction = new TransformFunction((Func<string[], string>) Concat);
            
//            var function = Functions.GetFunction(typeof(SampleFunction), typeof(SampleFunction).GetMethod(nameof(SampleFunction.Concat)));
//            var transformFunction = function.GetTransformFunction(typeof(string));
            var parameters = new Parameters
            {
                Inputs = new List<Parameter.Parameter>
                {
                    new ParameterArray("input", ETypeCode.String, 1,
                        new List<Parameter.Parameter>
                        {
                            new ParameterColumn("values", inputColumn1),
                            new ParameterColumn("values", inputColumn2),
                        })
                },
                ReturnParameters = new List<Parameter.Parameter> { new ParameterOutputColumn("return", outputColumn) }
            };

            // map a value
            var mapFunction = new MapFunction(transformFunction, parameters, EFunctionCaching.EnableCache);
            mapFunction.InitializeColumns(inputTable);
            mapFunction.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            await mapFunction.ProcessInputRowAsync(_functionVariables, inputRow);
            mapFunction.MapOutputRow(outputRow);

            Assert.Equal("aaabbb", outputRow[0]);
        }
        
        [Fact]
        public async void Mapping_Series()
        {
            var inputColumn = new TableColumn("day", ETypeCode.DateTime);
            
            var outputColumn = new TableColumn("output");
            var inputRow = new object[] {new DateTime(2018, 1,1, 12, 12, 12), };
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            
            var outputTable = new Table("output");

            var mapSeries = new MapSeries(inputColumn, outputColumn, ESeriesGrain.Day, false, null, null);
            
            mapSeries.InitializeColumns(inputTable);
            mapSeries.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            await mapSeries.ProcessInputRowAsync(_functionVariables, inputRow);
            mapSeries.MapOutputRow(outputRow);

            // series value should have the non day elements removed.
            Assert.Equal(new DateTime(2018, 1,1, 0, 0, 0), outputRow[0]);

            var nextValue = mapSeries.NextValue(1);
            Assert.Equal(new DateTime(2018, 1,2, 0, 0, 0), nextValue);
        }
    }
}