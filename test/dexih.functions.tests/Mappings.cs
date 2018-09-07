using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using dexih.functions.BuiltIn;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.functions.tests
{
    public class Mappings
    {
        [Fact]
        public void Mapping_Column()
        {
            var inputColumn = new TableColumn("input");
            var outputColumn = new TableColumn("input");
            var inputRow = new object[] {"field1"};
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            var outputTable = new Table("output");
            
            // map a value
            var mapColumn = new MapColumn("123", outputColumn);
            mapColumn.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            mapColumn.ProcessInputRow(inputRow);
            mapColumn.ProcessOutputRow(outputRow);

            Assert.Equal("123", outputRow[0]);
            
            mapColumn = new MapColumn(inputColumn, outputColumn);
            mapColumn.InitializeInputOrdinals(inputTable);
            mapColumn.AddOutputColumns(outputTable);
            mapColumn.ProcessInputRow(inputRow);
            mapColumn.ProcessOutputRow(outputRow);
            
            Assert.Equal("field1", outputRow[0]);
        }
        
        [Fact]
        public void Mapping_Group()
        {
            var inputColumn = new TableColumn("input", DataType.ETypeCode.String);
            var outputColumn = new TableColumn("input", DataType.ETypeCode.String);
            var inputRow = new object[] {"field1"};
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            var outputTable = new Table("output");
            
            // map a value
            var mapColumn = new MapGroup("123", outputColumn);
            mapColumn.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            mapColumn.ProcessInputRow(inputRow);
            mapColumn.ProcessOutputRow(outputRow);

            Assert.Equal("123", outputRow[0]);
            
            mapColumn = new MapGroup(inputColumn, outputColumn);
            mapColumn.InitializeInputOrdinals(inputTable);
            mapColumn.AddOutputColumns(outputTable);
            mapColumn.ProcessInputRow(inputRow);
            mapColumn.ProcessOutputRow(outputRow);
            
            Assert.Equal("field1", outputRow[0]);
        }
        
        [Fact]
        public void Mapping_Filter()
        {
            var inputColumn1 = new TableColumn("input1", DataType.ETypeCode.String);
            var inputColumn2 = new TableColumn("input2", DataType.ETypeCode.String);
            var inputColumn3 = new TableColumn("input3", DataType.ETypeCode.String);
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn1);
            inputTable.Columns.Add(inputColumn2);
            inputTable.Columns.Add(inputColumn3);

            var inputRow = new object[] {"val1", "val1", "not val1"};
            
            // test filter
            var mapFilter = new MapFilter(inputColumn1, "val1");
            mapFilter.InitializeInputOrdinals(inputTable);
            Assert.True(mapFilter.ProcessInputRow(inputRow));
            
            mapFilter = new MapFilter(inputColumn1, "not val1");
            mapFilter.InitializeInputOrdinals(inputTable);
            Assert.False(mapFilter.ProcessInputRow(inputRow));

            mapFilter = new MapFilter(inputColumn1, inputColumn2);
            mapFilter.InitializeInputOrdinals(inputTable);
            Assert.True(mapFilter.ProcessInputRow(inputRow));

            mapFilter = new MapFilter(inputColumn1, inputColumn3);
            mapFilter.InitializeInputOrdinals(inputTable);
            Assert.False(mapFilter.ProcessInputRow(inputRow));

        }
        
        [Fact]
        public void Mapping_Join()
        {
            var inputColumn1 = new TableColumn("input1", DataType.ETypeCode.String);
            var inputColumn2 = new TableColumn("input2", DataType.ETypeCode.String);
            var inputColumn3 = new TableColumn("input3", DataType.ETypeCode.String);
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
            mapJoin.InitializeInputOrdinals(inputTable, joinTable);
            Assert.True(mapJoin.ProcessInputRow(inputRow, joinRow));

            mapJoin = new MapJoin(inputColumn1, inputColumn3);
            mapJoin.InitializeInputOrdinals(inputTable, joinTable);
            Assert.False(mapJoin.ProcessInputRow(inputRow, joinRow));

        }
        
        [Fact]
        public void Mapping_Aggregate()
        {
            var inputColumn = new TableColumn("input", DataType.ETypeCode.Int32);
            var outputColumn = new TableColumn("output", DataType.ETypeCode.Int32);
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            var outputTable = new Table("output");
            
            // map a value
            var mapAggregate = new MapAggregate(inputColumn, outputColumn, SelectColumn.EAggregate.Sum);
            mapAggregate.AddOutputColumns(outputTable);

            //run twice to ensure reset works.
            for (var i = 0; i < 2; i++)
            {
                mapAggregate.ProcessInputRow(new object[] {1});
                mapAggregate.ProcessInputRow(new object[] {2});
                mapAggregate.ProcessInputRow(new object[] {3});
                var outputRow = new object[1];
                mapAggregate.ProcessResultRow(outputRow, EFunctionType.Aggregate);
                Assert.Equal(6, outputRow[0]);
                mapAggregate.Reset(EFunctionType.Aggregate);
            }
        }

        [Fact]
        public void Mapping_Function()
        {
            var inputColumn1 = new TableColumn("input1", DataType.ETypeCode.String);
            var inputColumn2 = new TableColumn("input2", DataType.ETypeCode.String);
            var outputColumn = new TableColumn("output");
            var inputRow = new object[] {"aaa", "bbb"};
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn1);
            inputTable.Columns.Add(inputColumn2);
            
            var outputTable = new Table("output");
            
            var function = Functions.GetFunction(typeof(MapFunctions).FullName, nameof(MapFunctions.Concat));
            var transformFunction = function.GetTransformFunction();
            var parameters = new Parameters
            {
                Inputs = new List<Parameter.Parameter>()
                {
                    new ParameterArray("input", DataType.ETypeCode.String,
                        new List<Parameter.Parameter>()
                        {
                            new ParameterColumn("values", inputColumn1),
                            new ParameterColumn("values", inputColumn2),
                        })
                },
                ReturnParameter = new ParameterOutputColumn("return", outputColumn)
            };

            // map a value
            var mapFunction = new MapFunction(transformFunction, parameters);
            mapFunction.InitializeInputOrdinals(inputTable);
            mapFunction.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            mapFunction.ProcessInputRow(inputRow);
            mapFunction.ProcessOutputRow(outputRow);

            Assert.Equal("aaabbb", outputRow[0]);
        }
        
        [Fact]
        public void Mapping_Series()
        {
            var inputColumn = new TableColumn("day", DataType.ETypeCode.DateTime);
            
            var outputColumn = new TableColumn("output");
            var inputRow = new object[] {new DateTime(2018, 1,1, 12, 12, 12), };
            
            var inputTable = new Table("input");
            inputTable.Columns.Add(inputColumn);
            
            var outputTable = new Table("output");

            var mapSeries = new MapSeries(inputColumn, outputColumn, ESeriesGrain.Day, false, null, null);
            
            mapSeries.InitializeInputOrdinals(inputTable);
            mapSeries.AddOutputColumns(outputTable);

            var outputRow = new object[1];
            mapSeries.ProcessInputRow(inputRow);
            mapSeries.ProcessOutputRow(outputRow);

            // series value should have the non day elements removed.
            Assert.Equal(new DateTime(2018, 1,1, 0, 0, 0), outputRow[0]);

            var nextValue = mapSeries.NextValue(1);
            Assert.Equal(new DateTime(2018, 1,2, 0, 0, 0), nextValue);
        }
    }
}