using System.Collections.Generic;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Parameter;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformRowTests
    {

        [Fact]
        public async void RowTest_CommaSeparated()
        {
            var table = new Table("test", 0, new TableColumns
            {
                new TableColumn("csvField", ETypeCode.String)
            });

            var values = new[] {"a", "b", "c", "d", "e"};

            table.AddRow(string.Join(',', values));
            var source = new ReaderMemory(table);
            source.Reset();
            
            var mappings = new Mappings(false);

            var split = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.SplitColumnToRows), Helpers.BuiltInAssembly).GetTransformFunction(typeof(string));
            
            var parameters = new Parameters
            {
                Inputs = new Parameter[]
                {
                    new ParameterValue("separator", ETypeCode.String, ","),
                    new ParameterColumn("csvField", ETypeCode.String),
                    new ParameterValue("rows", ETypeCode.Int32, 4),
                },
                Outputs = new Parameter[] 
                {
                    new ParameterOutputColumn("Value", ETypeCode.String)
                }
            };
            
            mappings.Add(new MapFunction(split, parameters, EFunctionCaching.NoCache));
            
            var transformRow = new TransformRows(source, mappings);
            await transformRow.Open();
            
            var pos = 0;
            while (await transformRow.ReadAsync())
            {
                Assert.Equal(values[pos++], transformRow["Value"]);
            }
            
            Assert.Equal(4, pos);
        }
        
        [Fact]
        public async void RowTest_ColumnPivot()
        {
            var table = new Table("test", 0, new TableColumns
            {
                new TableColumn("col0"),
                new TableColumn("col1"),
                new TableColumn("col2"),
                new TableColumn("col3")
            });

            var values = new object[] {"a", "b", "c", "d"};

            table.AddRow(values);
            var source = new ReaderMemory(table);
            source.Reset();
            
            var mappings = new Mappings(false);

            var parameters = new Parameters
            {
                Inputs = new Parameter[]
                {
                    new ParameterArray("columns", ETypeCode.String, 1, new List<Parameter>
                    {
                        new ParameterColumn("col0", ETypeCode.String),
                        new ParameterColumn("col1", ETypeCode.String),
                        new ParameterColumn("col2", ETypeCode.String),
                        new ParameterColumn("col3", ETypeCode.String),
                    }), 
                },
                Outputs = new Parameter[] 
                {
                    new ParameterOutputColumn("column", ETypeCode.String),
                    new ParameterOutputColumn("value", ETypeCode.String),
                }
            };

            var function = Functions.GetFunction(typeof(RowFunctions).FullName, nameof(RowFunctions.ColumnsToRows), Helpers.BuiltInAssembly).GetTransformFunction(typeof(string), parameters);

            mappings.Add(new MapFunction(function, parameters, EFunctionCaching.NoCache));
            
            var transformRow = new TransformRows(source, mappings);
            await transformRow.Open();

            var pos = 0;
            while (await transformRow.ReadAsync())
            {
                Assert.Equal($"col{pos}", transformRow["column"]);
                Assert.Equal(values[pos], transformRow["value"]);
                pos++;
            }
            
            Assert.Equal(4, pos);
        }
    }
}