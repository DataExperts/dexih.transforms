using System;
using dexih.functions;
using dexih.functions.Parameter;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class dexih_functions_parameters
    {
        [Theory]
        [InlineData(DataType.ETypeCode.Boolean, "abc", false)]
        [InlineData(DataType.ETypeCode.Boolean, "true", true)]
        [InlineData(DataType.ETypeCode.Int32, "abc", false)]
        [InlineData(DataType.ETypeCode.Int32, "123", true)]
        [InlineData(DataType.ETypeCode.DateTime, "32 january 2015", false)]
        [InlineData(DataType.ETypeCode.DateTime, "31 january 2015", true)]
        public void ParameterValue_Tests(DataType.ETypeCode dataType, object value, bool expectedSuccess)
        {
            try
            {
                var parameter = new ParameterValue("test", dataType, value);
                Assert.True(expectedSuccess);
            }
            catch (Exception)
            {
                Assert.False(expectedSuccess);
            }
        }

        [Theory]
        [InlineData(DataType.ETypeCode.Boolean, "abc", false)]
        [InlineData(DataType.ETypeCode.Boolean, "true", true)]
        [InlineData(DataType.ETypeCode.Int32, "abc", false)]
        [InlineData(DataType.ETypeCode.Int32, "123", true)]
        [InlineData(DataType.ETypeCode.DateTime, "32 january 2015", false)]
        [InlineData(DataType.ETypeCode.DateTime, "31 january 2015", true)]
        public void ParameterColumn_Tests(DataType.ETypeCode dataType, object value, bool expectedSuccess)
        {
            var table = new Table("Test");
            var column = new TableColumn("test", dataType);
            table.Columns.Add(column);
            var parameter = new ParameterColumn("test", column);
            parameter.InitializeOrdinal(table);

            try
            {
                var row = new [] {value};
                parameter.SetInputData(row);
                Assert.True(expectedSuccess);
            }
            catch (Exception)
            {
                Assert.False(expectedSuccess);
            }
        }
        
        [Theory]
        [InlineData(DataType.ETypeCode.Boolean, "abc", false)]
        [InlineData(DataType.ETypeCode.Boolean, "true", true)]
        [InlineData(DataType.ETypeCode.Int32, "abc", false)]
        [InlineData(DataType.ETypeCode.Int32, "123", true)]
        [InlineData(DataType.ETypeCode.DateTime, "32 january 2015", false)]
        [InlineData(DataType.ETypeCode.DateTime, "31 january 2015", true)]
        public void ParameterArray_Tests(DataType.ETypeCode dataType, object value, bool expectedSuccess)
        {
            var parameters = new Parameter[2];
            parameters[0] = new ParameterValue("p1", DataType.ETypeCode.String, "abc");
            
            var table = new Table("Test");
            var column = new TableColumn("test", dataType);
            table.Columns.Add(column);
            parameters[1] = new ParameterColumn("p2", column);

            var parameterArray = new ParameterArray("p2", DataType.ETypeCode.String, parameters);
            parameterArray.InitializeOrdinal(table);

            try
            {
                // create a row with first parameter is array.
                var row = new [] {value, value};
                parameterArray.SetInputData(row);
                Assert.True(expectedSuccess);
            }
            catch (Exception)
            {
                Assert.False(expectedSuccess);
            }
        }

        [Fact]
        public void Parameter_WriteInputs()
        {
            var table = new Table("test");
            var column1 = new TableColumn("in1", DataType.ETypeCode.String);
            var column2 = new TableColumn("in2", DataType.ETypeCode.String);
            var column3 = new TableColumn("in3", DataType.ETypeCode.String);
            table.Columns.Add(column1);
            table.Columns.Add(column2);
            table.Columns.Add(column3);

            var inputs = new Parameter[3];
            inputs[0] = new ParameterValue("p1", DataType.ETypeCode.String, "static");
            inputs[1] = new ParameterColumn("p2", column1);
            var arrayParameters = new Parameter[] {new ParameterColumn("p3a", column2), new ParameterColumn("p3b", column3)};
            inputs[2] = new ParameterArray("p3", DataType.ETypeCode.String, arrayParameters);

            var parameters = new Parameters();
            parameters.Inputs = inputs;
            
            parameters.InitializeInputs(inputs, table);

            var row = new object[] {"val1", "val2", "val3"};
            parameters.SetFromRow(row);
            var functionParameters = parameters.GetFunctionParameters();

            Assert.Equal("static", functionParameters[0]);
            Assert.Equal("val1", functionParameters[1]);
            Assert.Equal("val2", ((object[])functionParameters[2])[0]);
            Assert.Equal("val3", ((object[])functionParameters[2])[1]);
        }

        
        [Fact]
        public void Parameter_WriteOutputs()
        {
            var table = new Table("test");
            var returnColumn = new TableColumn("return", DataType.ETypeCode.String);
            var column1 = new TableColumn("out1", DataType.ETypeCode.String);
            var column2 = new TableColumn("out2", DataType.ETypeCode.String);
            var column3 = new TableColumn("out3", DataType.ETypeCode.String);

            table.Columns.Add(returnColumn);
            table.Columns.Add(column1);
            table.Columns.Add(column2);
            table.Columns.Add(column3);

            var returnParameter = new ParameterColumn("return", returnColumn);
            var outputs = new Parameter[2];
            outputs[0] = new ParameterColumn("p2", column1);
            var arrayParameters = new Parameter[] {new ParameterColumn("p3a", column2), new ParameterColumn("p3b", column3)};
            outputs[1] = new ParameterArray("p3", DataType.ETypeCode.String, arrayParameters);

            var parameters = new Parameters();
            parameters.InitializeOutputs(returnParameter, outputs, table);

            var row = new object[4];
            parameters.SetFunctionResult("returnValue", new object[] {"val1", new object[] {"val2a", "val2b"} }, row );
            
            Assert.Equal("returnValue", row[0]);
            Assert.Equal("val1", row[1]);
            Assert.Equal("val2a", row[2]);
            Assert.Equal("val2b", row[3]);
            
        }

    }
}