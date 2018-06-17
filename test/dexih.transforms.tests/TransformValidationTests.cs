using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformValidationTests
    {
        private readonly ITestOutputHelper _output;

        public TransformValidationTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public async Task Validations_unit()
        {
            var table = Helpers.CreateValidationTestData();

            //set a validatoin that rejects all.
            var validations = new List<TransformFunction>();
            var transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.ConditionFunctions", "IsEqual").GetTransformFunction();
            transformFunction.Inputs = new[] {
                    new Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn"), isArray: true  ),
                    new Parameter("Compare", ETypeCode.String, false, "junk", isArray: true ) };
            validations.Add(transformFunction);

            var transformValidation = new TransformValidation(table, validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            var count = 0;
            while (await transformValidation.ReadAsync())
            {
                count = count + 1;
                Assert.Equal('R', transformValidation["Operation"]);
            }

            Assert.Equal(10, count);

            table.SetRowNumber(0);

            //set a validation that rejects and cleans
            validations = new List<TransformFunction>();

            //create a simple clean function that set's the max value.
            transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.ValidationFunctions", "MaxLength").GetTransformFunction();
            transformFunction.Inputs = new[] {
                    new Parameter("value", ETypeCode.String, true, null, new TableColumn("StringColumn") ),
                    new Parameter("maxLength", ETypeCode.Int32, false, 5) };
            transformFunction.Outputs = new[] {
                    new Parameter("cleanedValue", ETypeCode.String, true, null, new TableColumn("StringColumn") )
            };
            transformFunction.InvalidAction = TransformFunction.EInvalidAction.Clean;

            validations.Clear();
            validations.Add(transformFunction);

            transformValidation = new TransformValidation(table, validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            var passCount = 0;
            var rejectCount = 0;
            while (await transformValidation.ReadAsync())
            {
                Assert.Equal('C', transformValidation["Operation"]);
                Assert.True((string)transformValidation["StringColumn"] == "value");
                passCount++;
            }

            Assert.Equal(10, passCount);

            //Run the same valuidation with RejectClean set.
            transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.ValidationFunctions", "MaxValue").GetTransformFunction();
            transformFunction.Inputs = new[] {
                    new Parameter("value", ETypeCode.Decimal, true, null, new TableColumn("IntColumn") ),
                    new Parameter("maxLength", ETypeCode.Decimal, false, 5) };
            transformFunction.Outputs = new[] {
                    new Parameter("cleanedValue", ETypeCode.Decimal, true, null, new TableColumn("IntColumn") )
            };
            transformFunction.InvalidAction = TransformFunction.EInvalidAction.RejectClean;

            validations.Clear();
            validations.Add(transformFunction);

            transformValidation = new TransformValidation(table, validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            passCount = 0;
            rejectCount = 0;
            while (await transformValidation.ReadAsync())
            {
                if ((char)transformValidation["Operation"] == 'C')
                {
                    Assert.True((int)transformValidation["IntColumn"] <= 5);
                    passCount++;
                }
                else
                    rejectCount++;
            }

            Assert.Equal(10, passCount);
            Assert.Equal(5, rejectCount);
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task ValidationPerformanceValidationOff(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var transform = new TransformValidation();
            transform.SetInTransform(data);

            var count = 0;
            while (await transform.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            _output.WriteLine(transform.PerformanceSummary());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task ValidationPerformanceValidationOn(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var transform = new TransformValidation();
            transform.ValidateDataTypes = true;
            transform.SetInTransform(data);

            var count = 0;
            while (await transform.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            _output.WriteLine(transform.PerformanceSummary());
        }

    }
}
