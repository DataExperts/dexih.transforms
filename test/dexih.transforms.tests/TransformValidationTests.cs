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
        private readonly ITestOutputHelper output;

        public TransformValidationTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Validations_unit()
        {
            ReaderMemory Table = Helpers.CreateValidationTestData();

            //set a validatoin that rejects all.
            List<Function> Validations = new List<Function>();
            Function Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn"), isArray: true  ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk", isArray: true ) };
            Validations.Add(Function);

            TransformValidation transformValidation = new TransformValidation(Table, Validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            int count = 0;
            while (await transformValidation.ReadAsync() == true)
            {
                count = count + 1;
                Assert.Equal('R', transformValidation["Operation"]);
            }

            Assert.Equal(10, count);

            Table.SetRowNumber(0);

            //set a validation that rejects and cleans
            Validations = new List<Function>();

            //create a simple clean function that set's the max value.
            Function = StandardFunctions.GetFunctionReference("MaxLength");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("value", ETypeCode.String, true, null, new TableColumn("StringColumn") ),
                    new dexih.functions.Parameter("maxLength", ETypeCode.Int32, false, 5) };
            Function.Outputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("cleanedValue", ETypeCode.String, true, null, new TableColumn("StringColumn") )
            };
            Function.InvalidAction = Function.EInvalidAction.Clean;

            Validations.Clear();
            Validations.Add(Function);

            transformValidation = new TransformValidation(Table, Validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            int passCount = 0;
            int rejectCount = 0;
            while (await transformValidation.ReadAsync() == true)
            {
                Assert.Equal('C', transformValidation["Operation"]);
                Assert.True((string)transformValidation["StringColumn"] == "value");
                passCount++;
            }

            Assert.Equal(10, passCount);

            //Run the same valuidation with RejectClean set.
            Function = StandardFunctions.GetFunctionReference("MaxValue");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("value", ETypeCode.Decimal, true, null, new TableColumn("IntColumn") ),
                    new dexih.functions.Parameter("maxLength", ETypeCode.Decimal, false, 5) };
            Function.Outputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("cleanedValue", ETypeCode.Decimal, true, null, new TableColumn("IntColumn") )
            };
            Function.InvalidAction = Function.EInvalidAction.RejectClean;

            Validations.Clear();
            Validations.Add(Function);

            transformValidation = new TransformValidation(Table, Validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            passCount = 0;
            rejectCount = 0;
            while (await transformValidation.ReadAsync() == true)
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
            TransformValidation transform = new TransformValidation();
            transform.SetInTransform(data);

            int count = 0;
            while (await transform.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transform.PerformanceSummary());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task ValidationPerformanceValidationOn(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            TransformValidation transform = new TransformValidation();
            transform.ValidateDataTypes = true;
            transform.SetInTransform(data);

            int count = 0;
            while (await transform.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            output.WriteLine(transform.PerformanceSummary());
        }

    }
}
