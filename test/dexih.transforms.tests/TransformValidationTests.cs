using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Parameter;
using dexih.transforms.Mapping;
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
            _output = output;
        }

        [Fact]
        public async Task Validations_Simple()
        {
            var table = Helpers.CreateValidationTestData();
            table.SetCacheMethod(Transform.ECacheMethod.DemandCache);

            //set a filter that filters all
            var function = Functions
                .GetFunction(typeof(ConditionFunctions<>).FullName, nameof(ConditionFunctions<string>.IsEqual), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(string));
            var parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterArray("Compare", ETypeCode.String, 1, new List<Parameter>
                    {
                        new ParameterColumn("StringColumn", new TableColumn("StringColumn")),
                        new ParameterValue("Compare", ETypeCode.String, "junk")
                    })
                },
            };

            var mappings = new Mappings
            {
                new MapValidation(function, parameters)
            };

            var transformValidation = new TransformValidation(table, mappings, true);
            await transformValidation.Open(0, null, CancellationToken.None);
            //            //set a validation that rejects all.
            //            var validations = new List<TransformFunction>();
            //            var transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.ConditionFunctions", "IsEqual").GetTransformFunction();
            //            transformFunction.Inputs = new[] {
            //                    new Parameter("StringColumn", ETypeCode.String, true, null, new TableColumn("StringColumn"), isArray: true  ),
            //                    new Parameter("Compare", ETypeCode.String, false, "junk", isArray: true ) };
            //            validations.Add(transformFunction);
            //
            //            var transformValidation = new TransformValidation(table, validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            var count = 0;
            while (await transformValidation.ReadAsync())
            {
                count = count + 1;
                Assert.Equal('R', transformValidation["Operation"]);
            }

            Assert.Equal(10, count);
        }


        [Fact]
        public async Task Validations_Clean()
        {
            var table = Helpers.CreateValidationTestData();
            table.SetCacheMethod(Transform.ECacheMethod.DemandCache);


            //set a validation that rejects and cleans
            //set a filter that filters all
            var function = Functions
                .GetFunction(typeof(ValidationFunctions).FullName, nameof(ValidationFunctions.MaxLength), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(string));
            function.InvalidAction = TransformFunction.EInvalidAction.Clean;
            var parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterColumn("value", new TableColumn("StringColumn")),
                    new ParameterValue("maxLength", ETypeCode.Int32, 5)
                },
                Outputs = new List<Parameter>
                {
                    new ParameterOutputColumn("StringColumn", ETypeCode.String)
                }
            };

            var mappings = new Mappings
            {
                new MapValidation(function, parameters)
            };
            var transformValidation = new TransformValidation(table, mappings, true);
            await transformValidation.Open();

            //            validations = new List<TransformFunction>();
            //
            //            //create a simple clean function that set's the max value.
            //            transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.ValidationFunctions", "MaxLength").GetTransformFunction();
            //            transformFunction.Inputs = new[] {
            //                    new Parameter("value", ETypeCode.String, true, null, new TableColumn("StringColumn") ),
            //                    new Parameter("maxLength", ETypeCode.Int32, false, 5) };
            //            transformFunction.Outputs = new[] {
            //                    new Parameter("cleanedValue", ETypeCode.String, true, null, new TableColumn("StringColumn") )
            //            };
            //            transformFunction.InvalidAction = TransformFunction.EInvalidAction.Clean;
            //
            //            validations.Clear();
            //            validations.Add(transformFunction);

            //            transformValidation = new TransformValidation(table, validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            var passCount = 0;
            while (await transformValidation.ReadAsync())
            {
                Assert.Equal('C', transformValidation["Operation"]);
                Assert.True((string) transformValidation["StringColumn"] == "value");
                passCount++;
            }

            Assert.Equal(10, passCount);
        }
        
        [Fact]
        public async Task Validations_RejectClean()
        {
            var table = Helpers.CreateValidationTestData();
            table.SetCacheMethod(Transform.ECacheMethod.DemandCache);

            //set a validation that rejects and cleans
            //set a filter that filters all
            var function = Functions
                .GetFunction(typeof(ValidationFunctions).FullName, nameof(ValidationFunctions.MaxValue), Helpers.BuiltInAssembly)
                .GetTransformFunction(typeof(int));
            function.InvalidAction = TransformFunction.EInvalidAction.RejectClean;
            var parameters = new Parameters
            {
                Inputs = new List<Parameter>
                {
                    new ParameterColumn("value", new TableColumn("IntColumn", ETypeCode.Int32)),
                    new ParameterValue("cleanedValue", ETypeCode.Int32, 5)
                },
                Outputs = new List<Parameter>
                {
                    new ParameterOutputColumn("IntColumn", ETypeCode.Int32)
                }
            };

            var mappings = new Mappings
            {
                new MapValidation(function, parameters)
            };
            var transformValidation = new TransformValidation(table, mappings, true);
            await transformValidation.Open(0, null, CancellationToken.None);

            //        //Run the same validation with RejectClean set.
            //            transformFunction = Functions.GetFunction("dexih.functions.BuiltIn.ValidationFunctions", "MaxValue").GetTransformFunction();
            //            transformFunction.Inputs = new[] {
            //                    new Parameter("value", ETypeCode.Decimal, true, null, new TableColumn("IntColumn") ),
            //                    new Parameter("maxLength", ETypeCode.Decimal, false, 5) };
            //            transformFunction.Outputs = new[] {
            //                    new Parameter("cleanedValue", ETypeCode.Decimal, true, null, new TableColumn("IntColumn") )
            //            };
            //            transformFunction.InvalidAction = TransformFunction.EInvalidAction.RejectClean;
            //
            //            validations.Clear();
            //            validations.Add(transformFunction);
            //
            //            transformValidation = new TransformValidation(table, validations, true);

            Assert.Equal(8, transformValidation.FieldCount);

            var passCount = 0;
            var rejectCount = 0;
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
            var mappings = new Mappings();
            var transform = new TransformValidation(data, mappings, false);
            await transform.Open(0, null, CancellationToken.None);

            var count = 0;
            while (await transform.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            _output.WriteLine(transform.PerformanceDetails());
        }

        [Theory]
        [InlineData(100000)] //should run in ~ 250ms
        public async Task ValidationPerformanceValidationOn(int rows)
        {
            var data = Helpers.CreateLargeTable(rows);
            var mappings = new Mappings();
            var transform = new TransformValidation(data, mappings, true);
            await transform.Open(0, null, CancellationToken.None);

            var count = 0;
            while (await transform.ReadAsync())
                count++;

            Assert.Equal(rows, count);

            _output.WriteLine(transform.PerformanceDetails());
        }

    }
}
