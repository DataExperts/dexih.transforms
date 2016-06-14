using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class TransformValidationTests
    {
        [Fact]
        public void Validations_unit()
        {
            ReaderMemory Table = Helpers.CreateValidationTestData();

            //set a validatoin that rejects all.
            List<Function> Validations = new List<Function>();
            Function Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk") };
            Validations.Add(Function);

            TransformValidation transformValidation = new TransformValidation(Table, Validations, true);

            Assert.Equal(7, transformValidation.FieldCount);

            int count = 0;
            while (transformValidation.Read() == true)
            {
                count = count + 1;
                Assert.Equal('R', transformValidation["Operation"]);
            }

            Assert.Equal(10, count);

            Table.SetRowNumber(0);

            //set a validation that rejects and cleans
            Validations = new List<Function>();

            //create a simple clean function that set's the max value.
            Function = StandardValidations.GetValidationReference("MaxValue");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("value", ETypeCode.Decimal, true, null, "IntColumn" ),
                    new dexih.functions.Parameter("maxValue", ETypeCode.Decimal, false, 5) };
            Function.Outputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("cleanedValue", ETypeCode.Decimal, true, null, "IntColumn" )
            };
            Function.InvalidAction = Function.EInvalidAction.Clean;

            Validations.Clear();
            Validations.Add(Function);

            transformValidation = new TransformValidation(Table, Validations, true);

            Assert.Equal(7, transformValidation.FieldCount);

            int passCount = 0;
            int rejectCount = 0;
            while (transformValidation.Read() == true)
            {
                Assert.Equal('C', transformValidation["Operation"]);
                Assert.True((int)transformValidation["IntColumn"] <= 5);
                passCount++;
            }

            Assert.Equal(10, passCount);

            //Run the same valuidation with RejectClean set.
            Function = StandardValidations.GetValidationReference("MaxValue");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("value", ETypeCode.Decimal, true, null, "IntColumn" ),
                    new dexih.functions.Parameter("maxValue", ETypeCode.Decimal, false, 5) };
            Function.Outputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("cleanedValue", ETypeCode.Decimal, true, null, "IntColumn" )
            };
            Function.InvalidAction = Function.EInvalidAction.RejectClean;

            Validations.Clear();
            Validations.Add(Function);

            transformValidation = new TransformValidation(Table, Validations, true);

            Assert.Equal(7, transformValidation.FieldCount);

            passCount = 0;
            rejectCount = 0;
            while (transformValidation.Read() == true)
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

    }
}
