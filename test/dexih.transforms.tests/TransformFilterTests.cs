using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class TransformFilterTests
    {
        [Fact]
        public void Filters()
        {
            SourceTable Table = Helpers.CreateSortedTestData();
            TransformFilter TransformFilter = new TransformFilter();
            TransformMapping TransformMapping = new TransformMapping();

            //set a filter that filters all
            List<Function> Conditions = new List<Function>();
            Function Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk") };
            Conditions.Add(Function);

            TransformFilter.SetConditions(Conditions);
            TransformFilter.SetInTransform(Table);

            Assert.Equal(5, TransformFilter.FieldCount);
            Assert.Equal(5, TransformFilter.Fields.Count());

            int count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
            }
            Assert.Equal(0, count);

            //set a filter than filters to 1 row.
            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("IsEqual");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("StringColumn", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.String, false, "value03") };
            Conditions.Add(Function);
            TransformFilter.SetConditions(Conditions);
            Table.ResetValues();
            TransformFilter.SetInTransform(Table);

            count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
                if (count == 1)
                    Assert.Equal(3, TransformFilter["IntColumn"]);
            }
            Assert.Equal(1, count);

            // use the "IN" function to filter 3 rows.
            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("IsIn");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Value", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value03", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value05", isArray: true) ,
                    new dexih.functions.Parameter("CompareTo", ETypeCode.String, false, "value07", isArray: true) };

            Conditions.Add(Function);
            TransformFilter.SetConditions(Conditions);
            Table.ResetValues();
            TransformFilter.SetInTransform(Table);

            count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
            }
            Assert.Equal(3, count);

            // create a mapping, and use the filter after the calculation.
            List<Function> Mappings = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("Substring");
            Function.TargetColumn = "Substring";
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("name", ETypeCode.String, true, null, "StringColumn" ),
                    new dexih.functions.Parameter("start", ETypeCode.Int32, false, 5),
                    new dexih.functions.Parameter("end", ETypeCode.Int32, false, 50) };
            Mappings.Add(Function);
            TransformMapping.SetMappings(null, Mappings);
            Table.ResetValues();
            TransformMapping.SetInTransform(Table);

            Conditions = new List<Function>();
            Function = StandardFunctions.GetFunctionReference("LessThan");
            Function.Inputs = new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("Substring", ETypeCode.Int32, true, null, "Substring" ),
                    new dexih.functions.Parameter("Compare", ETypeCode.Int32, false, 5) };
            Conditions.Add(Function);
            TransformFilter.SetConditions(Conditions);
            TransformFilter.SetInTransform(TransformMapping);

            count = 0;
            while (TransformFilter.Read() == true)
            {
                count = count + 1;
            }
            Assert.Equal(4, count);

        }

    }
}
