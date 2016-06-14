using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static dexih.functions.DataType;

namespace dexih.transforms.tests
{
    public class TransformProfileTests
    {
        public static ReaderMemory CreateProfileTestData()
        {
            Table table = new Table("test", 0,
                new TableColumn("StringColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("NullsBlanksColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("ZerosColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("MaxLengthColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("MaxValueColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DistinctValuesColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("PatternsColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey)
                );

            table.Data.Add(new object[] { "value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), DBNull.Value, 0, "a", 1, "value1", "12345"  });
            table.Data.Add(new object[] { "value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), null, 0, "ab", 3, "value2", "1234a" });
            table.Data.Add(new object[] { "value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), "", 1, "abcd", 4, "value1", "54321" });
            table.Data.Add(new object[] { "value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), "", 2, "abcde", 1, "value3", "AB123" });
            table.Data.Add(new object[] { "value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), "not null", -1, "abc", 3, "value1", "ZA123" });
            table.Data.Add(new object[] { "value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), "not null", "not zero", "abc", 3.5, "value1", "AB123" });
            table.Data.Add(new object[] { "value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), "not null", 2, "ab", 4.1, "value1", "12345" });
            table.Data.Add(new object[] { "value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), "not null", 2, "ab", 1, "value2", "32122" });
            table.Data.Add(new object[] { "value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), "not null", 2, "ab", 1, "value1", "1234a" });
            table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), "not null", 2.1, "ab", -1, "value1", "12335" });

            ReaderMemory Adapter = new ReaderMemory(table, new List<Sort>() { new Sort("StringColumn") });
            Adapter.Reset();
            return Adapter;
        }

        [Fact]
        public void ProfileTest()
        {
            ReaderMemory Table = CreateProfileTestData();

            List<Function> profiles = new List<Function>();

            profiles.Add(StandardProfiles.GetProfileReference(false, "BestDataType", "StringColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "BestDataType", "IntColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "BestDataType", "DecimalColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "BestDataType", "DateColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "Nulls", "NullsBlanksColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "Blanks", "NullsBlanksColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "Zeros", "ZerosColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "MaxLength", "MaxLengthColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "MaxValue", "MaxValueColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "DistinctValues", "DistinctValuesColumn"));
            profiles.Add(StandardProfiles.GetProfileReference(false, "Patterns", "PatternsColumn"));

            TransformProfile transformProfile = new TransformProfile(Table, profiles);

            //read all records in the tranform profile
            int count = 0;
            while(transformProfile.Read())
            {
                count++;
            }

            Assert.Equal(10, count); //confirm profile hasn't impacted the read.

            Transform profileResults = transformProfile.ProfileResults;
            count = 0;
            while(profileResults.Read())
            {
                switch((string)profileResults["Column"])
                {
                    case "StringColumn":
                        Assert.Equal("String", (string)profileResults["Result"]);
                        break;
                    case "IntColumn":
                        Assert.Equal("Integer", (string)profileResults["Result"]);
                        break;
                    case "DecimalColumn":
                        Assert.Equal("Double", (string)profileResults["Result"]);
                        break;
                    case "DateColumn":
                        Assert.Equal("DateTime", (string)profileResults["Result"]);
                        break;
                    case "NullsBlanksColumn":
                        if((string)profileResults["Profile"] == "Nulls")
                            Assert.Equal("20.00%", (string)profileResults["Result"]);
                        else
                            Assert.Equal("40.00%", (string)profileResults["Result"]);
                        break;
                    case "MaxLengthColumn":
                        Assert.Equal("5", (string)profileResults["Result"]);
                        break;
                    case "MaxValueColumn":
                        Assert.Equal("4.1", (string)profileResults["Result"]);
                        break;
                    case "DistinctValuesColumn":
                        Assert.Equal("3", (string)profileResults["Result"]);
                        break;
                    case "PatternsColumn":
                        Assert.Equal("Pattern Count=3, Most common(50.00%): 99999", (string)profileResults["Result"]);
                        break;
                }
                count ++;
            }

            Assert.Equal(11, count);
        }

    }
}
