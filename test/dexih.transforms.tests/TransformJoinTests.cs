using dexih.transforms;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformJoinTests
    {

        [Fact]
        public void DSortedJoin()
        {
            SourceTable Source = Helpers.CreateSortedTestData();
            TransformJoin transformJoin = new TransformJoin();
            transformJoin.SetJoins("JoinTable", new List<JoinPair>() { new JoinPair { SourceColumn = "StringColumn", JoinColumn = "StringColumn" } });
            transformJoin.SetInTransform(Source, Helpers.CreateSortedJoinData());

            Assert.Equal(8, transformJoin.FieldCount);

            int pos = 0;
            while (transformJoin.Read() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Equal(DBNull.Value, transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public void Join()
        {
            SourceTable Source = Helpers.CreateSortedTestData();
            TransformJoin transformJoin = new TransformJoin();
            transformJoin.SetJoins("JoinTable", new List<JoinPair>() { new JoinPair { SourceColumn = "StringColumn", JoinColumn = "StringColumn" } });
            transformJoin.SetInTransform(Source, Helpers.CreateUnSortedJoinData());

            Assert.Equal(8, transformJoin.FieldCount);

            int pos = 0;
            while (transformJoin.Read() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Equal(null, transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }
    }
}
