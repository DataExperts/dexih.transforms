using dexih.transforms;
using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using dexih.transforms.Exceptions;
using static Dexih.Utils.DataType.DataType;
using dexih.functions.Query;

namespace dexih.transforms.tests
{
    public class TransformJoinTests
    {

        [Fact]
        public async Task JoinSorted()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateSortedJoinData(), new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public async Task JoinHash()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateUnSortedJoinData(), new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");

            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Checks the join transform correctly raises an exception when a duplicate join key exists.
        /// </summary>
        [Fact]
        public async Task JoinHashDuplicate()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateDuplicatesJoinData(), new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            await Assert.ThrowsAsync<DuplicateJoinKeyException>(async () => { while (await transformJoin.ReadAsync() == true) ; });

        }

        /// <summary>
        /// Checks the join transform correctly raises an exception when a duplicate join key exists.  The data is sorted to test the sortedjoin algorithm.
        /// </summary>
        [Fact]
        public async Task JoinSortedDuplicate()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformSort sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort>() { new Sort("StringColumn") });
            TransformJoin transformJoin = new TransformJoin(Source, sortedJoinData, new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            await Assert.ThrowsAsync<DuplicateJoinKeyException>(async () => { while (await transformJoin.ReadAsync() == true) ; });
        }

        /// <summary>
        /// Run a join with an outer join
        /// </summary>
        [Fact]
        public async Task JoinSortedOuterJoin()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformSort sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort>() { new Sort("StringColumn") });
            TransformJoin transformJoin = new TransformJoin(Source, sortedJoinData, new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.All, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if(pos == 4)
                {
                    Assert.Equal("lookup4a", transformJoin["LookupValue"]);
                    await transformJoin.ReadAsync();
                    Assert.Equal("lookup4", transformJoin["LookupValue"]);
                }
                else if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }


        /// <summary>
        /// Run a join with a pre-filter.
        /// </summary>
        [Fact]
        public async Task JoinSortedPreFilter()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformSort sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort>() { new Sort("StringColumn") });
            var conditions = new List<Function>();
            //create a condition to filter only when IsValid == true;
            conditions.Add(new Function( new Func<bool, bool>((isValid) => isValid ), new TableColumn[] { new TableColumn("IsValid", ETypeCode.Boolean, "Join")}, null, null));

            TransformJoin transformJoin = new TransformJoin(Source, sortedJoinData, new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a join with a sort to resolve the duplicate record.
        /// </summary>
        [Fact]
        public async Task JoinPreSortFirstFilter()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformSort sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort>() { new Sort("StringColumn") });
            TransformJoin transformJoin = new TransformJoin(Source, sortedJoinData, new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.First, new TableColumn("LookupValue", ETypeCode.String, "Join"), "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a join with a sort to resolve the duplicate record.
        /// </summary>
        [Fact]
        public async Task JoinPreSortLastFilter()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformSort sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort>() { new Sort("StringColumn") });
            TransformJoin transformJoin = new TransformJoin(Source, sortedJoinData, new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Last, new TableColumn("LookupValue", ETypeCode.String, "Join"), "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos == 4)
                    Assert.Equal("lookup4a", transformJoin["LookupValue"]);
                else if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a join with a static value as one of the join conditions.
        /// </summary>
        [Fact]
        public async Task JoinSortedStaticValue()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            TransformSort sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort>() { new Sort("StringColumn") });

            TransformJoin transformJoin = new TransformJoin(Source, sortedJoinData, new List<JoinPair>() {
                new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
                new JoinPair(new TableColumn("IsValid"), true)
            }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a join with a static value as one of the join conditions.
        /// </summary>
        [Fact]
        public async Task JoinHashStaticValue()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();

            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateDuplicatesJoinData(), new List<JoinPair>() {
                new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
                new JoinPair(new TableColumn("IsValid"), true)
            }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a join with a pre-filter.
        /// </summary>
        [Fact]
        public async Task JoinHashPreFilter()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();
            var conditions = new List<Function>();
            //create a condition to filter only when IsValid == true;
            conditions.Add(new Function(new Func<bool, bool>((isValid) => isValid), new TableColumn[] { new TableColumn("IsValid", ETypeCode.Boolean, "Join") }, null, null));

            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateDuplicatesJoinData(), new List<JoinPair>() { new JoinPair(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos.ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public async Task JoinSortedFunctionFilter()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();

            //create a condition to join the source to the join columns + 1
            var conditions = new List<Function>();
            conditions.Add(new Function(
                new Func<int, int, bool>((source, join) => source == (join - 1) ), 
                new TableColumn[] { new TableColumn("IntColumn", ETypeCode.Int32),  new TableColumn("IntColumn", ETypeCode.Int32, "Join") }, 
                null, null));

            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateSortedJoinData(), null, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 9)
                    Assert.Equal("lookup" + (pos+1).ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }


        [Fact]
        public async Task JoinHashFunctionFilter()
        {
            ReaderMemory Source = Helpers.CreateSortedTestData();

            //create a condition to join the source to the join columns + 1
            var conditions = new List<Function>();
            conditions.Add(new Function(
                new Func<int, int, bool>((source, join) => source == (join - 1)),
                new TableColumn[] { new TableColumn("IntColumn", ETypeCode.Int32), new TableColumn("IntColumn", ETypeCode.Int32, "Join") },
                null, null));

            TransformJoin transformJoin = new TransformJoin(Source, Helpers.CreateUnSortedJoinData(), null, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            int pos = 0;
            while (await transformJoin.ReadAsync() == true)
            {
                pos++;
                if (pos < 9)
                    Assert.Equal("lookup" + (pos + 1).ToString(), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

    }
}
