using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Xunit;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class TransformJoinTests
    {

        [Fact]
        public async Task JoinSorted()
        {
            var source = Helpers.CreateSortedTestData();

            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, Helpers.CreateSortedJoinData(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");
            // var transformJoin = new TransformJoin(source, Helpers.CreateSortedJoinData(), new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public async Task JoinHash()
        {
            var source = Helpers.CreateSortedTestData();
            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, Helpers.CreateUnSortedJoinData(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");
            // var transformJoin = new TransformJoin(source, Helpers.CreateUnSortedJoinData(), new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");

            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
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
            var source = Helpers.CreateSortedTestData();
            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, Helpers.CreateDuplicatesJoinData(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");
            // var transformJoin = new TransformJoin(source, Helpers.CreateDuplicatesJoinData(), new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            await Assert.ThrowsAsync<TransformException>(async () => { while (await transformJoin.ReadAsync()) ; });

        }

        /// <summary>
        /// Checks the join transform correctly raises an exception when a duplicate join key exists.  The data is sorted to test the sortedjoin algorithm.
        /// </summary>
        [Fact]
        public async Task JoinSortedDuplicate()
        {
            var source = Helpers.CreateSortedTestData();
            var sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort> { new Sort("StringColumn") });

            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, sortedJoinData, mappings, Transform.EDuplicateStrategy.Abend, null, "Join");

            // var transformJoin = new TransformJoin(source, sortedJoinData, new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            await Assert.ThrowsAsync<TransformException>(async () => { while (await transformJoin.ReadAsync()) ; });
        }
        
        /// <summary>
        /// Checks a sorted join with missing rows in the join table
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task JoinSortedMissingJoinRow()
        {
            var source = Helpers.CreateSortedTestData();

            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, Helpers.CreateSortedJoinDataMissingRows(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");
            // var transformJoin = new TransformJoin(source, Helpers.CreateSortedJoinDataMissingRows(), new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                //Missing rows should be null
                if (pos == 1 || pos == 5 || pos == 10)
                {
                    Assert.Null(transformJoin["LookupValue"]);
                }
                else
                {
                    // Other rows should join ok
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
                }
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a join with an outer join
        /// </summary>
        [Fact]
        public async Task JoinSortedOuterJoin()
        {
            var source = Helpers.CreateSortedTestData();
            var sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort> { new Sort("StringColumn") });

            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, sortedJoinData, mappings, Transform.EDuplicateStrategy.All, null, "Join");
//            var transformJoin = new TransformJoin(source, sortedJoinData, new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.All, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if(pos == 4)
                {
                    Assert.Equal("lookup4a", transformJoin["LookupValue"]);
                    await transformJoin.ReadAsync();
                    Assert.Equal("lookup4", transformJoin["LookupValue"]);
                }
                else if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
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
            var source = Helpers.CreateSortedTestData();
            var sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort> { new Sort("StringColumn") });

            var mappings = new Mappings
            {
                new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
                new MapFunction(
                    new TransformFunction(new Func<bool, bool>(isValid => isValid), null, null),
                    new Parameters()
                    {
                        Inputs = new List<Parameter>() {new ParameterJoinColumn("IsValid", ETypeCode.Boolean)}
                    })
            };
            
            var transformJoin = new TransformJoin(source, sortedJoinData, mappings, Transform.EDuplicateStrategy.Abend, null, "Join");
            
            //            var conditions = new List<TransformFunction>
//            {
//                //create a condition to filter only when IsValid == true;
//                new TransformFunction(
//                new Func<bool, bool>(isValid => isValid),
//                new[] { new TableColumn("IsValid", ETypeCode.Boolean, "Join") },
//                null,
//                null, new GlobalVariables(null))
//            };

//            var transformJoin = new TransformJoin(source, sortedJoinData, new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
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
            var source = Helpers.CreateSortedTestData();
            var sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort> { new Sort("StringColumn") });
            
            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, sortedJoinData, mappings, Transform.EDuplicateStrategy.First, new TableColumn("LookupValue", ETypeCode.String, parentTable: "Join"), "Join");

            // var transformJoin = new TransformJoin(source, sortedJoinData, new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.First, new TableColumn("LookupValue", ETypeCode.String, "Join"), "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
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
            var source = Helpers.CreateSortedTestData();
            var sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort> { new Sort("StringColumn") });
            
            var mappings = new Mappings {new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn"))};
            var transformJoin = new TransformJoin(source, sortedJoinData, mappings, Transform.EDuplicateStrategy.Last, new TableColumn("LookupValue", ETypeCode.String, parentTable: "Join"), "Join");

//            var transformJoin = new TransformJoin(source, sortedJoinData, new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, null, Transform.EDuplicateStrategy.Last, new TableColumn("LookupValue", ETypeCode.String, "Join"), "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos == 4)
                    Assert.Equal("lookup4a", transformJoin["LookupValue"]);
                else if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

        /// <summary>
        /// Run a sorted join with a static value as one of the join conditions.
        /// </summary>
        [Fact]
        public async Task JoinSortedStaticValue()
        {
            var source = Helpers.CreateSortedTestData();
            var sortedJoinData = new TransformSort(Helpers.CreateDuplicatesJoinData(), new List<Sort> { new Sort("StringColumn") });

            var mappings = new Mappings
            {
                new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
                new MapFilter(new TableColumn("IsValid", ETypeCode.Boolean, parentTable: "Join"), true )
            };
            var transformJoin = new TransformJoin(source, sortedJoinData, mappings, Transform.EDuplicateStrategy.Abend, new TableColumn("LookupValue", ETypeCode.String, parentTable: "Join"), "Join");

//            var transformJoin = new TransformJoin(source, sortedJoinData, new List<Join>
//            {
//                new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
//                new Join(new TableColumn("IsValid"), true)
//            }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Sorted);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
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
            var source = Helpers.CreateSortedTestData();

            var mappings = new Mappings
            {
                new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
                new MapFilter(new TableColumn("IsValid", ETypeCode.Boolean, parentTable: "Join"), true )
            };
            var transformJoin = new TransformJoin(source, Helpers.CreateDuplicatesJoinData(), mappings, Transform.EDuplicateStrategy.Abend, new TableColumn("LookupValue", ETypeCode.String, parentTable: "Join"), "Join");

//            var transformJoin = new TransformJoin(source, Helpers.CreateDuplicatesJoinData(), new List<Join>
//            {
//                new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
//                new Join(new TableColumn("IsValid"), true)
//            }, null, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
//            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
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
            var source = Helpers.CreateSortedTestData();

            var mappings = new Mappings
            {
                new MapJoin(new TableColumn("StringColumn"), new TableColumn("StringColumn")),
                new MapFunction(
                    new TransformFunction(new Func<bool, bool>(isValid => isValid), null, null),
                    new Parameters()
                    {
                        Inputs = new List<Parameter>() {new ParameterJoinColumn("IsValid", ETypeCode.Boolean)}
                    })
            };
            
            var transformJoin = new TransformJoin(source, Helpers.CreateDuplicatesJoinData(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");

            
//            var conditions = new List<TransformFunction>
//            {
//                //create a condition to filter only when IsValid == true;
//                new TransformFunction(
//                new Func<bool, bool>(isValid => isValid),
//                new[] { new TableColumn("IsValid", ETypeCode.Boolean, "Join") },
//                null,
//                null, new GlobalVariables(null))
//            };
//            var transformJoin = new TransformJoin(source, Helpers.CreateDuplicatesJoinData(), new List<Join> { new Join(new TableColumn("StringColumn"), new TableColumn("StringColumn")) }, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");

            Assert.Equal(9, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 10)
                    Assert.Equal("lookup" + pos, transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }

        [Fact]
        public async Task JoinSortedFunctionFilter()
        {
            var source = Helpers.CreateSortedTestData();
            var mappings = new Mappings
            {
                new MapFunction(
                    new TransformFunction(new Func<int, int, bool>((source1, join) => source1 == join - 1), null, null),
                    new Parameters()
                    {
                        Inputs = new List<Parameter>()
                        {
                            new ParameterColumn("IntColumn", ETypeCode.Int32),
                            new ParameterJoinColumn("Join", new TableColumn("IntColumn", ETypeCode.Int32, parentTable: "Join"))
                        }
                    })
            };
            
            var transformJoin = new TransformJoin(source, Helpers.CreateSortedJoinData(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");

//            //create a condition to join the source to the join columns + 1
//            var conditions = new List<TransformFunction>
//            {
//                new TransformFunction(
//                new Func<int, int, bool>((source1, join) => source1 == (join - 1)),
//                new[] { new TableColumn("IntColumn", ETypeCode.Int32), new TableColumn("IntColumn", ETypeCode.Int32, "Join") },
//                null,
//                null, new GlobalVariables(null))
//            };
//
//            var transformJoin = new TransformJoin(source, Helpers.CreateSortedJoinData(), null, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 9)
                    Assert.Equal("lookup" + (pos+1), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.

            }
            Assert.Equal(10, pos);
        }


        [Fact]
        public async Task JoinHashFunctionFilter()
        {
            var source = Helpers.CreateSortedTestData();

            var mappings = new Mappings
            {
                new MapFunction(
                    new TransformFunction(new Func<int, int, bool>((source1, join) => source1 == join - 1), null, null),
                    new Parameters()
                    {
                        Inputs = new List<Parameter>()
                        {
                            new ParameterColumn("IntColumn", ETypeCode.Int32),
                            new ParameterJoinColumn("Join", new TableColumn("IntColumn", ETypeCode.Int32, parentTable: "Join"))
                        }
                    })
            };
            
            var transformJoin = new TransformJoin(source, Helpers.CreateUnSortedJoinData(), mappings, Transform.EDuplicateStrategy.Abend, null, "Join");

//            //create a condition to join the source to the join columns + 1
//            var conditions = new List<TransformFunction>
//            {
//                new TransformFunction(
//                new Func<int, int, bool>((source1, join) => source1 == (join - 1)),
//                new[] { new TableColumn("IntColumn", ETypeCode.Int32), new TableColumn("IntColumn", ETypeCode.Int32, "Join") },
//                null,
//                null, new GlobalVariables(null))
//            };
//
//            var transformJoin = new TransformJoin(source, Helpers.CreateUnSortedJoinData(), null, conditions, Transform.EDuplicateStrategy.Abend, null, "Join");
            Assert.Equal(8, transformJoin.FieldCount);

            await transformJoin.Open(1, null, CancellationToken.None);
            Assert.True(transformJoin.JoinAlgorithm == TransformJoin.EJoinAlgorithm.Hash);

            var pos = 0;
            while (await transformJoin.ReadAsync())
            {
                pos++;
                if (pos < 9)
                    Assert.Equal("lookup" + (pos + 1), transformJoin["LookupValue"]);
                else
                    Assert.Null(transformJoin["LookupValue"]); //test the last join which is not found.
            }
            Assert.Equal(10, pos);
        }

    }
}
