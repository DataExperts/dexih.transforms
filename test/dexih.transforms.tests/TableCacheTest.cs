using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class TableCacheTest
    {
        [Fact]
        public void TableCache_UnitTests()
        {
            //set a cache with a max of 5 items.
            TableCache tableCache = new TableCache(5);

            //add less items that the max size to the cache.
            for (int i = 0; i < 4; i++)
            {
                tableCache.Add(new object[] { i });
            }

            Assert.True(tableCache.Count == 4);

            //test the items are added
            int count = 0;
            int index = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal(count, (int)row[0]); //check enumerated result.
                Assert.Equal(count, (int)tableCache[index++][0]); //check indexed result
                count++;
            }

            Assert.Equal(4, index);

            //add another item so that the cache = maxsize.
            tableCache.Add(new object[] { 4 });

            Assert.Equal(5, tableCache.Count);

            //test the items are added
            count = 0;
            index = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal(count, (int)row[0]);
                Assert.Equal(count, (int)tableCache[index++][0]); //check indexed result
                count++;
            }

            Assert.Equal(5, index);

            //add another item so that the cache will be exceed. 
            tableCache.Add(new object[] { 5 });

            //table cound should be 5 as this is the max.
            Assert.Equal(5, tableCache.Count);
            //test the items are added starting the count at 1
            count = 1;
            index = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal(count, (int)row[0]);
                Assert.Equal(count, (int)tableCache[index++][0]); //check indexed result
                count++;
            }

            Assert.Equal(5, index);

            //add one more items to be sure
            tableCache.Add(new object[] { 6 });

            //table cound should be 5 as this is the max.
            Assert.Equal(5, tableCache.Count);

            //test the items are added starting the count at 1
            count = 2;
            index = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal(count, (int)row[0]);
                Assert.Equal(count, (int)tableCache[index++][0]); //check indexed result
                count++;
            }

            Assert.Equal(5, index);


            //run a test with rows = 0 (which means no unlimited cache)
            //set a cache with a max of 5 items.
            tableCache = new TableCache();

            //add less items that the max size to the cache.
            for (int i = 0; i < 4; i++)
            {
                tableCache.Add(new object[] { i });
            }

            Assert.True(tableCache.Count == 4);

            //test the items are added
            count = 0;
            index = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal(count, (int)row[0]); //check enumerated result.
                Assert.Equal(count, (int)tableCache[index++][0]); //check indexed result
                count++;
            }
        }

        [Fact]
        public void TableCache_Performance()
        {
            // this should run in about 250ms.

            int MaxItems = 1000000;

            TableCache tableCache = new TableCache(MaxItems);

            for (int i = 0; i < MaxItems; i++)
            {
                object[] testRow = new object[] { i, "test1" + i.ToString() };
            }

            //test the items loaded successfully
            int count = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal("test" + count.ToString(), (string)row[1]);
            }

            //add the items again.  these should replace previous values.
            for (int i = 0; i < MaxItems; i++)
            {
                object[] testRow = new object[] { i, "test2" + i.ToString() };
            }

            //test the items loaded successfully
            count = 0;
            foreach (object[] row in tableCache)
            {
                Assert.Equal("test2" + count.ToString(), (string)row[1]);
            }

        }
    }
}
