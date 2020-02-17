using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformDeltaTests
    {
        [Fact]
        public async Task RunDeltaTest_reload()
        {
            var source = Helpers.CreateSortedTestData();

            var targetTable = source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            var target = new ReaderMemory(targetTable);

            //run a reload.  
            var transformDelta = new TransformDelta(source, target, EUpdateStrategy.Reload, 0, false);
            await transformDelta.Open(0, null, CancellationToken.None);
            await transformDelta.ReadAsync();
            Assert.True((char) transformDelta["Operation"] == 'T');

            var count = 0;
            while (await transformDelta.ReadAsync())
            {
                Assert.True((char) transformDelta["Operation"] == 'C');
                Assert.True((long) transformDelta["SurrogateKey"] == count + 1);
                Assert.True((int) transformDelta["IntColumn"] == count + 1);

                count++;
            }

            Assert.True(count == 10);

            //run an append.  (only difference from reload is no truncate record at start.
            transformDelta = new TransformDelta(source, target, EUpdateStrategy.Append, 0, false);
            await transformDelta.Open(0, null, CancellationToken.None);
            transformDelta.Reset();
            transformDelta.Open();

            count = 0;
            while (await transformDelta.ReadAsync())
            {
                Assert.True((char) transformDelta["Operation"] == 'C');
                Assert.True((long) transformDelta["SurrogateKey"] == count + 1);
                Assert.True((int) transformDelta["IntColumn"] == count + 1);

                count++;
            }

            Assert.True(count == 10);
        }

        [Fact]
        public async Task RunDeltaTest_update()
        {
            var source = Helpers.CreateUnSortedTestData();
            source.SetCacheMethod(ECacheMethod.DemandCache);

            var targetTable = source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            Transform target = new ReaderMemory(targetTable);

            //run an update load with nothing in the target, which will result in 10 rows created.
            var transformDelta = new TransformDelta(source, target, EUpdateStrategy.AppendUpdate, 0, false);
            transformDelta.SetCacheMethod(ECacheMethod.DemandCache);
            await transformDelta.Open(0, null, CancellationToken.None);

            var count = 0;
            while (await transformDelta.ReadAsync())
            {
                Assert.True((char) transformDelta["Operation"] == 'C');
                Assert.True((long) transformDelta["SurrogateKey"] == count + 1);
                Assert.True((int) transformDelta["IntColumn"] == count + 1);

                count++;
            }

            Assert.Equal(10, count);

            transformDelta.SetRowNumber(0);

            //write result to a memory table
            var memoryConnection = new ConnectionMemory();
            var targetWriter = new TransformWriterTarget(memoryConnection, targetTable );
            await targetWriter.WriteRecordsAsync(transformDelta, null, CancellationToken.None);

            target = memoryConnection.GetTransformReader(target.CacheTable); // new ReaderMemory(target.CacheTable, null);
            target.SetCacheMethod(ECacheMethod.DemandCache);

            //Set the target pointer back to the start and rerun.  Now 10 rows should be ignored.
            source.SetRowNumber(0);
            target.SetRowNumber(0);

            //run an append.  (only difference from reload is no truncate record at start.
            transformDelta = new TransformDelta(source, target, EUpdateStrategy.AppendUpdate, 0, false);
            await transformDelta.Open();

            count = 0;
            while (await transformDelta.ReadAsync())
            {
                count++;
            }

            Assert.Equal(10, transformDelta.TotalRowsIgnored);
            Assert.Equal(0, count);

            //change 3 rows. (first, middle, last) to simulate target table data changes
            target.CacheTable.Data[0][4] = 100;
            target.CacheTable.Data[5][4] = 200;
            target.CacheTable.Data[9][4] = 300;

            //add a duplicate in the source
            var row = new object[target.CacheTable.Columns.Count];
            target.CacheTable.Data[9].CopyTo(row, 0);
            target.CacheTable.AddRow(row);

            transformDelta.Reset();
            transformDelta.Open();

            count = 0;
            while (await transformDelta.ReadAsync())
            {
                count++;
                Assert.True((char) transformDelta["Operation"] == 'U');
            }

            Assert.True(count == 3);

            //delete rows from the target, which should trigger two creates.
            target.CacheTable.Data.RemoveAt(1);
            target.CacheTable.Data.RemoveAt(7);

            transformDelta.Reset();
            transformDelta.Open();

            count = 0;
            var rowsCreated = 0;
            var rowsUpdated = 0;
            while (await transformDelta.ReadAsync())
            {
                rowsCreated += (char) transformDelta["Operation"] == 'C' ? 1 : 0;
                rowsUpdated += (char) transformDelta["Operation"] == 'U' ? 1 : 0;
                count++;
            }

            Assert.Equal(2, rowsCreated);
            Assert.Equal(3, rowsUpdated);
            Assert.Equal(5, count);

            //delete rows from the source, which should not cause any change as delete detection is not on.
            source.CacheTable.Data.RemoveAt(9);
            source.CacheTable.Data.RemoveAt(0); //this is the row that was updated, so update now = 2

            transformDelta.Reset();
            transformDelta.Open();

            count = 0;
            rowsCreated = 0;
            rowsUpdated = 0;
            while (await transformDelta.ReadAsync())
            {
                rowsCreated += (char) transformDelta["Operation"] == 'C' ? 1 : 0;
                rowsUpdated += (char) transformDelta["Operation"] == 'U' ? 1 : 0;
                count++;
            }

            Assert.True(rowsCreated == 1);
            Assert.True(rowsUpdated == 2);
            Assert.True(count == 3);
        }

        // checks a datetime is within 100 second of the current.
        private bool DateIsNearCurrent(DateTime value)
        {
            return value > DateTime.Now.AddSeconds(-100) && value < DateTime.Now;
        }

        [Fact]
        public async Task RunDeltaTest_updatePreserve()
        {
            var source = Helpers.CreateUnSortedTestData();
            source.SetCacheMethod(ECacheMethod.DemandCache);

            var targetTable = source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            var surrrogateKey = 0L;

            Transform target = new ReaderMemory(targetTable);
            target.SetCacheMethod(ECacheMethod.DemandCache);

            //run an update load with nothing in the target.  
            var transformDelta = new TransformDelta(source, target, EUpdateStrategy.AppendUpdateDeletePreserve,  surrrogateKey, false);
            transformDelta.SetCacheMethod(ECacheMethod.DemandCache);
            await transformDelta.Open(0, null, CancellationToken.None);

            var count = 0;

            var createDateMin = DateTime.Now;
            
            while (await transformDelta.ReadAsync())
            {
                Assert.True((char) transformDelta["Operation"] == 'C');
                Assert.True((long) transformDelta["SurrogateKey"] == count + 1);
                Assert.True((int) transformDelta["IntColumn"] == count + 1);
                Assert.True((int) transformDelta["Version"] == 1);
                Assert.True(DateIsNearCurrent((DateTime) transformDelta["CreateDate"]));
                Assert.True(DateIsNearCurrent((DateTime) transformDelta["UpdateDate"]));
                Assert.True((bool) transformDelta["IsCurrent"]);

                count++;
            }

            var createDateMax = DateTime.Now;

            Assert.Equal(10, count);
            surrrogateKey = transformDelta.AutoIncrementValue;

            transformDelta.SetRowNumber(0);

            // create the table in memory database
            var memoryConnection = new ConnectionMemory();
            var table = target.CacheTable;
            await memoryConnection.CreateTable(table, false, CancellationToken.None);

            // write result to a memory table
            var targetWriter = new TransformWriterTarget(memoryConnection, table );
            await targetWriter.WriteRecordsAsync(transformDelta, null, CancellationToken.None);

//            var writer = new TransformWriterTarget(memoryConnection, target.CacheTable);
//            await writer.WriteRecordsAsync(transformDelta, CancellationToken.None);

            target = memoryConnection.GetTransformReader(table);
            target.SetCacheMethod(ECacheMethod.DemandCache);

            //run an append.  (only difference from reload is no truncate record at start.
            transformDelta = new TransformDelta(source, target, EUpdateStrategy.AppendUpdatePreserve,  surrrogateKey, false);
            await transformDelta.Open(0, null, CancellationToken.None);

            count = 0;
            while (await transformDelta.ReadAsync())
            {
                count++;
            }
            Assert.Equal(0, count );

            //change 3 rows. (first, middle, last)
            table.Data[0][4] = 100;
            table.Data[5][4] = 200;
            table.Data[9][4] = 300;

            //add a duplicate in the source
            var row = new object[table.Columns.Count];
            table.Data[9].CopyTo(row, 0);
            table.AddRow(row);

            transformDelta = new TransformDelta(source, target, EUpdateStrategy.AppendUpdatePreserve, surrrogateKey, false);
            transformDelta.SetCacheMethod(ECacheMethod.DemandCache);
            await transformDelta.Open(0, null, CancellationToken.None);

            count = 0;
            var rowsCreated = 0;
            var rowsUpdated = 0;
            while (await transformDelta.ReadAsync())
            {
                if ((char) transformDelta["Operation"] == 'C')
                {
                    rowsCreated += 1;    
                    Assert.Equal((long)surrrogateKey + rowsCreated, (long) transformDelta["SurrogateKey"]);
                    Assert.Equal(2, (int) transformDelta["Version"]);
                    Assert.True(DateIsNearCurrent((DateTime) transformDelta["UpdateDate"]));
                    Assert.True(DateIsNearCurrent((DateTime) transformDelta["CreateDate"]));
                    Assert.True((bool) transformDelta["IsCurrent"]);
                }
                
                else if((char) transformDelta["Operation"] == 'U')
                {
                    Assert.True(((DateTime) transformDelta["CreateDate"]) >= createDateMin && ((DateTime) transformDelta["CreateDate"]) <= createDateMax);
                    Assert.Equal(1, (int) transformDelta["Version"]);
                    Assert.False((bool)transformDelta["IsCurrent"]);
                    rowsUpdated++;
                }
                
                else
                {
                    Assert.True(false); 
                }
                count++;
            }

            Assert.Equal(3, rowsCreated);
            Assert.Equal(3, rowsUpdated);
            Assert.Equal(3, transformDelta.TotalRowsPreserved);
            Assert.Equal(6, count);

            //run the delta again.  this should ignore all 10 records.
            transformDelta.SetRowNumber(0);

            // write result to a memory table
            targetWriter = new TransformWriterTarget(memoryConnection, table );
            await targetWriter.WriteRecordsAsync(transformDelta, null, CancellationToken.None);

//            writer = new TransformWriterTarget(memoryConnection, target.CacheTable);
//            await writer.WriteRecordsAsync(transformDelta, CancellationToken.None);
//            
            target = memoryConnection.GetTransformReader(table);
            transformDelta = new TransformDelta(source, target, EUpdateStrategy.AppendUpdatePreserve, surrrogateKey, false);
            await transformDelta.Open(0, null, CancellationToken.None);

            count = 0;
            while (await transformDelta.ReadAsync())
            {
                count++;
            }

            Assert.Equal(10, transformDelta.TotalRowsIgnored);
            Assert.Equal(0, count );
        }


        //[Fact]
        //public async Task RunDeltaTest_updatePreserveDelete()
        //{
        //    ReaderMemory Source = Helpers.CreateUnSortedTestData();

        //    Table targetTable = Source.CacheTable.Copy();
        //    targetTable.AddAuditColumns();

        //    ConnectionMemory Target = new ConnectionMemory(targetTable);

        //    //run an update load with nothing in the target.  
        //    TransformDelta transformDelta = new TransformDelta();
        //    transformDelta.SetDeltaType(targetTable, TransformDelta.EDeltaType.AppendUpdateDeletePreserve, 1, 10);
        //    transformDelta.SetInTransform(Source, Target);

        //    int count = 0;
        //    while (await transformDelta.ReadAsync())
        //    {
        //        Assert.True((char)transformDelta["Operation"] == 'C');
        //        Assert.True((Int64)transformDelta["SurrogateKey"] == count + 1);
        //        Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

        //        count++;
        //    }
        //    Assert.True(count == 10);

        //    //run an update load with source/target matching (however in different sort orders).
        //    Target = Helpers.CreateSortedTestData();

        //    //run an append.  (only difference from reload is no truncate record at start.
        //    transformDelta.SetDeltaType(targetTable, TransformDelta.EDeltaType.AppendUpdateDeletePreserve, 1, 10);
        //    transformDelta.SetInTransform(Source, Target);
        //    transformDelta.Reset();

        //    count = 0;
        //    while (await transformDelta.ReadAsync())
        //    {
        //        count++;
        //    }

        //    Assert.True(transformDelta.RowsIgnored == 10);
        //    Assert.True(count == 0);

        //    //change 4 rows.
        //    Target.CacheTable.Data[0][4] = 100;
        //    Target.CacheTable.Data[5][4] = 200;
        //    Target.CacheTable.Data[9][4] = 300;

        //    //add a duplicate in the source
        //    Target.CacheTable.AddRow(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1 });

        //    transformDelta.Reset();

        //    count = 0;
        //    while (await transformDelta.ReadAsync())
        //    {
        //        count++;
        //    }

        //    Assert.True(transformDelta.RowsInserted == 3);
        //    Assert.True(transformDelta.RowsPreserved == 3);
        //    Assert.True(count == 6);

        //    //delete rows from the target, which should trigger two creates.
        //    Target.CacheTable.Data.RemoveAt(1);
        //    Target.CacheTable.Data.RemoveAt(7);

        //    transformDelta.Reset();

        //    count = 0;
        //    while (await transformDelta.ReadAsync())
        //    {
        //        count++;
        //    }

        //    Assert.True(transformDelta.RowsInserted == 5);
        //    Assert.True(transformDelta.RowsPreserved == 3);
        //    Assert.True(count == 8);

        //    //delete rows from the source, which should not cause any change as delete detection is not on.
        //    Source.CacheTable.Data.RemoveAt(9);
        //    Source.CacheTable.Data.RemoveAt(0); //this is the row that was updated, so update now = 2

        //    transformDelta.Reset();

        //    count = 0;
        //    while (await transformDelta.ReadAsync())
        //    {
        //        count++;
        //    }

        //    Assert.True(transformDelta.RowsInserted == 3);
        //    Assert.True(transformDelta.RowsPreserved == 4);
        //    Assert.True(count == 7);

        //}

        [Theory]
        [InlineData(100000, EUpdateStrategy.Append)]
        [InlineData(100000, EUpdateStrategy.AppendUpdate)]
        [InlineData(100000, EUpdateStrategy.AppendUpdateDelete)]
        [InlineData(100000, EUpdateStrategy.AppendUpdateDeletePreserve)]
        [InlineData(100000, EUpdateStrategy.AppendUpdatePreserve)]
        [InlineData(100000, EUpdateStrategy.Reload)]
        public async Task TransformDeltaPerformance(int rows, EUpdateStrategy updateStrategy)
        {
            var source = Helpers.CreateLargeTable(rows);

            var targetTable = source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            Transform target = new ReaderMemory(targetTable);

            var transformDelta = new TransformDelta(source, target, updateStrategy, 1, false);
            await transformDelta.Open(0, null, CancellationToken.None);

            var count = 0;
            while (await transformDelta.ReadAsync())
            {
                count++;
            }

            //appendupdate and appenddelete will merge all rows in to one.  
            if (updateStrategy == EUpdateStrategy.AppendUpdate ||
                updateStrategy == EUpdateStrategy.AppendUpdateDelete)
                Assert.True(count == 1);
            //reload has one extract row which is the truncate row.
            else if (updateStrategy == EUpdateStrategy.Reload)
                Assert.True(count == rows + 1);
            else
                Assert.True(count == rows);

            Trace.WriteLine(transformDelta.PerformanceDetails());

            ////write result to a memory table
            //ConnectionMemory memoryConnection = new ConnectionMemory();
            //TransformWriter writer = new TransformWriter();
            //writer.WriteAllRecords(transformDelta, Target.CacheTable, memoryConnection, null, null).Wait();
            //Target = memoryConnection.ExecuteReader(Target.CacheTable, null).Result.Value;

            //Assert.True(transformDelta.RowsCreated == 100000);
        }

    }
}