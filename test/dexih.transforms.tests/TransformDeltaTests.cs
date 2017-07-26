using dexih.functions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformDeltaTests
    {

        [Fact]
        public void RunDeltaTest_reload()
        {
            var source = Helpers.CreateSortedTestData();

            var targetTable = source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            var target = new ReaderMemory(targetTable);

            //run a reload.  
            var transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.Reload, 0, false);
            transformDelta.Read();
            Assert.True((char)transformDelta["Operation"] == 'T');

            var count = 0;
            while (transformDelta.Read()) {
                Assert.True((char)transformDelta["Operation"] == 'C');
                Assert.True((Int64)transformDelta["SurrogateKey"] == count+1);
                Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

                count++;
            }

            Assert.True(count == 10);

            //run an append.  (only difference from reload is no truncate record at start.
            transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.Append, 0, false);
            transformDelta.Reset();

            count = 0;
            while (transformDelta.Read())
            {
                Assert.True((char)transformDelta["Operation"] == 'C');
                Assert.True((Int64)transformDelta["SurrogateKey"] == count + 1);
                Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

                count++;
            }

            Assert.True(count == 10);

        }

        [Fact]
        public async Task RunDeltaTest_update()
        {
                var source = Helpers.CreateUnSortedTestData();

                var targetTable = source.CacheTable.Copy();
                targetTable.AddAuditColumns();

                Transform target = new ReaderMemory(targetTable);

                //run an update load with nothing in the target, which will result in 10 rows created.
                var transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.AppendUpdate, 0, false);
                transformDelta.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);

                var count = 0;
                while (await transformDelta.ReadAsync())
                {
                    Assert.True((char)transformDelta["Operation"] == 'C');
                    Assert.True((long)transformDelta["SurrogateKey"] == count + 1);
                    Assert.True((int)transformDelta["IntColumn"] == count + 1);

                    count++;
                }
                Assert.Equal(10, count);

                transformDelta.SetRowNumber(0);

                //write result to a memory table
                var memoryConnection = new ConnectionMemory();
                var writer = new TransformWriter();
                var result = new TransformWriterResult(0, 10, "DataLink", 1, 2, "Test", 1, "Source", 2, "Target", null, null, TransformWriterResult.ETriggerMethod.Manual, "Test");
                var writeResult = await writer.WriteAllRecords(result, transformDelta, target.CacheTable, memoryConnection, CancellationToken.None);
                Assert.True(writeResult.Success, writeResult.Message);
            
                target = new ReaderMemory(target.CacheTable, null);

                //Set the target pointer back to the start and rerun.  Now 10 rows should be ignored.
                source.SetRowNumber(0);
                target.SetRowNumber(0);

                //run an append.  (only difference from reload is no truncate record at start.
                transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.AppendUpdate, 0, false);

                count = 0;
                while (transformDelta.Read())
                {
                    count++;
                }

                Assert.True(transformDelta.TotalRowsIgnored == 10);
                Assert.True(count == 0);

                //change 3 rows. (first, middle, last)
                target.CacheTable.Data[0][4] = 100;
                target.CacheTable.Data[5][4] = 200;
                target.CacheTable.Data[9][4] = 300;

                //add a duplicate in the source
                var row = new object[target.CacheTable.Columns.Count];
                target.CacheTable.Data[9].CopyTo(row, 0);
                target.CacheTable.Data.Add(row);

                transformDelta.Reset();

                count = 0;
                while (transformDelta.Read())
                {
                    count++;
                    Assert.True((char)transformDelta["Operation"] == 'U');
                }

                Assert.True(count == 3);

                //delete rows from the target, which should trigger two creates.
                target.CacheTable.Data.RemoveAt(1);
                target.CacheTable.Data.RemoveAt(7);

                transformDelta.Reset();

                count = 0;
                var rowsCreated = 0;
                var rowsUpdated = 0;
                while (transformDelta.Read())
                {
                    rowsCreated += (char)transformDelta["Operation"] == 'C' ? 1 : 0;
                    rowsUpdated += (char)transformDelta["Operation"] == 'U' ? 1 : 0;
                    count++;
                }

                Assert.True(rowsCreated == 2);
                Assert.True(rowsUpdated == 3);
                Assert.True(count == 5);

                //delete rows from the source, which should not cause any change as delete detection is not on.
                source.CacheTable.Data.RemoveAt(9);
                source.CacheTable.Data.RemoveAt(0); //this is the row that was updated, so update now = 2

                transformDelta.Reset();

                count = 0;
                rowsCreated = 0;
                rowsUpdated = 0;
                while (transformDelta.Read())
                {
                    rowsCreated += (char)transformDelta["Operation"] == 'C' ? 1 : 0;
                    rowsUpdated += (char)transformDelta["Operation"] == 'U' ? 1 : 0;
                    count++;
                }

                Assert.True(rowsCreated == 1);
                Assert.True(rowsUpdated == 2);
                Assert.True(count == 3);
        }

        [Fact]
        public async Task RunDeltaTest_updatePreserve()
        {
               var source = Helpers.CreateUnSortedTestData();

               var targetTable = source.CacheTable.Copy();
               targetTable.AddAuditColumns();

               long surrrogateKey = 0;

               Transform target = new ReaderMemory(targetTable);

                //run an update load with nothing in the target.  
                var transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve, surrrogateKey, false);
               transformDelta.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);

               var count = 0;
               while (transformDelta.Read())
               {
                   Assert.True((char)transformDelta["Operation"] == 'C');
                   Assert.True((Int64)transformDelta["SurrogateKey"] == count + 1);
                   Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

                   count++;
               }
               Assert.True(count == 10);
               surrrogateKey = transformDelta.SurrogateKey;

               transformDelta.SetRowNumber(0);

                //write result to a memory table
                var memoryConnection = new ConnectionMemory();
               var writer = new TransformWriter();
               var result = new TransformWriterResult(0, 1, "DataLink", 1, 2, "Test", 1, "Source", 2, "Target", null, null, TransformWriterResult.ETriggerMethod.Manual, "Test");
               await writer.WriteAllRecords(result, transformDelta, target.CacheTable, memoryConnection, CancellationToken.None);
               target = new ReaderMemory(target.CacheTable, null);

                //run an append.  (only difference from reload is no truncate record at start.
                transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.AppendUpdatePreserve, surrrogateKey, false);

               count = 0;
               while (transformDelta.Read())
               {
                   count++;
               }

               //change 3 rows. (first, middle, last)
               target.CacheTable.Data[0][4] = 100;
               target.CacheTable.Data[5][4] = 200;
               target.CacheTable.Data[9][4] = 300;

                //add a duplicate in the source
                var row = new object[target.CacheTable.Columns.Count];
               target.CacheTable.Data[9].CopyTo(row, 0);
               target.CacheTable.Data.Add(row);

               transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.AppendUpdatePreserve, surrrogateKey, false);
               transformDelta.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);

               count = 0;
               var rowsCreated = 0;
               while (transformDelta.Read())
               {
                   rowsCreated += (char)transformDelta["Operation"] == 'C' ? 1 : 0;
                   count++;
               }

               Assert.True(rowsCreated == 3);
               Assert.True(transformDelta.TotalRowsPreserved == 3);
               Assert.True(count == 6);

                //run the delta again.  this should ignore all 10 records.
                transformDelta.SetRowNumber(0);
               result = new TransformWriterResult(0, 1, "DataLink", 30, 40, "Test", 1, "Source", 2, "Target", null, null, TransformWriterResult.ETriggerMethod.Manual, "Test");
               await writer.WriteAllRecords(result, transformDelta, target.CacheTable, memoryConnection,CancellationToken.None);
               target = new ReaderMemory(target.CacheTable, null);
               transformDelta = new TransformDelta(source, target, TransformDelta.EUpdateStrategy.AppendUpdatePreserve, surrrogateKey, false);

               count = 0;
               while (transformDelta.Read())
               {
                   count++;
               }

               Assert.True(transformDelta.TotalRowsIgnored == 10);
               Assert.True(count == 0);

        }


        //[Fact]
        //public void RunDeltaTest_updatePreserveDelete()
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
        //    while (transformDelta.Read())
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
        //    while (transformDelta.Read())
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
        //    Target.CacheTable.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1 });

        //    transformDelta.Reset();

        //    count = 0;
        //    while (transformDelta.Read())
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
        //    while (transformDelta.Read())
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
        //    while (transformDelta.Read())
        //    {
        //        count++;
        //    }

        //    Assert.True(transformDelta.RowsInserted == 3);
        //    Assert.True(transformDelta.RowsPreserved == 4);
        //    Assert.True(count == 7);

        //}

        [Theory]
        [InlineData(100000, TransformDelta.EUpdateStrategy.Append)]
        [InlineData(100000, TransformDelta.EUpdateStrategy.AppendUpdate)]
        [InlineData(100000, TransformDelta.EUpdateStrategy.AppendUpdateDelete)]
        [InlineData(100000, TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve)]
        [InlineData(100000, TransformDelta.EUpdateStrategy.AppendUpdatePreserve)]
        [InlineData(100000, TransformDelta.EUpdateStrategy.Reload)]
        public async void TransformDeltaPerformance(int rows, TransformDelta.EUpdateStrategy updateStrategy)
        {
            var source = Helpers.CreateLargeTable(rows);

            var targetTable = source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            Transform target = new ReaderMemory(targetTable);

            var transformDelta = new TransformDelta(source, target, updateStrategy, 1, false);

            var count = 0;
            while(await transformDelta.ReadAsync())
            {
                count++;
            }

            //appendupdate and appenddelete will merge all rows in to one.  
            if(updateStrategy == TransformDelta.EUpdateStrategy.AppendUpdate || updateStrategy == TransformDelta.EUpdateStrategy.AppendUpdateDelete)
                Assert.True(count == 1);
            //reload has one extract row which is the truncate row.
            else if (updateStrategy == TransformDelta.EUpdateStrategy.Reload)
                Assert.True(count == rows+1);
            else
                Assert.True(count == rows);

            WriteTransformPerformance(transformDelta);

            ////write result to a memory table
            //ConnectionMemory memoryConnection = new ConnectionMemory();
            //TransformWriter writer = new TransformWriter();
            //writer.WriteAllRecords(transformDelta, Target.CacheTable, memoryConnection, null, null).Wait();
            //Target = memoryConnection.ExecuteReader(Target.CacheTable, null).Result.Value;

            //Assert.True(transformDelta.RowsCreated == 100000);
        }

        void WriteTransformPerformance(Transform transform)
        {
            if (transform != null)
            {
                Trace.WriteLine(transform.Details() + " performance: " + transform.TransformTimerTicks().ToString());
                WriteTransformPerformance(transform.PrimaryTransform);
                WriteTransformPerformance(transform.ReferenceTransform);
            }
        }
    }
}
