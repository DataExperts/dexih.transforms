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
            ReaderMemory Source = Helpers.CreateSortedTestData();

            Table targetTable = Source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            ReaderMemory Target = new ReaderMemory(targetTable);

            //run a reload.  
            TransformDelta transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.Reload, 0, 10);
            transformDelta.Read();
            Assert.True((char)transformDelta["Operation"] == 'T');

            int count = 0;
            while (transformDelta.Read()) {
                Assert.True((char)transformDelta["Operation"] == 'C');
                Assert.True((Int64)transformDelta["SurrogateKey"] == count+1);
                Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

                count++;
            }

            Assert.True(count == 10);

            //run an append.  (only difference from reload is no truncate record at start.
            transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.Append, 0, 20);
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
        public void RunDeltaTest_update()
        {
            Task.Run(async () =>
            {
                ReaderMemory Source = Helpers.CreateUnSortedTestData();

                Table targetTable = Source.CacheTable.Copy();
                targetTable.AddAuditColumns();

                Transform Target = new ReaderMemory(targetTable);

                //run an update load with nothing in the target, which will result in 10 rows created.
                TransformDelta transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdate, 0, 10);
                transformDelta.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);

                int count = 0;
                while (transformDelta.Read())
                {
                    Assert.True((char)transformDelta["Operation"] == 'C');
                    Assert.True((Int64)transformDelta["SurrogateKey"] == count + 1);
                    Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

                    count++;
                }
                Assert.Equal(10, count);

                transformDelta.SetRowNumber(0);

                //write result to a memory table
                ConnectionMemory memoryConnection = new ConnectionMemory();
                TransformWriter writer = new TransformWriter();
                TransformWriterResult result = new TransformWriterResult();
                await writer.WriteAllRecords(result, transformDelta, Target.CacheTable, memoryConnection, null, null, CancellationToken.None);
                Target = new ReaderMemory(Target.CacheTable, null);

                //Set the target pointer back to the start and rerun.  Now 10 rows should be ignored.
                Source.SetRowNumber(0);
                Target.SetRowNumber(0);

                //run an append.  (only difference from reload is no truncate record at start.
                transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdate, 0, 10);

                count = 0;
                while (transformDelta.Read())
                {
                    count++;
                }

                Assert.True(transformDelta.TotalRowsIgnored == 10);
                Assert.True(count == 0);

                //change 3 rows. (first, middle, last)
                Target.CacheTable.Data[0][4] = 100;
                Target.CacheTable.Data[5][4] = 200;
                Target.CacheTable.Data[9][4] = 300;

                //add a duplicate in the source
                object[] row = new object[Target.CacheTable.Columns.Count];
                Target.CacheTable.Data[9].CopyTo(row, 0);
                Target.CacheTable.Data.Add(row);

                transformDelta.Reset();

                count = 0;
                while (transformDelta.Read())
                {
                    count++;
                    Assert.True((char)transformDelta["Operation"] == 'U');
                }

                Assert.True(count == 3);

                //delete rows from the target, which should trigger two creates.
                Target.CacheTable.Data.RemoveAt(1);
                Target.CacheTable.Data.RemoveAt(7);

                transformDelta.Reset();

                count = 0;
                int rowsCreated = 0;
                int rowsUpdated = 0;
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
                Source.CacheTable.Data.RemoveAt(9);
                Source.CacheTable.Data.RemoveAt(0); //this is the row that was updated, so update now = 2

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
            });
        }

        [Fact]
        public void RunDeltaTest_updatePreserve()
        {
            Task.Run(async () =>
           {
               ReaderMemory Source = Helpers.CreateUnSortedTestData();

               Table targetTable = Source.CacheTable.Copy();
               targetTable.AddAuditColumns();

               long SurrrogateKey = 0;

               Transform Target = new ReaderMemory(targetTable);

                //run an update load with nothing in the target.  
                TransformDelta transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve, SurrrogateKey, 10);
               transformDelta.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);

               int count = 0;
               while (transformDelta.Read())
               {
                   Assert.True((char)transformDelta["Operation"] == 'C');
                   Assert.True((Int64)transformDelta["SurrogateKey"] == count + 1);
                   Assert.True((Int32)transformDelta["IntColumn"] == count + 1);

                   count++;
               }
               Assert.True(count == 10);
               SurrrogateKey = transformDelta.SurrogateKey;

               transformDelta.SetRowNumber(0);

                //write result to a memory table
                ConnectionMemory memoryConnection = new ConnectionMemory();
               TransformWriter writer = new TransformWriter();
               TransformWriterResult result = new TransformWriterResult();
               await writer.WriteAllRecords(result, transformDelta, Target.CacheTable, memoryConnection, null, null, CancellationToken.None);
               Target = new ReaderMemory(Target.CacheTable, null);

                //run an append.  (only difference from reload is no truncate record at start.
                transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdatePreserve, SurrrogateKey, 20);

               count = 0;
               while (transformDelta.Read())
               {
                   count++;
               }

                //change 3 rows. (first, middle, last)
                Target.CacheTable.Data[0][4] = 100;
               Target.CacheTable.Data[5][4] = 200;
               Target.CacheTable.Data[9][4] = 300;

                //add a duplicate in the source
                object[] row = new object[Target.CacheTable.Columns.Count];
               Target.CacheTable.Data[9].CopyTo(row, 0);
               Target.CacheTable.Data.Add(row);

               transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdatePreserve, SurrrogateKey, 30);
               transformDelta.SetCacheMethod(Transform.ECacheMethod.PreLoadCache);

               count = 0;
               int rowsCreated = 0;
               while (transformDelta.Read())
               {
                   rowsCreated += (char)transformDelta["Operation"] == 'C' ? 1 : 0;
                   count++;
               }

               Assert.True(rowsCreated == 3);
               Assert.True(transformDelta.TotalRowsPreserved == 4);
               Assert.True(count == 6);

                //run the delta again.  this should ignore all 10 records.
                transformDelta.SetRowNumber(0);
               result = new TransformWriterResult();
               await writer.WriteAllRecords(result, transformDelta, Target.CacheTable, memoryConnection, null, null, CancellationToken.None);
               Target = new ReaderMemory(Target.CacheTable, null);
               transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdatePreserve, SurrrogateKey, 40);

               count = 0;
               while (transformDelta.Read())
               {
                   count++;
               }

               Assert.True(transformDelta.TotalRowsIgnored == 10);
               Assert.True(count == 0);
           });

        }


        //[Fact]
        //public void RunDeltaTest_updatePreserveDelete()
        //{
        //    ConnectionMemory Source = Helpers.CreateUnSortedTestData();

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

        [Fact]
        public void DeltaLargeTable()
        {
            ReaderMemory Source = Helpers.CreateLargeTable(100000);

            Table targetTable = Source.CacheTable.Copy();
            targetTable.AddAuditColumns();

            Transform Target = new ReaderMemory(targetTable);

            TransformDelta transformDelta = new TransformDelta(Source, Target, TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve, 1, 10);

            int count = 0;
            while(transformDelta.Read())
            {
                count++;
            }
            Assert.True(count == 100000);

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
