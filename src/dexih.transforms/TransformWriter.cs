using dexih.functions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class TransformWriter
    {
        /// <summary>
        /// Indicates the rows buffer per commit.  
        /// </summary>
        public virtual int CommitSize { get; protected set; } = 10000;

        private TableCache CreateRows;
        private TableCache UpdateRows;
        private TableCache DeleteRows;
        private TableCache RejectRows;


        private bool WriteOpen = false;
        private int OperationColumnIndex; //the index of the operation in the source data.

        private Task<ReturnValue<int>> CreateRecordsTask; //task is use to allow writes to run asych with other processing.
        private Task<ReturnValue<int>> UpdateRecordsTask; //task is use to allow writes to run asych with other processing.
        private Task<ReturnValue<int>> DeleteRecordsTask; //task is use to allow writes to run asych with other processing.
        private Task<ReturnValue<int>> RejectRecordsTask; //task is use to allow writes to run asych with other processing.

        private Transform InTransform;
        private Table TargetTable;
        private Table RejectTable;

        private Connection TargetConnection;
        private Connection RejectConnection;

        private CancellationToken CancelToken;

        private InsertQuery TargetInsertQuery;
        private UpdateQuery TargetUpdateQuery;
        private DeleteQuery TargetDeleteQuery;
        private InsertQuery RejectInsertQuery;

        public Stopwatch WriteDataTimer;
        public Stopwatch ProcessingDataTimer;
        public Stopwatch TestDataTimer;

        private List<int> _fieldOrdinals;

        /// <summary>
        /// Writes all record from the inTransform to the target table and reject table.
        /// </summary>
        /// <param name="inTransform">Transform to read data from</param>
        /// <param name="tableName">Target table name</param>
        /// <param name="connection">Target to write data to</param>
        /// <param name="rejecteTableName">Reject table name</param>
        /// <param name="rejectConnection">Reject connection (if null will use connection)</param>
        /// <returns></returns>
        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult WriterResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, CancellationToken cancelToken)
        {
            try
            {
                CancelToken = cancelToken;
                TargetConnection = targetConnection;
                RejectConnection = rejectConnection;
                //WriterResult = new TransformWriterResult();

                WriterResult.RunStatus = TransformWriterResult.ERunStatus.Started;

                TargetTable = targetTable;
                RejectTable = rejectTable;

                InTransform = inTransform;

                var returnValue = await WriteStart(InTransform);

                if (returnValue.Success == false)
                {
                    WriterResult.RunStatus = TransformWriterResult.ERunStatus.Abended;
                    WriterResult.Message = returnValue.Message;

                    return returnValue;
                }

                bool firstRead = true;
                Task<ReturnValue> writeTask = null;

                while (await inTransform.ReadAsync(cancelToken))
                {
                    if (firstRead)
                    {
                        WriterResult.RunStatus = TransformWriterResult.ERunStatus.Running;
                        firstRead = false;
                    }

                    if (writeTask != null)
                        await writeTask;

                    writeTask = WriteRecord(WriterResult, inTransform);

                    if (returnValue.Success == false)
                    {
                        WriterResult.RunStatus = TransformWriterResult.ERunStatus.Abended;
                        WriterResult.Message = returnValue.Message;
                        return new ReturnValue(false);
                    }

                    if (cancelToken.IsCancellationRequested)
                    {
                        WriterResult.RunStatus = TransformWriterResult.ERunStatus.Cancelled;
                        return new ReturnValue(false);
                    }
                }

                WriteDataTimer.Stop();

                returnValue = await WriteFinish(WriterResult, inTransform);
                if (returnValue.Success == false)
                {
                    WriterResult.RunStatus = TransformWriterResult.ERunStatus.Abended;
                    WriterResult.Message = returnValue.Message;
                    return new ReturnValue(false, returnValue.Message, null);
                }

                WriterResult.RunStatus = TransformWriterResult.ERunStatus.Finished;

                return new ReturnValue(true);
            }
            catch(Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when attempting to run the transform: " + ex.Message, ex);
            }
        }

        public async Task<ReturnValue> WriteStart(Transform inTransform)
        {

            if (WriteOpen == true)
                return new ReturnValue(false, "Write cannot start, as a previous operation is still running.  Run the WriteFinish command to reset.", null);

            var returnValue = await InTransform.Open(null); 
            if (!returnValue.Success)
                return returnValue;

            OperationColumnIndex = InTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);

            CreateRows = new TableCache();
            UpdateRows = new TableCache();
            DeleteRows = new TableCache();
            RejectRows = new TableCache();

            //create template queries, with the values set to paramaters (i.e. @param1, @param2)
            TargetInsertQuery = new InsertQuery(TargetTable.TableName, TargetTable.Columns.Select(c => new QueryColumn(c.ColumnName, c.DataType, "@param" + TargetTable.GetOrdinal(c.ColumnName).ToString())).ToList());

            TargetUpdateQuery = new UpdateQuery(
                TargetTable.TableName,
                TargetTable.Columns.Where(c=> c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c.ColumnName, c.DataType, "@param" + TargetTable.GetOrdinal(c.ColumnName).ToString())).ToList(),
                TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c=> new Filter(c.ColumnName, Filter.ECompare.IsEqual, "@surrogateKey")).ToList()
                );

            TargetDeleteQuery = new DeleteQuery(TargetTable.TableName, TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c.ColumnName, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            if (RejectTable != null)
            {
                RejectInsertQuery = new InsertQuery(RejectTable.TableName, RejectTable.Columns.Select(c => new QueryColumn(c.ColumnName, c.DataType, "@param" + RejectTable.GetOrdinal(c.ColumnName).ToString())).ToList());
                returnValue = await TargetConnection.CreateTable(RejectTable, false);
            }

            //if the table doesn't exist, create it.  
            returnValue = await TargetConnection.CreateTable(TargetTable, false);

            returnValue = await TargetConnection.DataWriterStart(TargetTable);

            WriteDataTimer = new Stopwatch();
            ProcessingDataTimer = Stopwatch.StartNew();

            TestDataTimer = new Stopwatch();

            //await InTransform.Open();

            _fieldOrdinals = new List<int>();
            for (int i = 0; i < TargetTable.Columns.Count; i++)
            {
                _fieldOrdinals.Add(inTransform.GetOrdinal(TargetTable.Columns[i].ColumnName));
            }

            WriteOpen = true;

            return new ReturnValue(true);
        }

        public async Task<ReturnValue> WriteRecord(TransformWriterResult writerResult, Transform reader)
        {
            if (WriteOpen == false)
                return new ReturnValue(false, "Cannot write records as the WriteStart has not been called.", null);

            //split the operation field (if it exists) and create copy of the row.
            char operation;

            //determine the type of operation (create, update, delete, reject)
            if (OperationColumnIndex == -1)
            {
                operation = 'C';
            }
            else
            {
                operation = (char)reader[OperationColumnIndex];
            }

            Table table;
            if (operation == 'R')
            {
                table = RejectTable;
                if(RejectTable == null)
                {
                    writerResult.Message = "Records have been rejected, however there is no reject table.";
                    writerResult.RunStatus = TransformWriterResult.ERunStatus.RunningErrors;
                    return new ReturnValue(false, writerResult.Message, null);
                }
            }
            else
                table = TargetTable;

            object[] row = new object[table.Columns.Count];

            for (int i = 0; i < table.Columns.Count; i++)
            {
                //int ordinal = reader.GetOrdinal(table.Columns[i].ColumnName);
                int ordinal = _fieldOrdinals[i];
                if (ordinal >= 0)
                    row[i] = reader[ordinal];
            }

            switch (operation)
            {
                case 'C':
                    CreateRows.Add(row);
                    writerResult.IncrementRowsCreated();
                    if (CreateRows.Count >= CommitSize)
                        return await doCreates();
                    break;
                case 'U':
                    UpdateRows.Add(row);
                    writerResult.IncrementRowsUpdated();
                    if (UpdateRows.Count >= CommitSize)
                        return await doUpdate();
                    break;
                case 'D':
                    DeleteRows.Add(row);
                    writerResult.IncrementRowsDeleted();
                    if (DeleteRows.Count >= CommitSize)
                        return await doDelete();
                    break;
                case 'R':
                    RejectRows.Add(row);
                    if (RejectRows.Count >= CommitSize)
                        return await doReject();
                    break;
                case 'T':
                    var truncateResult = await TargetConnection.TruncateTable(TargetTable, CancelToken);
                    if (!truncateResult.Success)
                        return truncateResult;
                    break;
            }

            return new ReturnValue(true);
        }

        public async Task<ReturnValue> WriteFinish(TransformWriterResult writerResult, Transform reader)
        {
            WriteOpen = false;

            if (CreateRows.Count > 0)
            {
                var returnValue = await doCreates();
                if (returnValue.Success == false)
                    return returnValue;
            }

            if (UpdateRows.Count > 0)
            {
                var returnValue = await doUpdate();
                if (returnValue.Success == false)
                    return returnValue;
            }

            if (DeleteRows.Count > 0)
            {
                var returnValue = await doDelete();
                if (returnValue.Success == false)
                    return returnValue;
            }

            if (RejectRows.Count > 0)
            {
                var returnValue = await doReject();
                if (returnValue.Success == false)
                    return returnValue;
            }

            if (CreateRecordsTask != null && !(await CreateRecordsTask).Success)
                return CreateRecordsTask.Result;

            if (UpdateRecordsTask != null && !(await UpdateRecordsTask).Success)
                return UpdateRecordsTask.Result;

            if (DeleteRecordsTask != null && !(await DeleteRecordsTask).Success)
                return DeleteRecordsTask.Result;

            if (RejectRecordsTask != null && !(await RejectRecordsTask).Success)
                return RejectRecordsTask.Result;

            //update the statistics.
            writerResult.RowsFiltered = reader.TotalRowsFiltered;
            writerResult.RowsSorted = reader.TotalRowsSorted;
            writerResult.RowsRejected = reader.TotalRowsRejected;
            writerResult.RowsPreserved = reader.TotalRowsPreserved;
            writerResult.RowsIgnored = reader.TotalRowsIgnored;
            writerResult.RowsReadPrimary = reader.TotalRowsReadPrimary;
            writerResult.RowsReadReference = reader.TotalRowsReadReference;

            //calculate the throughput figures
            long rowsWritten = writerResult.RowsTotal - writerResult.RowsIgnored;
            if (WriteDataTimer.ElapsedMilliseconds == 0)
                writerResult.WriteThroughput = 0;
            else
                writerResult.WriteThroughput = (Decimal)rowsWritten / ((decimal)WriteDataTimer.ElapsedMilliseconds / 1000);

            //get read times for base connections.
            int recordsRead = 0; long elapsedMilliseconds = 0;
            reader.ReadThroughput(ref recordsRead, ref elapsedMilliseconds);
            if (elapsedMilliseconds == 0)
                writerResult.ReadThroughput = 0;
            else
                writerResult.ReadThroughput = (Decimal)recordsRead / ((decimal)elapsedMilliseconds / 1000);

            //calculate the overall processing througput 
            ProcessingDataTimer.Stop();
            if (ProcessingDataTimer.ElapsedMilliseconds == 0)
                writerResult.ProcessingThroughput = 0;
            else
                writerResult.ProcessingThroughput = (Decimal)writerResult.RowsTotal / ((decimal)ProcessingDataTimer.ElapsedMilliseconds / 1000);

            writerResult.EndTime = DateTime.Now;
            writerResult.MaxIncrementalValue = reader.GetMaxIncrementalValue();

            reader.Dispose();

            var returnValue2 = await TargetConnection.DataWriterFinish(TargetTable);

            return new ReturnValue(true);
        }
        private async Task<ReturnValue> doCreates()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                if (!result.Success)
                    return result;
            }

            Table createTable = new Table(TargetTable.TableName, TargetTable.Columns, CreateRows);
            var createReader = new ReaderMemory(createTable);

            WriteDataTimer.Start();
            CreateRecordsTask = TargetConnection.ExecuteInsertBulk(TargetTable, createReader, CancelToken);  //this has no await to ensure processing continues.
            WriteDataTimer.Stop();

            CreateRows = new TableCache();
            return new ReturnValue(true);
        }

        private async Task<ReturnValue> doUpdate()
        {
            //update must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                if (!result.Success)
                    return result;
            }

            if (UpdateRecordsTask != null)
            {
                var result = await UpdateRecordsTask;
                if (!result.Success)
                    return result;
            }

            List<UpdateQuery> updateQueries = new List<UpdateQuery>();
            foreach(object[] row in UpdateRows)
            {
                UpdateQuery updateQuery = new UpdateQuery(
                TargetTable.TableName,
                TargetTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c.ColumnName, c.DataType, row[TargetTable.GetOrdinal(c.ColumnName)])).ToList(),
                TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c.ColumnName, Filter.ECompare.IsEqual, row[TargetTable.GetOrdinal(c.ColumnName)])).ToList()
                );

                updateQueries.Add(updateQuery);
            }

            WriteDataTimer.Start();
            UpdateRecordsTask = TargetConnection.ExecuteUpdate(TargetTable, updateQueries, CancelToken);  //this has no await to ensure processing continues.
            WriteDataTimer.Stop();

            UpdateRows = new TableCache();

            return new ReturnValue(true);
        }

        private async Task<ReturnValue> doDelete()
        {
            //delete must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                if (!result.Success)
                    return result;
            }

            if (UpdateRecordsTask != null)
            {
                var result = await UpdateRecordsTask;
                if (!result.Success)
                    return result;
            }

            if (DeleteRecordsTask != null)
            {
                var result = await DeleteRecordsTask;
                if (!result.Success)
                    return result;
            }

            TargetDeleteQuery = new DeleteQuery(TargetTable.TableName, TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c.ColumnName, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            List<DeleteQuery> deleteQueries = new List<DeleteQuery>();
            foreach (object[] row in DeleteRows)
            {
                DeleteQuery deleteQuery = new DeleteQuery(
                TargetTable.TableName,
                TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c.ColumnName, Filter.ECompare.IsEqual, row[TargetTable.GetOrdinal(c.ColumnName)])).ToList()
                );

                deleteQueries.Add(deleteQuery);
            }

            WriteDataTimer.Start();
            DeleteRecordsTask = TargetConnection.ExecuteDelete(TargetTable, deleteQueries, CancelToken);  //this has no await to ensure processing continues.
            WriteDataTimer.Stop();

            DeleteRows = new TableCache();

            return new ReturnValue(true);
        }

        private async Task<ReturnValue> doReject()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (RejectRecordsTask != null)
            {
                var result = await RejectRecordsTask;
                if (!result.Success)
                    return result;
            }

            Table createTable = new Table(RejectTable.TableName, RejectTable.Columns, RejectRows);

            var createReader = new ReaderMemory(createTable);

            WriteDataTimer.Start();
            RejectRecordsTask = TargetConnection.ExecuteInsertBulk(createTable, createReader, CancelToken);  //this has no await to ensure processing continues.
            WriteDataTimer.Stop();

            return new ReturnValue(true);
        }

    }
}
