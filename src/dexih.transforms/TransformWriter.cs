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

        private Task<ReturnValue<long>> CreateRecordsTask; //task to allow writes to run async with other processing.
        private Task<ReturnValue<long>> UpdateRecordsTask; //task to allow writes to run async with other processing.
        private Task<ReturnValue<long>> DeleteRecordsTask; //task to allow writes to run async with other processing.
        private Task<ReturnValue<long>> RejectRecordsTask; //task to allow writes to run async with other processing.

        private Transform InTransform;
        private Table TargetTable;
        private Table RejectTable;
        private Table ProfileTable;

        private Connection TargetConnection;
        private Connection RejectConnection;
        private Connection ProfileConnection;

        private CancellationToken CancelToken;

        private InsertQuery TargetInsertQuery;
        private UpdateQuery TargetUpdateQuery;
        private DeleteQuery TargetDeleteQuery;
        private InsertQuery RejectInsertQuery;

        public long WriteDataTicks;

        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;

        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult WriterResult, Transform inTransform, Table targetTable, Connection targetConnection, CancellationToken cancelToken)
        {
            return await WriteAllRecords(WriterResult, inTransform, targetTable, targetConnection, null, null, null, null, cancelToken);
        }

        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult WriterResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, CancellationToken cancelToken)
        {
            return await WriteAllRecords(WriterResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, null, null, cancelToken);
        }

        public async Task<ReturnValue> WriteAllRecords( TransformWriterResult WriterResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Table profileTable, CancellationToken cancelToken)
        {
            return await WriteAllRecords(WriterResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, profileTable, targetConnection, cancelToken);
        }


        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult WriterResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, CancellationToken cancelToken)
        {
            return await WriteAllRecords(WriterResult, inTransform, targetTable, targetConnection, rejectTable, rejectConnection, null, null, cancelToken);
        }

        /// <summary>
        /// Writes all record from the inTransform to the target table and reject table.
        /// </summary>
        /// <param name="inTransform">Transform to read data from</param>
        /// <param name="tableName">Target table name</param>
        /// <param name="connection">Target to write data to</param>
        /// <param name="rejecteTableName">Reject table name</param>
        /// <param name="rejectConnection">Reject connection (if null will use connection)</param>
        /// <param name="profileTableName">Reject table name</param>
        /// <param name="profileConnection">Reject connection (if null will use connection)</param>
        /// <returns></returns>
        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult WriterResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, Table profileTable, Connection profileConnection, CancellationToken cancelToken)
        {
            try
            {
                CancelToken = cancelToken;
                TargetConnection = targetConnection;

                if (rejectConnection == null)
                    RejectConnection = targetConnection;
                else
                    RejectConnection = rejectConnection;

                if (profileConnection == null)
                    ProfileConnection = targetConnection;
                else
                    ProfileConnection = profileConnection;

                var updateResult = await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Started);
                if (!updateResult.Success)
                    return updateResult;

                TargetTable = targetTable;
                RejectTable = rejectTable;
                ProfileTable = profileTable;

                InTransform = inTransform;

                var returnValue = await WriteStart(InTransform, WriterResult);

                if (returnValue.Success == false)
                {
                    await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, returnValue.Message);
                    return returnValue;
                }

                bool firstRead = true;
                Task<ReturnValue> writeTask = null;

                while (await inTransform.ReadAsync(cancelToken))
                {
                    if (firstRead)
                    {
                        var runStatusResult = await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running);
                        if (!runStatusResult.Success)
                            return runStatusResult;
                        firstRead = false;
                    }

                    if (writeTask != null)
                    {
                        var result = await writeTask;
                        if(!result.Success)
                        {
                            await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, returnValue.Message);
                            return returnValue;
                        }
                    }

                    writeTask = WriteRecord(WriterResult, inTransform);

                    if (cancelToken.IsCancellationRequested)
                    {
                        var runStatusResult = await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled);
                        return runStatusResult;
                    }
                }

                returnValue = await WriteFinish(WriterResult, inTransform);
                if (returnValue.Success == false)
                {
                    await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, returnValue.Message);
                    return new ReturnValue(false, returnValue.Message, null);
                }

                if (ProfileTable != null)
                {
                    var profileResults = inTransform.GetProfileResults();
                    if (profileResults != null)
                    {
                        var profileResult = await ProfileConnection.ExecuteInsertBulk(ProfileTable, profileResults, cancelToken);
                        if (!profileResult.Success)
                        {
                            await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, "Failed to save profile results with error: " + profileResult.Message);
                            return profileResult;
                        }
                    }
                }

                var setRunStatusResult = await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished);

                return setRunStatusResult;
            }
            catch(Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when attempting to run the transform: " + ex.Message, ex);
            }
        }

        public async Task<ReturnValue> WriteStart(Transform inTransform, TransformWriterResult writerResult)
        {

            if (WriteOpen == true)
                return new ReturnValue(false, "Write cannot start, as a previous operation is still running.  Run the WriteFinish command to reset.", null);

            var returnValue = await InTransform.Open(writerResult.AuditKey, null); 
            if (!returnValue.Success)
                return returnValue;

            OperationColumnIndex = InTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);

            CreateRows = new TableCache();
            UpdateRows = new TableCache();
            DeleteRows = new TableCache();
            RejectRows = new TableCache();

            //create template queries, with the values set to paramaters (i.e. @param1, @param2)
            TargetInsertQuery = new InsertQuery(TargetTable.TableName, TargetTable.Columns.Select(c => new QueryColumn(new TableColumn(c.ColumnName, c.DataType), "@param" + TargetTable.GetOrdinal(c.ColumnName).ToString())).ToList());

            TargetUpdateQuery = new UpdateQuery(
                TargetTable.TableName,
                TargetTable.Columns.Where(c=> c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, "@param" + TargetTable.GetOrdinal(c.ColumnName).ToString())).ToList(),
                TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c=> new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList()
                );

            TargetDeleteQuery = new DeleteQuery(TargetTable.TableName, TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            if (RejectTable != null)
            {
                RejectInsertQuery = new InsertQuery(RejectTable.TableName, RejectTable.Columns.Select(c => new QueryColumn(c, "@param" + RejectTable.GetOrdinal(c.ColumnName).ToString())).ToList());
                returnValue = await RejectConnection.CreateTable(RejectTable, false);
                //if (!returnValue.Success)
                //    return returnValue;
            }

            //create the profile results table if it doesn't exist.
            if (ProfileTable != null)
            {
                returnValue = await ProfileConnection.CreateTable(ProfileTable, false);
            }

            //if the table doesn't exist, create it.  
            returnValue = await TargetConnection.CreateTable(TargetTable, false);
            returnValue = await TargetConnection.DataWriterStart(TargetTable);

            //if the truncate table flag is set, then truncate the target table.
            if (writerResult.TruncateTarget)
            {
                var truncateResult = await TargetConnection.TruncateTable(TargetTable, CancelToken);
                if (!truncateResult.Success)
                    return truncateResult;
            }

            int columnCount = TargetTable.Columns.Count;
            _fieldOrdinals = new int[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                _fieldOrdinals[i] = inTransform.GetOrdinal(TargetTable.Columns[i].ColumnName);
            }

            if(RejectTable != null)
            {
                columnCount = RejectTable.Columns.Count;
                _rejectFieldOrdinals = new int[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    _rejectFieldOrdinals[i] = inTransform.GetOrdinal(RejectTable.Columns[i].ColumnName);
                }
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
            int[] ordinals = _fieldOrdinals;

            if (operation == 'R')
            {
                table = RejectTable;
                ordinals = _rejectFieldOrdinals;
                if (RejectTable == null)
                {
                    var rejectColumn = reader.GetOrdinal("RejectedReason");
                    string rejectReason = "";
                    if (rejectColumn > 0)
                        rejectReason = reader[rejectColumn].ToString();
                    else
                        rejectReason = "No reject reason found.";

                    var setStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.RunningErrors, "A record was rejected, however there is no reject table.  The message was: " + rejectReason);
                    return setStatusResult;
                }
            }
            else
                table = TargetTable;

            int columnCount = table.Columns.Count;

            object[] row = new object[columnCount];

            for (int i = 0; i < columnCount; i++)
            {
                //int ordinal = reader.GetOrdinal(table.Columns[i].ColumnName);
                int ordinal = ordinals[i];
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

            //write out the remaining rows.
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

            //wait for any write tasks to finish
            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (UpdateRecordsTask != null)
            {
                var result = await UpdateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (DeleteRecordsTask != null)
            {
                var result = await DeleteRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (RejectRecordsTask != null)
            {
                var result = await RejectRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            //update the statistics.
            writerResult.RowsFiltered = reader.TotalRowsFiltered;
            writerResult.RowsSorted = reader.TotalRowsSorted;
            writerResult.RowsRejected = reader.TotalRowsRejected;
            writerResult.RowsPreserved = reader.TotalRowsPreserved;
            writerResult.RowsIgnored = reader.TotalRowsIgnored;
            writerResult.RowsReadPrimary = reader.TotalRowsReadPrimary;
            writerResult.RowsReadReference = reader.TotalRowsReadReference;

            writerResult.PerformanceSummary = reader.PerformanceSummary();

            //calculate the throughput figures
            long rowsWritten = writerResult.RowsTotal - writerResult.RowsIgnored;

            writerResult.WriteTicks = WriteDataTicks;
            writerResult.ReadTicks = reader.ReaderTimerTicks();
            writerResult.ProcessingTicks = reader.ProcessingTimerTicks();

            writerResult.EndTime = DateTime.Now;

            if (writerResult.RowsTotal == 0)
                writerResult.MaxIncrementalValue = writerResult.LastMaxIncrementalValue;
            else
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
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            Table createTable = new Table(TargetTable.TableName, TargetTable.Columns, CreateRows);
            var createReader = new ReaderMemory(createTable);

            CreateRecordsTask = TargetConnection.ExecuteInsertBulk(TargetTable, createReader, CancelToken);  //this has no await to ensure processing continues.

            CreateRows = new TableCache();
            return new ReturnValue(true);
        }

        private async Task<ReturnValue> doUpdate()
        {
            //update must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (UpdateRecordsTask != null)
            {
                var result = await UpdateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            List<UpdateQuery> updateQueries = new List<UpdateQuery>();
            foreach(object[] row in UpdateRows)
            {
                UpdateQuery updateQuery = new UpdateQuery(
                TargetTable.TableName,
                TargetTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, row[TargetTable.GetOrdinal(c.ColumnName)])).ToList(),
                TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[TargetTable.GetOrdinal(c.ColumnName)])).ToList()
                );

                updateQueries.Add(updateQuery);
            }

            UpdateRecordsTask = TargetConnection.ExecuteUpdate(TargetTable, updateQueries, CancelToken);  //this has no await to ensure processing continues.

            UpdateRows = new TableCache();

            return new ReturnValue(true);
        }

        private async Task<ReturnValue> doDelete()
        {
            //delete must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (CreateRecordsTask != null)
            {
                var result = await CreateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (UpdateRecordsTask != null)
            {
                var result = await UpdateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (DeleteRecordsTask != null)
            {
                var result = await DeleteRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            TargetDeleteQuery = new DeleteQuery(TargetTable.TableName, TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            List<DeleteQuery> deleteQueries = new List<DeleteQuery>();
            foreach (object[] row in DeleteRows)
            {
                DeleteQuery deleteQuery = new DeleteQuery(
                TargetTable.TableName,
                TargetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[TargetTable.GetOrdinal(c.ColumnName)])).ToList()
                );

                deleteQueries.Add(deleteQuery);
            }

            DeleteRecordsTask = TargetConnection.ExecuteDelete(TargetTable, deleteQueries, CancelToken);  //this has no await to ensure processing continues.

            DeleteRows = new TableCache();

            return new ReturnValue(true);
        }

        private async Task<ReturnValue> doReject()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (RejectRecordsTask != null)
            {
                var result = await RejectRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            Table createTable = new Table(RejectTable.TableName, RejectTable.Columns, RejectRows);

            var createReader = new ReaderMemory(createTable);

            RejectRecordsTask = TargetConnection.ExecuteInsertBulk(createTable, createReader, CancelToken);  //this has no await to ensure processing continues.

            return new ReturnValue(true);
        }

    }
}
