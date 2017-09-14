using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static dexih.transforms.TransformWriterResult;

namespace dexih.transforms
{
    public class TransformWriter
    {
        /// <summary>
        /// Indicates the rows buffer per commit.  
        /// </summary>
        public virtual int CommitSize { get; protected set; } = 10000;

        private TableCache _createRows;
        private TableCache _updateRows;
        private TableCache _deleteRows;
        private TableCache _rejectRows;


        private bool _writeOpen;
        private int _operationColumnIndex; //the index of the operation in the source data.

        private Task<long> _createRecordsTask; //task to allow writes to run async with other processing.
        private Task<long> _updateRecordsTask; //task to allow writes to run async with other processing.
        private Task<long> _deleteRecordsTask; //task to allow writes to run async with other processing.
        private Task<long> _rejectRecordsTask; //task to allow writes to run async with other processing.

        private Transform _inTransform;
        private Table _targetTable;
        private Table _rejectTable;
        private Table _profileTable;

        private Connection _targetConnection;
        private Connection _rejectConnection;
        private bool rejectTableCreated = false;
        private Connection _profileConnection;

        private CancellationToken _cancelToken;

        public long WriteDataTicks;

        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;


        public Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, CancellationToken cancelToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, null, null, null, null, cancelToken);
        }

        public Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, CancellationToken cancelToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, null, null, cancelToken);
        }

        public Task<bool> WriteAllRecords( TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Table profileTable, CancellationToken cancelToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, profileTable, targetConnection, cancelToken);
        }


        public Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, CancellationToken cancelToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, rejectConnection, null, null, cancelToken);
        }

        /// <summary>
        /// Writes all record from the inTransform to the target table and reject table.
        /// </summary>
        /// <param name="writerResult"></param>
        /// <param name="inTransform">Transform to read data from</param>
        /// <param name="targetConnection"></param>
        /// <param name="rejectTable"></param>
        /// <param name="rejectConnection">Reject connection (if null will use connection)</param>
        /// <param name="profileTable"></param>
        /// <param name="profileConnection">Reject connection (if null will use connection)</param>
        /// <param name="targetTable"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public async Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, Table profileTable, Connection profileConnection, CancellationToken cancelToken)
        {
            _cancelToken = cancelToken;
            _targetConnection = targetConnection;

            if (rejectConnection == null)
                _rejectConnection = targetConnection;
            else
                _rejectConnection = rejectConnection;

            if (profileConnection == null)
                _profileConnection = targetConnection;
            else
                _profileConnection = profileConnection;

            var updateResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Started);
            if (!updateResult)
            {
                return updateResult;
            }

            _targetTable = targetTable;
            _rejectTable = rejectTable;
            _profileTable = profileTable;

            _inTransform = inTransform;

            writerResult.RejectTableName = rejectTable?.Name;

            try
            {
                await WriteStart(_inTransform, writerResult, cancelToken);
            }
            catch (Exception ex)
            {
                var message = $"The transform writer failed to start.  {ex.Message}";
                await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message);
                await _targetConnection.DataWriterError(message, ex);
                return false;
            }

            bool firstRead = true;
            Task writeTask = null;

            while (await inTransform.ReadAsync(cancelToken))
            {
                if (firstRead)
                {
                    var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Running);
                    if (!runStatusResult)
                    {
                        return runStatusResult;
                    }
                    firstRead = false;
                }

                if (writeTask != null)
                {
                    await writeTask;
                    if(writeTask.IsFaulted)
                    {
                        var message = $"The transform writer failed writing data.  {writeTask.Exception?.Message}";
                        await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message);
                        await _targetConnection.DataWriterError(message, writeTask.Exception);
                        return false;
                    }
                }

                writeTask = WriteRecord(writerResult, inTransform);

                if (cancelToken.IsCancellationRequested)
                {
                    var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled);
                    return runStatusResult;
                }
            }

            try
            {
                await WriteFinish(writerResult, inTransform);
            }
            catch (Exception ex)
            {
                var message = $"The transform writer failed to finish.  {ex.Message}";
                await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message);
                await _targetConnection.DataWriterError(message, ex);
                return false;
            }

            if (_profileTable != null)
            {
                var profileResults = inTransform.GetProfileResults();
                if (profileResults != null)
                {
                    await _profileConnection.CreateTable(_profileTable, false, cancelToken);
                    writerResult.ProfileTableName = _profileTable.Name;

                    try
                    {
                        var profileResult = await _profileConnection.ExecuteInsertBulk(_profileTable, profileResults, cancelToken);
                    }
                    catch(Exception ex)
                    {
                        var message = $"Failed to save profile results.  {ex.Message}";
                        await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, ex);
                        await _targetConnection.DataWriterError(message, ex);
                        return false;
                    }
                }
            }

            var setRunStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished);
            return setRunStatusResult;
        }

        ///// <summary>
        ///// This updates the transformWriter record when an attempt is made to return.
        ///// </summary>
        ///// <param name="returnValue"></param>
        ///// <param name="writerResult"></param>
        ///// <param name="cancelToken"></param>
        ///// <returns></returns>
        //private async Task ReturnUpdate(bool Success, string Message, Exception Exception, TransformWriterResult writerResult, CancellationToken cancelToken)
        //{
        //    if(cancelToken.IsCancellationRequested)
        //    {
        //        await writerResult.SetRunStatus(ERunStatus.Cancelled, "Job was cancelled");
        //    }
        //    else
        //    {
        //        if(!Success)
        //        {
        //            await writerResult.SetRunStatus(ERunStatus.Abended, Message);
        //            await _targetConnection.DataWriterError(Message, Exception);

        //        }
        //    }
        //}

        public async Task WriteStart(Transform inTransform, TransformWriterResult writerResult, CancellationToken cancelToken)
        {

            if (_writeOpen)
            {
                throw new TransformWriterException("Transform write failed to start, as a previous operation is still running.");
            }

            var returnValue = await _inTransform.Open(writerResult.AuditKey, null, cancelToken);
            if (!returnValue)
            {
                throw new TransformWriterException("Transform write failed to start, could not open the first transform.");
            }

            _operationColumnIndex = _inTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);

            _createRows = new TableCache();
            _updateRows = new TableCache();
            _deleteRows = new TableCache();
            _rejectRows = new TableCache();

            //create template queries, with the values set to paramaters (i.e. @param1, @param2)
            new InsertQuery(_targetTable.Name, _targetTable.Columns.Select(c => new QueryColumn(new TableColumn(c.Name, c.Datatype), "@param" + _targetTable.GetOrdinal(c.Name).ToString())).ToList());

            new UpdateQuery(
                _targetTable.Name,
                _targetTable.Columns.Where(c=> c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, "@param" + _targetTable.GetOrdinal(c.Name).ToString())).ToList(),
                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c=> new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList()
            );

            new DeleteQuery(_targetTable.Name, _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            //if the table doesn't exist, create it.  
            var tableExistsResult = await _targetConnection.TableExists(_targetTable, cancelToken);
            if (!tableExistsResult)
            {
                returnValue = await _targetConnection.CreateTable(_targetTable, false, cancelToken);
                if (!returnValue)
                {
                    throw new TransformWriterException($"Transform write failed to start, could not create the target table {_targetTable?.Name}.");
                }
            }

            await _targetConnection.DataWriterStart(_targetTable);

            //if the truncate table flag is set, then truncate the target table.
            if (writerResult.TruncateTarget)
            {
                var truncateResult = await _targetConnection.TruncateTable(_targetTable, cancelToken);
                if (!truncateResult)
                {
                    throw new TransformWriterException($"Transform write failed to start, could not truncate the target table {_targetTable?.Name}.");
                }
            }

            int columnCount = _targetTable.Columns.Count;
            _fieldOrdinals = new int[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                _fieldOrdinals[i] = inTransform.GetOrdinal(_targetTable.Columns[i].Name);
            }

            if(_rejectTable != null)
            {
                columnCount = _rejectTable.Columns.Count;
                _rejectFieldOrdinals = new int[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    _rejectFieldOrdinals[i] = inTransform.GetOrdinal(_rejectTable.Columns[i].Name);
                }
            }

            _writeOpen = true;

        }

        public async Task WriteRecord(TransformWriterResult writerResult, Transform reader)
        {
            if (_writeOpen == false)
            {
                throw new TransformWriterException($"Transform write failed to write record as the WriteStart has not been called.");
            }

            //split the operation field (if it exists) and create copy of the row.
            char operation;

            //determine the type of operation (create, update, delete, reject)
            if (_operationColumnIndex == -1)
            {
                operation = 'C';
            }
            else
            {
                operation = (char)reader[_operationColumnIndex];
            }

            Table table;
            int[] ordinals = _fieldOrdinals;

            if (operation == 'R')
            {
                table = _rejectTable;
                ordinals = _rejectFieldOrdinals;
                if (_rejectTable == null)
                {
                    var rejectColumn = reader.GetOrdinal("RejectedReason");
                    string rejectReason = "";
                    if (rejectColumn > 0)
                        rejectReason = reader[rejectColumn].ToString();
                    else
                        rejectReason = "No reject reason found.";

                    throw new TransformWriterException($"Transform write failed as a record was rejected, however there is no reject table set.  The reject reason was: {rejectReason}.");
                }
            }
            else
                table = _targetTable;

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
                    _createRows.Add(row);
                    writerResult.IncrementRowsCreated();
                    if (_createRows.Count >= CommitSize)
                    {
                        await DoCreates();
                        return;
                    }
                    break;
                case 'U':
                    _updateRows.Add(row);
                    writerResult.IncrementRowsUpdated();
                    if (_updateRows.Count >= CommitSize)
                    {
                        await DoUpdate();
                        return;
                    }
                    break;
                case 'D':
                    _deleteRows.Add(row);
                    writerResult.IncrementRowsDeleted();
                    if (_deleteRows.Count >= CommitSize)
                    {
                        await DoDelete();
                        return;
                    }
                    break;
                case 'R':
                    _rejectRows.Add(row);
                    if (_rejectRows.Count >= CommitSize)
                    {
                        await DoReject();
                        return;
                    }
                    break;
                case 'T':
                    if (!_targetConnection.DynamicTableCreation)
                    {
                        var truncateResult = await _targetConnection.TruncateTable(_targetTable, _cancelToken);

                        if (!truncateResult)
                        {
                            throw new TransformWriterException($"Transform write failed, could not truncate the target table {_targetTable?.Name}.");
                        }
                    } 
                    else
                    {
                        return;
                    }
                    break;
            }

            return;
        }

        public async Task WriteFinish(TransformWriterResult writerResult, Transform reader)
        {
            _writeOpen = false;

            //write out the remaining rows.
            if (_createRows.Count > 0)
            {
                await DoCreates();
            }

            if (_updateRows.Count > 0)
            {
                await DoUpdate();
            }

            if (_deleteRows.Count > 0)
            {
                await DoDelete();
            }

            if (_rejectRows.Count > 0)
            {
                await DoReject();
            }

            //wait for any write tasks to finish
            if (_createRecordsTask != null)
            {
                var returnValue = await _createRecordsTask;
                WriteDataTicks += returnValue;
            }

            if (_createRecordsTask != null)
            {
                var returnValue = await _createRecordsTask;
                WriteDataTicks += returnValue;
            }

            if (_updateRecordsTask != null)
            {
                var returnValue = await _updateRecordsTask;
                WriteDataTicks += returnValue;
            }

            if (_deleteRecordsTask != null)
            {
                var returnValue = await _deleteRecordsTask;
                WriteDataTicks += returnValue;
            }

            if (_rejectRecordsTask != null)
            {
                var returnValue = await _rejectRecordsTask;
                WriteDataTicks += returnValue;
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

            try
            {
                await _targetConnection.DataWriterFinish(_targetTable);
            }
            catch(Exception ex)
            {
                throw new TransformWriterException($"The transform writer failed to finish when attempting a finish on the target table {_targetTable.Name} in {_targetConnection.Name}.  {ex.Message}.", ex);
            }

        }
        private async Task DoCreates()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result;
            }

            Table createTable = new Table(_targetTable.Name, _targetTable.Columns, _createRows);
            var createReader = new ReaderMemory(createTable);

            _createRecordsTask = _targetConnection.ExecuteInsertBulk(_targetTable, createReader, _cancelToken);  //this has no await to ensure processing continues.

            _createRows = new TableCache();
        }

        private async Task DoUpdate()
        {
            //update must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result;
            }

            if (_updateRecordsTask != null)
            {
                var result = await _updateRecordsTask;
                WriteDataTicks += result;
            }

            List<UpdateQuery> updateQueries = new List<UpdateQuery>();
            foreach(object[] row in _updateRows)
            {
                UpdateQuery updateQuery = new UpdateQuery(
                _targetTable.Name,
                _targetTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, row[_targetTable.GetOrdinal(c.Name)])).ToList(),
                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[_targetTable.GetOrdinal(c.Name)])).ToList()
                );

                updateQueries.Add(updateQuery);
            }

            _updateRecordsTask = _targetConnection.ExecuteUpdate(_targetTable, updateQueries, _cancelToken);  //this has no await to ensure processing continues.

            _updateRows = new TableCache();
        }

        private async Task DoDelete()
        {
            //delete must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result;
            }

            if (_updateRecordsTask != null)
            {
                var result = await _updateRecordsTask;
                WriteDataTicks += result;
            }

            if (_deleteRecordsTask != null)
            {
                var result = await _deleteRecordsTask;
                WriteDataTicks += result;
            }

            new DeleteQuery(_targetTable.Name, _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            List<DeleteQuery> deleteQueries = new List<DeleteQuery>();
            foreach (object[] row in _deleteRows)
            {
                DeleteQuery deleteQuery = new DeleteQuery(
                _targetTable.Name,
                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[_targetTable.GetOrdinal(c.Name)])).ToList()
                );

                deleteQueries.Add(deleteQuery);
            }

            _deleteRecordsTask = _targetConnection.ExecuteDelete(_targetTable, deleteQueries, _cancelToken);  //this has no await to ensure processing continues.

            _deleteRows = new TableCache();
        }

        private async Task DoReject()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (_rejectRecordsTask != null)
            {
                var result = await _rejectRecordsTask;
                WriteDataTicks += result;
            }

            // create a reject table if reject records have occurred.
            if(!rejectTableCreated)
            {
                if (_rejectTable != null)
                {
                    var rejectExistsResult = await _rejectConnection.TableExists(_rejectTable, _cancelToken);

                    if (!rejectExistsResult)
                    {
                        var returnValue = await _rejectConnection.CreateTable(_rejectTable, false, _cancelToken);
                        if (!returnValue)
                        {
                            throw new TransformWriterException($"The transform writer failed create the reject table {_rejectTable.Name} on {_rejectConnection.Name}.");
                        }
                    }
                    // compare target table to ensure all columns exist.
                    var compareTableResult = await _rejectConnection.CompareTable(_rejectTable, _cancelToken);
                    if (!compareTableResult)
                    {
                        throw new TransformWriterException($"The transform writer failed as the reject table columns did not match expected columns.  Table {_rejectTable.Name} on {_rejectConnection.Name}.");
                    }

                    rejectTableCreated = true;
                }
                else
                {
                    throw new TransformWriterException($"The transform writer failed there were rejected records, however no reject table specified.");
                }
            }

            Table createTable = new Table(_rejectTable.Name, _rejectTable.Columns, _rejectRows);

            var createReader = new ReaderMemory(createTable);

            _rejectRecordsTask = _targetConnection.ExecuteInsertBulk(createTable, createReader, _cancelToken);  //this has no await to ensure processing continues.

        }

    }
}
