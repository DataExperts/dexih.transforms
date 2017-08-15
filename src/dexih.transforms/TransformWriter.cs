using dexih.functions;
using System;
using System.Collections.Generic;
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

        private TableCache _createRows;
        private TableCache _updateRows;
        private TableCache _deleteRows;
        private TableCache _rejectRows;


        private bool _writeOpen;
        private int _operationColumnIndex; //the index of the operation in the source data.

        private Task<ReturnValue<long>> _createRecordsTask; //task to allow writes to run async with other processing.
        private Task<ReturnValue<long>> _updateRecordsTask; //task to allow writes to run async with other processing.
        private Task<ReturnValue<long>> _deleteRecordsTask; //task to allow writes to run async with other processing.
        private Task<ReturnValue<long>> _rejectRecordsTask; //task to allow writes to run async with other processing.

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


        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, CancellationToken cancelToken)
        {
            return await WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, null, null, null, null, cancelToken);
        }

        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, CancellationToken cancelToken)
        {
            return await WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, null, null, cancelToken);
        }

        public async Task<ReturnValue> WriteAllRecords( TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Table profileTable, CancellationToken cancelToken)
        {
            return await WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, profileTable, targetConnection, cancelToken);
        }


        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, CancellationToken cancelToken)
        {
            return await WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, rejectConnection, null, null, cancelToken);
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
        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, Table profileTable, Connection profileConnection, CancellationToken cancelToken)
        {
            try
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
                if (!updateResult.Success)
                    return updateResult;

                _targetTable = targetTable;
                _rejectTable = rejectTable;
                _profileTable = profileTable;

                _inTransform = inTransform;

                writerResult.RejectTableName = rejectTable?.Name;

                var returnValue = await WriteStart(_inTransform, writerResult, cancelToken);

                if (returnValue.Success == false)
                {
                    await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, returnValue);
                    return returnValue;
                }

                bool firstRead = true;
                Task<ReturnValue> writeTask = null;

                while (await inTransform.ReadAsync(cancelToken))
                {
                    if (firstRead)
                    {
                        var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Running);
                        if (!runStatusResult.Success)
                            return runStatusResult;
                        firstRead = false;
                    }

                    if (writeTask != null)
                    {
                        var result = await writeTask;
                        if(!result.Success)
                        {
                            await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, returnValue);
                            return returnValue;
                        }
                    }

                    writeTask = WriteRecord(writerResult, inTransform);

                    if (cancelToken.IsCancellationRequested)
                    {
                        var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled);
                        return runStatusResult;
                    }
                }

                returnValue = await WriteFinish(writerResult, inTransform);
                if (returnValue.Success == false)
                {
                    await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, returnValue);
                    return new ReturnValue(false, returnValue.Message, null);
                }

                if (_profileTable != null)
                {
                    returnValue = await _profileConnection.CreateTable(_profileTable, false, cancelToken);
                    writerResult.ProfileTableName = _profileTable.Name;
                    var profileResults = inTransform.GetProfileResults();
                    if (profileResults != null)
                    {
                        var profileResult = await _profileConnection.ExecuteInsertBulk(_profileTable, profileResults, cancelToken);
                        if (!profileResult.Success)
                        {
                            profileResult.Message = "Failed to save profile results";
                            await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, profileResult);
                            return profileResult;
                        }
                    }
                }

                var setRunStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished);

                return setRunStatusResult;
            }
            catch(Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when attempting to run the transform: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// This updates the transformWriter record when an attempt is made to return.
        /// </summary>
        /// <param name="returnValue"></param>
        /// <param name="writerResult"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        private async Task<ReturnValue> returnUpdate(ReturnValue returnValue, TransformWriterResult writerResult, CancellationToken cancelToken)
        {
            var newReturn = new ReturnValue(returnValue.Success, returnValue.Message, returnValue.Exception);
            if(cancelToken.IsCancellationRequested)
            {
                newReturn.Success = false;
                newReturn.Message = "Job was cancelled";
                await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, newReturn);
            }
            else
            {
                if(!returnValue.Success)
                {
                    await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, newReturn);
                }
            }

            return newReturn;
        }

        public async Task<ReturnValue> WriteStart(Transform inTransform, TransformWriterResult writerResult, CancellationToken cancelToken)
        {

            if (_writeOpen)
            {
                return await returnUpdate(new ReturnValue(false, "Write cannot start, as a previous operation is still running.  Run the WriteFinish command to reset.", null), writerResult, cancelToken);
            }

            var returnValue = await _inTransform.Open(writerResult.AuditKey, null, cancelToken); 
            if (!returnValue.Success)
                return returnValue;

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
            if (!tableExistsResult.Success)
            {
                return await returnUpdate(tableExistsResult, writerResult, cancelToken);
            }

            if (!tableExistsResult.Value)
            {
                returnValue = await _targetConnection.CreateTable(_targetTable, false, cancelToken);
                if (!returnValue.Success)
                {
                    return await returnUpdate(returnValue, writerResult, cancelToken);
                }
            }

            returnValue = await _targetConnection.DataWriterStart(_targetTable);
            if (!returnValue.Success)
            {
                return await returnUpdate(returnValue, writerResult, cancelToken);
            }

            //if the truncate table flag is set, then truncate the target table.
            if (writerResult.TruncateTarget)
            {
                var truncateResult = await _targetConnection.TruncateTable(_targetTable, cancelToken);
                if (!truncateResult.Success)
                {
                    return await returnUpdate(returnValue, writerResult, cancelToken);
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

            return new ReturnValue(true);
        }

        public async Task<ReturnValue> WriteRecord(TransformWriterResult writerResult, Transform reader)
        {
            if (_writeOpen == false)
            {
                return await returnUpdate(new ReturnValue(false, "Cannot write records as the WriteStart has not been called.", null), writerResult, _cancelToken);

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

                    var returnValue = new ReturnValue(false, "A record was rejected, however there is no reject table.  The message was: " + rejectReason, null);
                    var setStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.RunningErrors, returnValue );
                    return setStatusResult;
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
                        return await DoCreates();
                    break;
                case 'U':
                    _updateRows.Add(row);
                    writerResult.IncrementRowsUpdated();
                    if (_updateRows.Count >= CommitSize)
                        return await DoUpdate();
                    break;
                case 'D':
                    _deleteRows.Add(row);
                    writerResult.IncrementRowsDeleted();
                    if (_deleteRows.Count >= CommitSize)
                        return await DoDelete();
                    break;
                case 'R':
                    _rejectRows.Add(row);
                    if (_rejectRows.Count >= CommitSize)
                        return await DoReject();
                    break;
                case 'T':
                    var truncateResult = await _targetConnection.TruncateTable(_targetTable, _cancelToken);
                    if (!truncateResult.Success)
                        return truncateResult;
                    break;
            }

            return new ReturnValue(true);
        }

        public async Task<ReturnValue> WriteFinish(TransformWriterResult writerResult, Transform reader)
        {
            _writeOpen = false;

            //write out the remaining rows.
            if (_createRows.Count > 0)
            {
                var returnValue = await DoCreates();
                if (returnValue.Success == false)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_updateRows.Count > 0)
            {
                var returnValue = await DoUpdate();
                if (returnValue.Success == false)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_deleteRows.Count > 0)
            {
                var returnValue = await DoDelete();
                if (returnValue.Success == false)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_rejectRows.Count > 0)
            {
                var returnValue = await DoReject();
                if (returnValue.Success == false)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            //wait for any write tasks to finish
            if (_createRecordsTask != null)
            {
                var returnValue = await _createRecordsTask;
                WriteDataTicks += returnValue.Value;
                if (!returnValue.Success)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_createRecordsTask != null)
            {
                var returnValue = await _createRecordsTask;
                WriteDataTicks += returnValue.Value;
                if (!returnValue.Success)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_updateRecordsTask != null)
            {
                var returnValue = await _updateRecordsTask;
                WriteDataTicks += returnValue.Value;
                if (!returnValue.Success)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_deleteRecordsTask != null)
            {
                var returnValue = await _deleteRecordsTask;
                WriteDataTicks += returnValue.Value;
                if (!returnValue.Success)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
            }

            if (_rejectRecordsTask != null)
            {
                var returnValue = await _rejectRecordsTask;
                WriteDataTicks += returnValue.Value;
                if (!returnValue.Success)
                    return await returnUpdate(returnValue, writerResult, _cancelToken);
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

            var returnValue2 = await _targetConnection.DataWriterFinish(_targetTable);
            if(!returnValue2.Success)
            {
                return await returnUpdate(returnValue2, writerResult, _cancelToken);
            }

            return new ReturnValue(true);
        }
        private async Task<ReturnValue> DoCreates()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            Table createTable = new Table(_targetTable.Name, _targetTable.Columns, _createRows);
            var createReader = new ReaderMemory(createTable);

            _createRecordsTask = _targetConnection.ExecuteInsertBulk(_targetTable, createReader, _cancelToken);  //this has no await to ensure processing continues.

            _createRows = new TableCache();
            return new ReturnValue(true);
        }

        private async Task<ReturnValue> DoUpdate()
        {
            //update must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (_updateRecordsTask != null)
            {
                var result = await _updateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
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

            return new ReturnValue(true);
        }

        private async Task<ReturnValue> DoDelete()
        {
            //delete must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (_updateRecordsTask != null)
            {
                var result = await _updateRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            if (_deleteRecordsTask != null)
            {
                var result = await _deleteRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
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

            return new ReturnValue(true);
        }

        private async Task<ReturnValue> DoReject()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (_rejectRecordsTask != null)
            {
                var result = await _rejectRecordsTask;
                WriteDataTicks += result.Value;
                if (!result.Success)
                    return result;
            }

            // create a reject table if reject records have occurred.
            if(!rejectTableCreated)
            {
                if (_rejectTable != null)
                {
                    var rejectExistsResult = await _rejectConnection.TableExists(_rejectTable, _cancelToken);
                    if (!rejectExistsResult.Success)
                    {
                        return rejectExistsResult;
                    }

                    if (!rejectExistsResult.Value)
                    {
                        var returnValue = await _rejectConnection.CreateTable(_rejectTable, false, _cancelToken);
                        if (!returnValue.Success)
                        {
                            return returnValue;
                        }
                    }
                    // compare target table to ensure all columns exist.
                    var compareTableResult = await _rejectConnection.CompareTable(_rejectTable, _cancelToken);
                    if (!compareTableResult.Success)
                    {
                        return compareTableResult;
                    }

                    rejectTableCreated = true;
                }
                else
                {
                    return new ReturnValue(false, "There were rejected records, and no reject table name specified on the target table.", null);
                }
            }

            Table createTable = new Table(_rejectTable.Name, _rejectTable.Columns, _rejectRows);

            var createReader = new ReaderMemory(createTable);

            _rejectRecordsTask = _targetConnection.ExecuteInsertBulk(createTable, createReader, _cancelToken);  //this has no await to ensure processing continues.

            return new ReturnValue(true);
        }

    }
}
