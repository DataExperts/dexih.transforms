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

        private Task<TimeSpan> _createRecordsTask; //task to allow writes to run async with other processing.
		private Task<TimeSpan> _updateRecordsTask; //task to allow writes to run async with other processing.
		private Task<TimeSpan> _deleteRecordsTask; //task to allow writes to run async with other processing.
		private Task<TimeSpan> _rejectRecordsTask; //task to allow writes to run async with other processing.

        private Transform _inTransform;
        private Table _targetTable;
        private Table _rejectTable;
        private Table _profileTable;

        private Connection _targetConnection;
        private Connection _rejectConnection;
        private bool rejectTableCreated = false;
        private Connection _profileConnection;

        private CancellationToken _cancellationToken;

        public TimeSpan WriteDataTicks;

        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;


        public async Task<ReturnValue> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, CancellationToken cancelToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, null, null, null, null, cancellationToken);
        }

        public Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, CancellationToken cancellationToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, null, null, cancellationToken);
        }

        public Task<bool> WriteAllRecords( TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Table profileTable, CancellationToken cancellationToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, targetConnection, profileTable, targetConnection, cancellationToken);
        }


        public Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, CancellationToken cancellationToken)
        {
            return WriteAllRecords(writerResult, inTransform, targetTable, targetConnection, rejectTable, rejectConnection, null, null, cancellationToken);
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
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, Table profileTable, Connection profileConnection, CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            _targetConnection = targetConnection;

            if (rejectConnection == null)
                _rejectConnection = targetConnection;
            else
                _rejectConnection = rejectConnection;

            if (profileConnection == null)
                _profileConnection = targetConnection;
            else
                _profileConnection = profileConnection;

            var updateResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
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
                await WriteStart(_inTransform, writerResult, cancellationToken);
            }
            catch (Exception ex)
            {
                var message = $"The transform writer failed to start.  {ex.Message}";
				var newException = new TransformWriterException(message, ex);
				await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
                return false;
            }

            var firstRead = true;
            Task writeTask = null;

            while (await inTransform.ReadAsync(cancellationToken))
            {
                if (firstRead)
                {
                    var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, cancellationToken);
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
						var newException = new TransformWriterException(message, writeTask.Exception);
                        await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
                        return false;
                    }
                }

                writeTask = WriteRecord(writerResult, inTransform);

                if (cancellationToken.IsCancellationRequested)
                {
                    var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, cancellationToken);
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
				var newException = new TransformWriterException(message, ex);
                await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
                return false;
            }

            if (_profileTable != null)
            {
                var profileResults = inTransform.GetProfileResults();
                if (profileResults != null)
                {
                    var profileExists = await _profileConnection.TableExists(_profileTable, cancellationToken);
                    if (!profileExists)
                    {
                        await _profileConnection.CreateTable(_profileTable, false, cancellationToken);
                    }

                    writerResult.ProfileTableName = _profileTable.Name;

                    try
                    {
                        await _profileConnection.ExecuteInsertBulk(_profileTable, profileResults, cancellationToken);
                    }
                    catch(Exception ex)
                    {
                        var message = $"Failed to save profile results.  {ex.Message}";
						var newException = new TransformWriterException(message, ex);
						await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
                        return false;
                    }
                }
            }

            var setRunStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, cancellationToken);
            return setRunStatusResult;
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

            var returnValue = await _inTransform.Open(writerResult.AuditKey, null, cancellationToken);
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
            //new InsertQuery(_targetTable.Name, _targetTable.Columns.Select(c => new QueryColumn(new TableColumn(c.Name, c.Datatype), "@param" + _targetTable.GetOrdinal(c.Name).ToString())).ToList());

            //new UpdateQuery(
            //    _targetTable.Name,
            //    _targetTable.Columns.Where(c=> c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, "@param" + _targetTable.GetOrdinal(c.Name).ToString())).ToList(),
            //    _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c=> new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList()
            //);

            //new DeleteQuery(_targetTable.Name, _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

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

            var columnCount = _targetTable.Columns.Count;
            _fieldOrdinals = new int[columnCount];
            for (var i = 0; i < columnCount; i++)
            {
                _fieldOrdinals[i] = inTransform.GetOrdinal(_targetTable.Columns[i].Name);
            }

            if(_rejectTable != null)
            {
                columnCount = _rejectTable.Columns.Count;
                _rejectFieldOrdinals = new int[columnCount];
                for (var i = 0; i < columnCount; i++)
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
            var ordinals = _fieldOrdinals;

            if (operation == 'R')
            {
                table = _rejectTable;
                ordinals = _rejectFieldOrdinals;
                if (_rejectTable == null)
                {
                    var rejectColumn = reader.GetOrdinal("RejectedReason");
                    var rejectReason = "";
                    if (rejectColumn > 0)
                        rejectReason = reader[rejectColumn].ToString();
                    else
                        rejectReason = "No reject reason found.";

                    throw new TransformWriterException($"Transform write failed as a record was rejected, however there is no reject table set.  The reject reason was: {rejectReason}.");
                }
            }
            else
                table = _targetTable;

            var columnCount = table.Columns.Count;

            var row = new object[columnCount];

            for (var i = 0; i < columnCount; i++)
            {
                //int ordinal = reader.GetOrdinal(table.Columns[i].ColumnName);
                var ordinal = ordinals[i];
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
                        await _targetConnection.TruncateTable(_targetTable, _cancellationToken);
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
            var rowsWritten = writerResult.RowsTotal - writerResult.RowsIgnored;

            writerResult.WriteTicks = WriteDataTicks.Ticks;
            writerResult.ReadTicks = reader.ReaderTimerTicks().Ticks;
            writerResult.ProcessingTicks = reader.ProcessingTimerTicks().Ticks;

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

        }
        private async Task DoCreates()
        {
            //wait for the previous create task to finish before writing next buffer.
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result;
            }

            var createTable = new Table(_targetTable.Name, _targetTable.Columns, _createRows);
            var createReader = new ReaderMemory(createTable);

			_createRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteInsertBulk(_targetTable, createReader, _cancellationToken));  //this has no await to ensure processing continues.

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

            var updateQueries = new List<UpdateQuery>();
            foreach(var row in _updateRows)
            {
                var updateQuery = new UpdateQuery(
                _targetTable.Name,
                _targetTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, row[_targetTable.GetOrdinal(c.Name)])).ToList(),
                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[_targetTable.GetOrdinal(c.Name)])).ToList()
                );

                updateQueries.Add(updateQuery);
            }

			_updateRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteUpdate(_targetTable, updateQueries, _cancellationToken));  //this has no await to ensure processing continues.

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

            //new DeleteQuery(_targetTable.Name, _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());

            var deleteQueries = new List<DeleteQuery>();
            foreach (var row in _deleteRows)
            {
                var deleteQuery = new DeleteQuery(
                _targetTable.Name,
                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[_targetTable.GetOrdinal(c.Name)])).ToList()
                );

                deleteQueries.Add(deleteQuery);
            }

			_deleteRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteDelete(_targetTable, deleteQueries, _cancellationToken));  //this has no await to ensure processing continues.

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
            if(!_rejectTableCreated)
            {
                if (_rejectTable != null)
                {
                    var rejectExistsResult = await _rejectConnection.TableExists(_rejectTable, _cancellationToken);

                    if (!rejectExistsResult)
                    {
                        await _rejectConnection.CreateTable(_rejectTable, false, _cancellationToken);
                    }
                    // compare target table to ensure all columns exist.
                    var compareTableResult = await _rejectConnection.CompareTable(_rejectTable, _cancellationToken);
                    if (!compareTableResult)
                    {
                        throw new TransformWriterException($"The transform writer failed as the reject table columns did not match expected columns.  Table {_rejectTable.Name} on {_rejectConnection.Name}.");
                    }

                    _rejectTableCreated = true;
                }
                else
                {
                    throw new TransformWriterException($"The transform writer failed there were rejected records, however no reject table specified.");
                }
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

			_rejectRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteInsertBulk(createTable, createReader, _cancellationToken));  //this has no await to ensure processing continues.

        }

    }
}
