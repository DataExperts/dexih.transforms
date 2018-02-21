using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class TransformWriter
    {
        /// <summary>
        /// Indicates the rows buffer per commit.  
        /// </summary>
        public int CommitSize { get; set; } = 10000;

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
        private bool _rejectTableCreated = false;
        private Connection _profileConnection;

        private CancellationToken _cancellationToken;

        public TimeSpan WriteDataTicks;

        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;


        public Task<bool> WriteAllRecords(TransformWriterResult writerResult, Transform inTransform, Table targetTable, Connection targetConnection, CancellationToken cancellationToken)
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
                        return false;
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

        ///// <summary>
        ///// This updates the transformWriter record when an attempt is made to return.
        ///// </summary>
        ///// <param name="returnValue"></param>
        ///// <param name="writerResult"></param>
        ///// <param name="cancellationToken"></param>
        ///// <returns></returns>
        //private async Task ReturnUpdate(bool Success, string Message, Exception Exception, TransformWriterResult writerResult, CancellationToken cancellationToken)
        //{
        //    if(cancellationToken.IsCancellationRequested)
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

        public async Task WriteStart(Transform inTransform, TransformWriterResult writerResult, CancellationToken cancellationToken)
        {

            if (_writeOpen)
            {
                throw new TransformWriterException("Transform write failed to start, as a previous operation is still running.");
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
            var tableExistsResult = await _targetConnection.TableExists(_targetTable, cancellationToken);
            if (!tableExistsResult)
            {
                await _targetConnection.CreateTable(_targetTable, false, cancellationToken);
            }

            await _targetConnection.DataWriterStart(_targetTable);

            //if the truncate table flag is set, then truncate the target table.
            if (writerResult.TruncateTarget)
            {
                await _targetConnection.TruncateTable(_targetTable, cancellationToken);
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

            //calculate the throughput figures
            var rowsWritten = writerResult.RowsTotal - writerResult.RowsIgnored;

            var performance = new StringBuilder();
            performance.AppendLine(reader.PerformanceSummary());
            performance.AppendLine($"Target {_targetConnection.Name} - Time: {WriteDataTicks:c}, Rows: {rowsWritten}, Performance: {(rowsWritten/WriteDataTicks.TotalSeconds):F} rows/second");

            writerResult.PerformanceSummary = performance.ToString();


            writerResult.WriteTicks = WriteDataTicks.Ticks;
            writerResult.ReadTicks = reader.ReaderTimerTicks().Ticks;
            writerResult.ProcessingTicks = reader.ProcessingTimerTicks().Ticks;

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

            var createTable = new Table(_rejectTable.Name, _rejectTable.Columns, _rejectRows);

            var createReader = new ReaderMemory(createTable);

			_rejectRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteInsertBulk(createTable, createReader, _cancellationToken));  //this has no await to ensure processing continues.

        }

    }
}
