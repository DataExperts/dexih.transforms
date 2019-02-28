//using dexih.functions;
//using dexih.functions.Query;
//using dexih.transforms.Exceptions;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;
//
//namespace dexih.transforms
//{
//    /// <summary>
//    /// Bulk writes the contents of a transform into a single target table.
//    /// </summary>
//    public class TransformWriterBulk: Writer
//    {
//        /// <summary>
//        /// Indicates the rows buffer per commit.  
//        /// </summary>
//        public int CommitSize { get; set; } = 10000;
//
//        private bool _writeOpen;
//        private int _operationColumnIndex; //the index of the operation in the source data.
//
//        private CancellationToken _cancellationToken;
//
//        public TimeSpan WriteDataTicks;
//
//        private TransformWriterTargets _writerTargets;
//
//
//        /// <summary>
//        /// Writes all record from the inTransform to the target table and reject table.
//        /// </summary>
//        /// <param name="inTransform"></param>
//        /// <param name="writerTargets"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public override async Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTargets writerTargets, CancellationToken cancellationToken)
//        {
//            _cancellationToken = cancellationToken;
//            _writerTargets = writerTargets;
//            
//            var updateResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
//            if (!updateResult)
//            {
//                return false;
//            }
//
//            try
//            {
//                await WriteStart(inTransform, writerTargets, cancellationToken);
//            }
//            catch (Exception ex)
//            {
//                var message = $"The transform writer failed to start.  {ex.Message}";
//				var newException = new TransformWriterException(message, ex);
//				await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
//                return false;
//            }
//
//            var firstRead = true;
//            Task writeTask = null;
//
//            async Task<bool> HandleWriteTask(Task task)
//            {
//                if (task != null)
//                {
//                    await task;
//                    if(task.IsFaulted)
//                    {
//                        var message = $"The transform writer failed writing data.  {task.Exception?.Message}";
//                        var newException = new TransformWriterException(message, task.Exception);
//                        await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, CancellationToken.None);
//                        return false;
//                    }
//                }
//
//                return true;
//            }
//            
//            while (await inTransform.ReadPrepareAsync(cancellationToken))
//            {
//                if (firstRead)
//                {
//                    var runStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, CancellationToken.None);
//                    if (!runStatusResult)
//                    {
//                        return false;
//                    }
//                    firstRead = false;
//                }
//
//                if(!await HandleWriteTask(writeTask)) return false;
//
//                inTransform.ReadApply();
//
//                // allow the write task to run whilst also retrieving the next record.
//                writeTask = WriteRecords(writerTargets, inTransform);
//
//                if (cancellationToken.IsCancellationRequested)
//                {
//                    var runStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, CancellationToken.None);
//                    return runStatusResult;
//                }
//            }
//
//            if(!await HandleWriteTask(writeTask)) return false;
//
//            try
//            {
//                await WriteFinish(writerResult, convertedTransform);
//            }
//            catch (Exception ex)
//            {
//                var message = $"The transform writer failed to finish.  {ex.Message}";
//				var newException = new TransformWriterException(message, ex);
//                await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, CancellationToken.None);
//                return false;
//            }
//
//            if (_profileTable != null)
//            {
//                var profileResults = convertedTransform.GetProfileResults();
//                if (profileResults != null)
//                {
//                    var profileExists = await _profileConnection.TableExists(_profileTable, cancellationToken);
//                    if (!profileExists)
//                    {
//                        await _profileConnection.CreateTable(_profileTable, false, cancellationToken);
//                    }
//
//                    writerResult.ProfileTableName = _profileTable.Name;
//
//                    try
//                    {
//                        await _profileConnection.ExecuteInsertBulk(_profileTable, profileResults, cancellationToken);
//                    }
//                    catch(Exception ex)
//                    {
//                        var message = $"Failed to save profile results.  {ex.Message}";
//						var newException = new TransformWriterException(message, ex);
//						await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, CancellationToken.None);
//                        return false;
//                    }
//                }
//            }
//
//            var setRunStatusResult = await writerResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, CancellationToken.None);
//            return setRunStatusResult;
//        }
//
//        public async Task WriteStart(Transform inTransform, TransformWriterTargets writerTargets, CancellationToken cancellationToken)
//        {
//
//            if (_writeOpen)
//            {
//                throw new TransformWriterException("Transform write failed to start, as a previous operation is still running.");
//            }
//
//            _operationColumnIndex = inTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
//
//            foreach (var writerTarget in writerTargets.Items)
//            {
//                await writerTarget.WriterInitialize(inTransform, cancellationToken);
//            }
//
//            _writeOpen = true;
//
//        }
//
//        public async Task WriteRecords(TransformWriterTargets writerTargets)
//        {
//            if (_writeOpen == false)
//            {
//                throw new TransformWriterException($"Transform write failed to write record as the WriteStart has not been called.");
//            }
//
////            //split the operation field (if it exists) and create copy of the row.
////            char operation;
////
////            //determine the type of operation (create, update, delete, reject)
////            if (_operationColumnIndex == -1)
////            {
////                operation = 'C';
////            }
////            else
////            {
////                operation = (char)inTransform[_operationColumnIndex];
////            }
////
////
////            Table table;
////            var ordinals = _fieldOrdinals;
////
////            if (operation == 'R')
////            {
////                table = _rejectTable;
////                ordinals = _rejectFieldOrdinals;
////                if (_rejectTable == null)
////                {
////                    var rejectColumn = inTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.RejectedReason);
////                    var rejectReason = "";
////                    if (rejectColumn > 0)
////                        rejectReason = inTransform[rejectColumn].ToString();
////                    else
////                        rejectReason = "No reject reason found.";
////
////                    throw new TransformWriterException($"Transform write failed as a record was rejected, however there is no reject table set.  The reject reason was: {rejectReason}.");
////                }
////            }
////            else
////                table = _targetTable;
////
////            var columnCount = table.Columns.Count;
////
////            var row = new object[columnCount];
////
////            for (var i = 0; i < columnCount; i++)
////            {
////                var ordinal = ordinals[i];
////                if (ordinal >= 0)
////                {
////                    row[i] = inTransform[ordinal];
////                }
////            }
//
//            // insert the row for the current writer targets
//            foreach (var writerTarget in writerTargets.Items)
//            {
//                var record = writerTarget.CacheRecord(writerTargets.Transform);
//
//                if (record.operation == 'T')
//                {
//                    if (!writerTarget.TargetConnection.DynamicTableCreation)
//                    {
//                        await writerTarget.TargetConnection.TruncateTable(
//                            writerTarget.TargetTable, _cancellationToken);
//                    }
//                }
//                else
//                {
//                    if (record.tableCache != null)
//                    {
//                        switch (record.operation)
//                        {
//                            case 'C':
//                                await DoCreates(record.tableCache);
//                                break;
//                            case 'U':
//                                await DoUpdates(record.tableCache);
//                                break;
//                            case 'D':
//                                await DoDeletes(record.tableCache);
//                                break;
//                            case 'R':
//                                await DoRejects(record.tableCache);
//                                break;
//                            
//                        }
//                    }
//                }
//            }
//            
//            if (writerTargets.ChildNodes == null || writerTargets.ChildNodes.Count == 0)
//            {
//                return;
//            }
//
//            // loop through any child nodes, and recurse to write more records.
//            foreach (var writerChild in writerTargets.ChildNodes)
//            {
//                var transform = (Transform) writerTargets.Transform[writerChild.NodeName];
//                await transform.Open(_cancellationToken);
//                writerChild.Transform = transform;
//
//                while (await transform.ReadAsync(_cancellationToken))
//                {
//                    await WriteRecords(writerChild);
//                }
//            }
//
//            return;
//        }
//
//        public async Task WriteFinish(TransformWriterResult writerResult, Transform reader)
//        {
//            _writeOpen = false;
//
//            //write out the remaining rows.
//            if (_createRows.Count > 0)
//            {
//                await DoCreates();
//            }
//
//            if (_updateRows.Count > 0)
//            {
//                await DoUpdate();
//            }
//
//            if (_deleteRows.Count > 0)
//            {
//                await DoDelete();
//            }
//
//            if (_rejectRows.Count > 0)
//            {
//                await DoReject();
//            }
//
//            //wait for any write tasks to finish
//            if (_createRecordsTask != null)
//            {
//                var returnValue = await _createRecordsTask;
//                WriteDataTicks += returnValue;
//            }
//
//            if (_updateRecordsTask != null)
//            {
//                var returnValue = await _updateRecordsTask;
//                WriteDataTicks += returnValue;
//            }
//
//            if (_deleteRecordsTask != null)
//            {
//                var returnValue = await _deleteRecordsTask;
//                WriteDataTicks += returnValue;
//            }
//
//            if (_rejectRecordsTask != null)
//            {
//                var returnValue = await _rejectRecordsTask;
//                WriteDataTicks += returnValue;
//            }
//
//            //update the statistics.
//            writerResult.RowsFiltered = reader.TotalRowsFiltered;
//            writerResult.RowsSorted = reader.TotalRowsSorted;
//            writerResult.RowsRejected = reader.TotalRowsRejected;
//            writerResult.RowsPreserved = reader.TotalRowsPreserved;
//            writerResult.RowsIgnored = reader.TotalRowsIgnored;
//            writerResult.RowsReadPrimary = reader.TotalRowsReadPrimary;
//            writerResult.RowsReadReference = reader.TotalRowsReadReference;
//
//            //calculate the throughput figures
//            var rowsWritten = writerResult.RowsTotal - writerResult.RowsIgnored;
//
//            var performance = new StringBuilder();
//            performance.AppendLine(reader.PerformanceSummary());
//            performance.AppendLine($"Target {_targetConnection.Name} - Time: {WriteDataTicks:c}, Rows: {rowsWritten}, Performance: {(rowsWritten/WriteDataTicks.TotalSeconds):F} rows/second");
//
//            writerResult.PerformanceSummary = performance.ToString();
//
//
//            writerResult.WriteTicks = WriteDataTicks.Ticks;
//            writerResult.ReadTicks = reader.ReaderTimerTicks().Ticks;
//            writerResult.ProcessingTicks = reader.ProcessingTimerTicks().Ticks;
//
//            writerResult.EndTime = DateTime.Now;
//
//            if (writerResult.RowsTotal == 0)
//                writerResult.MaxIncrementalValue = writerResult.LastMaxIncrementalValue;
//            else
//                writerResult.MaxIncrementalValue = reader.GetMaxIncrementalValue();
//
//            reader.Dispose();
//
//            try
//            {
//                await _targetConnection.DataWriterFinish(_targetTable);
//            }
//            catch(Exception ex)
//            {
//                throw new TransformWriterException($"The transform writer failed to finish when attempting a finish on the target table {_targetTable.Name} in {_targetConnection.Name}.  {ex.Message}.", ex);
//            }
//
//        }
//        private async Task DoCreates(TableCache tableCache)
//        {
//            //wait for the previous create task to finish before writing next buffer.
//            if (_createRecordsTask != null)
//            {
//                var result = await _createRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            var createTable = new Table(_targetTable.Name, _targetTable.Columns, _createRows);
//            var createReader = new ReaderMemory(createTable);
//
//			_createRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteInsertBulk(_targetTable, createReader, _cancellationToken));  //this has no await to ensure processing continues.
//
//            _createRows = new TableCache();
//        }
//
//        private async Task DoUpdates(TableCache tableCache)
//        {
//            //update must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
//            if (_createRecordsTask != null)
//            {
//                var result = await _createRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            if (_updateRecordsTask != null)
//            {
//                var result = await _updateRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            var updateQueries = new List<UpdateQuery>();
//            foreach(var row in _updateRows)
//            {
//                var updateQuery = new UpdateQuery(
//                _targetTable.Name,
//                _targetTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.SurrogateKey).Select(c => new QueryColumn(c, row[_targetTable.GetOrdinal(c.Name)])).ToList(),
//                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[_targetTable.GetOrdinal(c.Name)])).ToList()
//                );
//
//                updateQueries.Add(updateQuery);
//            }
//
//			_updateRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteUpdate(_targetTable, updateQueries, _cancellationToken));  //this has no await to ensure processing continues.
//
//            _updateRows = new TableCache();
//        }
//
//        private async Task DoDeletes(TableCache tableCache)
//        {
//            //delete must wait for any inserts to complete (to avoid updates on records that haven't been inserted yet)
//            if (_createRecordsTask != null)
//            {
//                var result = await _createRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            if (_updateRecordsTask != null)
//            {
//                var result = await _updateRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            if (_deleteRecordsTask != null)
//            {
//                var result = await _deleteRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            //new DeleteQuery(_targetTable.Name, _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, "@surrogateKey")).ToList());
//
//            var deleteQueries = new List<DeleteQuery>();
//            foreach (var row in _deleteRows)
//            {
//                var deleteQuery = new DeleteQuery(
//                _targetTable.Name,
//                _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, Filter.ECompare.IsEqual, row[_targetTable.GetOrdinal(c.Name)])).ToList()
//                );
//
//                deleteQueries.Add(deleteQuery);
//            }
//
//			_deleteRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteDelete(_targetTable, deleteQueries, _cancellationToken));  //this has no await to ensure processing continues.
//
//            _deleteRows = new TableCache();
//        }
//
//        private async Task DoRejects(TableCache tableCache)
//        {
//            //wait for the previous create task to finish before writing next buffer.
//            if (_rejectRecordsTask != null)
//            {
//                var result = await _rejectRecordsTask;
//                WriteDataTicks += result;
//            }
//
//            // create a reject table if reject records have occurred.
//            if(!_rejectTableCreated)
//            {
//                if (_rejectTable != null)
//                {
//                    var rejectExistsResult = await _rejectConnection.TableExists(_rejectTable, _cancellationToken);
//
//                    if (!rejectExistsResult)
//                    {
//                        await _rejectConnection.CreateTable(_rejectTable, false, _cancellationToken);
//                    }
//                    // compare target table to ensure all columns exist.
//                    var compareTableResult = await _rejectConnection.CompareTable(_rejectTable, _cancellationToken);
//                    if (!compareTableResult)
//                    {
//                        throw new TransformWriterException($"The transform writer failed as the reject table columns did not match expected columns.  Table {_rejectTable.Name} on {_rejectConnection.Name}.");
//                    }
//
//                    _rejectTableCreated = true;
//                }
//                else
//                {
//                    throw new TransformWriterException($"The transform writer failed there were rejected records, however no reject table specified.");
//                }
//            }
//
//            var createTable = new Table(_rejectTable.Name, _rejectTable.Columns, _rejectRows);
//
//            var createReader = new ReaderMemory(createTable);
//
//			_rejectRecordsTask = TaskTimer.Start(() => _targetConnection.ExecuteInsertBulk(createTable, createReader, _cancellationToken));  //this has no await to ensure processing continues.
//
//        }
//
//    }
//}
