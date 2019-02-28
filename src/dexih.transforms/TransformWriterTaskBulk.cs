using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    public class TransformWriterBulkTask
    {
        private Task<TimeSpan> _createRecordsTask;
        private Task<TimeSpan> _updateRecordsTask;
        private Task<TimeSpan> _rejectRecordsTask;
        private Task<TimeSpan> _deleteRecordsTask;

        private TableCache _createRows;
        private TableCache _updateRows;
        private TableCache _deleteRows;
        private TableCache _rejectRows;

        private Table _targetTable;
        private Connection _targetConnection;

        private Table _rejectTable;
        private Connection _rejectConnection;
        
        public TimeSpan WriteDataTicks;

        private CancellationToken _cancellationToken;

        private const int CommitSize = 10000;

        private bool _rejectTableCreated = false;
        

        public TransformWriterBulkTask(TransformWriterResult writerResult, Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection, CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            _targetTable = targetTable;
            _targetConnection = targetConnection;
            _rejectTable = rejectTable;
            _rejectConnection = rejectConnection;
        }

        public async Task AddRecord(char operation, object[] row)
        {
            switch (operation)
            {
                case 'C':
                    _createRows.Add(row);
                    if (_createRows.Count > CommitSize)
                    {
                        await DoCreates();
                    }

                    break;
                case 'U':
                    _updateRows.Add(row);
                    if (_updateRows.Count > CommitSize)
                    {
                        await DoUpdates();
                    }

                    break;
                case 'D':
                    _deleteRows.Add(row);
                    if (_deleteRows.Count > CommitSize)
                    {
                        await DoDeletes();
                    }
                    break;
                case 'R':
                    _rejectRows.Add(row);
                    if (_rejectRows.Count > CommitSize)
                    {
                        await DoRejects();
                    }
                    break;
                            
            }
        }

        public async Task Finalize()
        {
            //write out the remaining rows.
            if (_createRows.Count > 0)
            {
                await DoCreates();
            }

            if (_updateRows.Count > 0)
            {
                await DoUpdates();
            }

            if (_deleteRows.Count > 0)
            {
                await DoDeletes();
            }

            if (_rejectRows.Count > 0)
            {
                await DoRejects();
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

        private async Task DoUpdates()
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

        private async Task DoDeletes()
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

        private async Task DoRejects()
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