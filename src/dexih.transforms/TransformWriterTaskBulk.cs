using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    public class TransformWriterTaskBulk: TransformWriterTask
    {
        private Task<TimeSpan> _createRecordsTask;
        private Task<TimeSpan> _updateRecordsTask;
        private Task<TimeSpan> _rejectRecordsTask;
        private Task<TimeSpan> _deleteRecordsTask;

        private TableCache _createRows;
        private TableCache _updateRows;
        private TableCache _deleteRows;
        private TableCache _rejectRows;

        private readonly int _commitSize;
        private bool _rejectTableCreated = false;

        public TransformWriterTaskBulk(int commitSize = 10000)
        {
            _commitSize = commitSize;
            
            _createRows = new TableCache();
            _updateRows = new TableCache();
            _deleteRows = new TableCache();
            _rejectRows = new TableCache();
        }

        public override Task<int> StartTransaction(int transactionReference = -1)
        {
            return Task.FromResult(-1);
        }

        public override void CommitTransaction()
        {
        }

        public override void RollbackTransaction()
        {
        }

        public override async Task<long> AddRecord(char operation, object[] row, CancellationToken cancellationToken = default)
        {
            switch (operation)
            {
                case 'C':
                    _createRows.Add(row);
                    if (_createRows.Count > _commitSize)
                    {
                        await DoCreates(cancellationToken);
                    }

                    break;
                case 'U':
                    _updateRows.Add(row);
                    if (_updateRows.Count > _commitSize)
                    {
                        await DoUpdates(cancellationToken);
                    }

                    break;
                case 'D':
                    _deleteRows.Add(row);
                    if (_deleteRows.Count > _commitSize)
                    {
                        await DoDeletes(cancellationToken);
                    }
                    break;
                case 'R':
                    _rejectRows.Add(row);
                    if (_rejectRows.Count > _commitSize)
                    {
                        await DoRejects(cancellationToken);
                    }
                    break;
                case 'T':
                    if (!TargetConnection.DynamicTableCreation && !TruncateComplete)
                    {
                        await TargetConnection.TruncateTable(TargetTable, cancellationToken);
                        TruncateComplete = true;
                    }

                    break;
            }

            if (AutoIncrementOrdinal >= 0)
            {
                var value = row[AutoIncrementOrdinal];
                if (value == null) return 0;
                return Operations.Parse<long>(value);
            }

            return 0;
        }

        public override async Task FinalizeWrites(CancellationToken cancellationToken = default)
        {
            //write out the remaining rows.
            if (_createRows.Count > 0)
            {
                await DoCreates(cancellationToken);
            }

            if (_updateRows.Count > 0)
            {
                await DoUpdates(cancellationToken);
            }

            if (_deleteRows.Count > 0)
            {
                await DoDeletes(cancellationToken);
            }

            if (_rejectRows.Count > 0)
            {
                await DoRejects(cancellationToken);
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


        
        private async Task DoCreates( CancellationToken cancellationToken = default)
        {
            //wait for the previous create task to finish before writing next buffer.
            if (_createRecordsTask != null)
            {
                var result = await _createRecordsTask;
                WriteDataTicks += result;
            }

            var createTable = new Table(TargetTable.Name, TargetTable.Columns, _createRows);
            var createReader = new ReaderMemory(createTable);

			_createRecordsTask = TaskTimer.StartAsync(() => TargetConnection.ExecuteInsertBulk(TargetTable, createReader, cancellationToken));  //this has no await to ensure processing continues.

            _createRows = new TableCache();
        }

        private async Task DoUpdates( CancellationToken cancellationToken = default)
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
                TargetTable.Columns.Where(c => !c.IsAutoIncrement()).Select(c => new QueryColumn(c, row[TargetTable.GetOrdinal(c.Name)])).ToList(),
                TargetTable.Columns.Where(c => c.IsAutoIncrement()).Select(c => new Filter(c, ECompare.IsEqual, row[TargetTable.GetOrdinal(c.Name)])).ToList()
                );

                updateQueries.Add(updateQuery);
            }

			_updateRecordsTask = TaskTimer.StartAsync(() => TargetConnection.ExecuteUpdate(TargetTable, updateQueries, cancellationToken));  //this has no await to ensure processing continues.

            _updateRows = new TableCache();
        }

        private async Task DoDeletes( CancellationToken cancellationToken = default)
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

            //new DeleteQuery(_targetTable.Name, _targetTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey).Select(c => new Filter(c, ECompare.IsEqual, "@surrogateKey")).ToList());

            var deleteQueries = new List<DeleteQuery>();
            foreach (var row in _deleteRows)
            {
                var deleteQuery = new DeleteQuery(
                TargetTable.Name,
                TargetTable.Columns.Where(c => c.IsAutoIncrement()).Select(c => new Filter(c, ECompare.IsEqual, row[TargetTable.GetOrdinal(c.Name)])).ToList()
                );

                deleteQueries.Add(deleteQuery);
            }

			_deleteRecordsTask = TaskTimer.StartAsync(() => TargetConnection.ExecuteDelete(TargetTable, deleteQueries, cancellationToken));  //this has no await to ensure processing continues.

            _deleteRows = new TableCache();
        }

        private async Task DoRejects( CancellationToken cancellationToken = default)
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
                if (RejectTable != null)
                {
                    var rejectExistsResult = await RejectConnection.TableExists(RejectTable, cancellationToken);

                    if (!rejectExistsResult)
                    {
                        await RejectConnection.CreateTable(RejectTable, false, cancellationToken);
                    }
                    // compare target table to ensure all columns exist.
                    var compareTableResult = await RejectConnection.CompareTable(RejectTable, cancellationToken);
                    if (!compareTableResult)
                    {
                        throw new TransformWriterException($"The transform writer failed as the reject table columns did not match expected columns.  Table {RejectTable.Name} on {RejectConnection.Name}.");
                    }

                    _rejectTableCreated = true;
                }
                else
                {
                    throw new TransformWriterException($"The transform writer failed there were rejected records, however no reject table specified.");
                }
            }

            var rejectTable = new Table(RejectTable.Name, RejectTable.Columns, _rejectRows);

            var rejectReader = new ReaderMemory(rejectTable);

			_rejectRecordsTask = TaskTimer.StartAsync(() => TargetConnection.ExecuteInsertBulk(rejectTable, rejectReader, cancellationToken));  //this has no await to ensure processing continues.
			
			_rejectRows = new TableCache();

        }
    }
}