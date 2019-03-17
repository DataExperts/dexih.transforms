using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.transforms
{
    public class TransformWriterTarget
    {        
        
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult transformWriterResult);
        public delegate void StatusUpdate(TransformWriterResult transformWriterResult);
        public delegate void Finish(TransformWriterResult transformWriterResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        public event Finish OnFinish;

        #endregion
        
        [JsonConverter(typeof(StringEnumConverter))]
        public enum ETransformWriterMethod
        {
            Bulk, 
            Transaction,
            None
        }

        #region Initialize

        public TransformWriterTarget() :
            this( null, null, ETransformWriterMethod.Bulk, null, null, null, null)
        {
            
        }

        public TransformWriterTarget(Connection targetConnection, Table targetTable, TransformWriterResult writerResult):
            this(targetConnection, targetTable, ETransformWriterMethod.Bulk, writerResult, null,  null, null)
        {}


        public TransformWriterTarget(Connection targetConnection, Table targetTable, ETransformWriterMethod transformWriterMethod = ETransformWriterMethod.Bulk, TransformWriterResult writerResult = null,  TransformWriterOptions writerOptions = null,  Connection rejectConnection = null, Table rejectTable = null)
        {
            TransformWriterMethod = transformWriterMethod;
            TargetConnection = targetConnection;
            TargetTable = targetTable;
            RejectTable = rejectTable;
            RejectConnection = rejectConnection;
            
            WriterResult = writerResult;
            if (WriterResult != null)
            {
                WriterResult.OnStatusUpdate += Writer_OnStatusUpdate;
                WriterResult.OnProgressUpdate += Writer_OnProgressUpdate;
                WriterResult.OnFinish += Writer_OnFinish;
            }

            WriterOptions = writerOptions ?? new TransformWriterOptions();

            if (TargetTable == null) return;
            
            switch (transformWriterMethod)
            {
                case ETransformWriterMethod.Bulk:
                    _transformWriterTask = new TransformWriterTaskBulk(WriterOptions.CommitSize);
                    break;
                case ETransformWriterMethod.Transaction:
                    _transformWriterTask = new TransformWriterTaskTransaction();
                    break;
            }

            _transformWriterTask.Initialize(targetTable, targetConnection, rejectTable, rejectConnection);
        }
        
        #endregion

        #region Public Properties
        
        public ETransformWriterMethod TransformWriterMethod { get; set; }
        
        public TransformWriterResult WriterResult { get; private set; }
        
        public TransformWriterOptions WriterOptions { get; set; }
        
        public string NodeName { get; set; }
        
        [CopyReference]
        public Connection TargetConnection { get; set; }
        
        [CopyReference]
        public Table TargetTable { get; set; }

        [CopyReference]
        public Connection RejectConnection { get; set; }
        
        [CopyReference]
        public Table RejectTable { get; set; }
        
        public long CurrentAutoIncrementKey { get; set; }
        
        [CopyReference]
        public List<TransformWriterTarget> ChildWriterTargets { get; set; } = new List<TransformWriterTarget>();

        public TransformDelta.EUpdateStrategy? UpdateStrategy { get; set; }

        #endregion
        
        #region Private Properties
        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;
        private int _operationOrdinal;
        private bool _ordinalsInitialized = false;
        private readonly TransformWriterTask _transformWriterTask;
        private bool _truncateComplete = false; // used to stop multiple truncates
        
        #endregion


        /// <summary>
        /// Adds a child node writer into the appropriate position in the parent/child node structure.
        /// </summary>
        /// <param name="transformWriterTarget"></param>
        /// <param name="nodePath">Array of node names that shows the path to the child node.</param>
        public void Add(TransformWriterTarget transformWriterTarget, Span<string> nodePath)
        {
            if (nodePath == null || nodePath.Length == 0)
            {
                throw new TransformWriterException("The node path requires a value.");
            }

            var nodeName = nodePath[0];
            
            if (nodePath.Length > 1)
            {
                var childWriterTarget = ChildWriterTargets.SingleOrDefault(c => c.NodeName == nodeName);
                
                // add a dummy node if there is a gap in the path.
                if (childWriterTarget == null)
                {
                    childWriterTarget = new TransformWriterTarget {NodeName = nodeName};
                    ChildWriterTargets.Add(childWriterTarget);
                }
                
                childWriterTarget.Add(transformWriterTarget, nodePath.Slice(1));
                return;
            }

            transformWriterTarget.NodeName = nodePath[0];

            var existingWriterTarget = ChildWriterTargets.SingleOrDefault(c => c.NodeName == nodeName);

            if (existingWriterTarget == null)
            {
                transformWriterTarget.NodeName = nodePath[0];
                ChildWriterTargets.Add(transformWriterTarget);
            }
            else
            {
                if (existingWriterTarget.TargetTable != null)
                {
                    throw new TransformWriterException($"The node {nodeName} contains two targets.  Only one target table per node can be written.");
                }

                // copy the current node over the existing one, however keep the child nodes from the dummy node.
                var existingChildNodes = existingWriterTarget.ChildWriterTargets;
                transformWriterTarget.CopyProperties(existingWriterTarget);
                transformWriterTarget.ChildWriterTargets = existingChildNodes;
            }
        }
        
        /// <summary>
        /// Get all added TransformWriterTarget items in the hierarchy
        /// </summary>
        /// <returns></returns>
        public List<TransformWriterTarget> GetAll()
        {
            var items = new List<TransformWriterTarget>() { this};

            foreach (var childNode in ChildWriterTargets)
            {
                items.AddRange(childNode.GetAll());
            }

            return items;
        }
        
        /// <summary>
        /// Initializes all target tables, including truncating, and creating missing tables.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task InitializeAsync(CancellationToken cancellationToken)
        {
            _truncateComplete = false;
            
            if (TargetConnection != null && TargetTable != null)
            {
                //if the table doesn't exist, create it.  
                var tableExistsResult = await TargetConnection.TableExists(TargetTable, cancellationToken);
                if (!tableExistsResult)
                {
                    await TargetConnection.CreateTable(TargetTable, false, cancellationToken);
                }

                await TargetConnection.DataWriterStart(TargetTable);

                switch (WriterOptions.TargetAction)
                {
                    case TransformWriterOptions.eTargetAction.Truncate:
                        await TargetConnection.TruncateTable(TargetTable, cancellationToken);
                        _truncateComplete = true;
                        break;
                    case TransformWriterOptions.eTargetAction.DropCreate:
                        await TargetConnection.CreateTable(TargetTable, true, cancellationToken);
                        break;
                    case TransformWriterOptions.eTargetAction.CreateNotExists:
                        await TargetConnection.CreateTable(TargetTable, false, cancellationToken);
                        break;

                }
                
                // get the last surrogate key it there is one on the table.
                var autoIncrement = TargetTable.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                if (autoIncrement != null)
                {
                    CurrentAutoIncrementKey = await TargetConnection.GetNextKey(TargetTable, autoIncrement, cancellationToken);
                }
                else
                {
                    CurrentAutoIncrementKey = -1;
                }
            }
            
            foreach(var childWriterTarget in ChildWriterTargets)
            {
                await childWriterTarget.InitializeAsync(cancellationToken);
            }

            _ordinalsInitialized = false;
        }

        public Task WriteRecordsAsync(Transform transform, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(transform, TransformDelta.EUpdateStrategy.Append, cancellationToken);
        }

        /// <summary>
        /// Writes records from the transform to the target table (and child nodes).
        /// This will add a delta transform to perform delta and add auditing column values.
        /// </summary>
        /// <param name="transform"></param>
        /// <param name="updateStrategy"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="TransformWriterException"></exception>
        public async Task WriteRecordsAsync(Transform transform, TransformDelta.EUpdateStrategy? updateStrategy = null, CancellationToken cancellationToken = default)
        {
            

            if (WriterResult != null)
            {
                var updateResult = WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
                if (!updateResult)
                {
                    return;
                }
            }

            try
            {
                await InitializeAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                var message = $"The transform writer failed to start.  {ex.Message}";
                var newException = new TransformWriterException(message, ex);
                WriterResult?.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
                throw newException;
            }

            var targetReader = TargetConnection.GetTransformReader(TargetTable);
            await ProcessRecords(transform, targetReader, -1, updateStrategy, cancellationToken);
            await WriterFinalize(transform, cancellationToken);
            
        }

        public async Task WriteChildRecordsAsync(TransformNode transform, TableColumn keyColumn, object parentAutoIncrement, char parentOperation, Connection parentConnection, int transactionReference, TransformDelta.EUpdateStrategy? updateStrategy, CancellationToken cancellationToken)
        {
            if(transform == null) return;
            
            transform.SetParentAutoIncrement(parentAutoIncrement);

            if (parentConnection != TargetConnection)
            {
                transactionReference = -1;
            }

            switch (parentOperation)
            {
                case 'C': 
                case 'U':
                    var targetReader = TargetConnection.GetTransformReader(TargetTable);
                    if (keyColumn != null)
                    {
                        var targetFilter = TargetTable.Columns[keyColumn];
                        if (targetFilter != null)
                        {
                            var mappings = new Mappings()
                            {
                                new MapFilter(targetFilter, parentAutoIncrement)
                            };
                            targetReader = new TransformFilter(targetReader, mappings);
                        }
                    }
                    
                    // if parent operation is create/update then parent key hasn't changed, so process the records normally.
                    await ProcessRecords(transform, targetReader, transactionReference, updateStrategy, cancellationToken);
                    await WriterFinalize(transform, cancellationToken);
                    break;
                case 'D':
                    // if parent operation is delete, the child records will need to be deleted.
                    
                    break;
                case 'T':
                    // if parent operation is truncate, child table should be truncated also.
                    if (!_truncateComplete)
                    {
                        await TargetConnection.TruncateTable(TargetTable, cancellationToken);
                        _truncateComplete = true;
                    }

                    break;
            }
        }

        private async Task ProcessRecords(Transform transform, Transform targetTransform, int transactionReference, TransformDelta.EUpdateStrategy? updateStrategy, CancellationToken cancellationToken)
        {
            var firstRead = true;
            
            if (updateStrategy != null)
            {
                transform = new TransformDelta(transform, targetTransform, updateStrategy.Value, CurrentAutoIncrementKey, WriterOptions.AddDefaultRow);
                transform.SetEncryptionMethod(Transform.EEncryptionMethod.EncryptDecryptSecureFields, WriterOptions?.GlobalVariables?.EncryptionKey);

                if (!await transform.Open(WriterResult?.AuditKey ?? 0, null, cancellationToken))
                {
                    throw new TransformWriterException("Failed to open the data reader.");
                }
            }

            while (await transform.ReadAsync(cancellationToken))
            {
                if (firstRead && transactionReference <= 0)
                {
                    if (WriterResult != null)
                    {
                        var runStatusResult = WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, cancellationToken);
                        if (!runStatusResult)
                        {
                            return;
                        }
                    }

                    firstRead = false;
                }

                await WriteRecord(transform, transactionReference, updateStrategy, cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                {
                    if (WriterResult != null)
                    {
                        WriterResult?.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, cancellationToken);
                        return;
                    }
                }
            }
        }


        /// <summary>
        /// Caches a records from the inTransform.
        /// 
        /// </summary>
        /// <param name="transform"></param>
        /// <param name="isChildRecord"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The operation, and the tableCache if the rows have been exceeded.</returns>
        private async Task WriteRecord(Transform transform, int transactionReference, TransformDelta.EUpdateStrategy? updateStrategy, CancellationToken cancellationToken)
        {
            // initialize the ordinal lookups if this is the first write.
            if (!_ordinalsInitialized)
            {
                var count = TargetTable.Columns.Count;
                _fieldOrdinals = new int[count];
                for (var i = 0; i < count; i++)
                {
                    _fieldOrdinals[i] = transform.GetOrdinal(TargetTable.Columns[i].Name);
                }

                if (RejectTable != null)
                {
                    count = RejectTable.Columns.Count;
                    _rejectFieldOrdinals = new int[count];
                    for (var i = 0; i < count; i++)
                    {
                        _rejectFieldOrdinals[i] = transform.GetOrdinal(RejectTable.Columns[i].Name);
                    }
                }

                _operationOrdinal = transform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
                _ordinalsInitialized = true;
            }
            
            Table table;
            var ordinals = _fieldOrdinals;
            
            var operation = _operationOrdinal >= 0 ? (char) transform[_operationOrdinal] : 'C';

            if (operation == 'R')
            {
                table = RejectTable;
                ordinals = _rejectFieldOrdinals;
                if (table == null)
                {
                    var rejectColumn = transform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.RejectedReason);
                    var rejectReason = "";
                    rejectReason = rejectColumn > 0 ? transform[rejectColumn].ToString() : "No reject reason found.";
                    throw new TransformWriterException($"Transform write failed as a record was rejected, however there is no reject table set.  The reject reason was: {rejectReason}.");
                }
            } 
            else
            {
                table = TargetTable;
            }

            var columnCount = table.Columns.Count;

            var row = new object[columnCount];

            if (operation != 'T')
            {
                for (var i = 0; i < columnCount; i++)
                {
                    var ordinal = ordinals[i];
                    if (ordinal >= 0)
                    {
                        if (table.Columns[i].DeltaType != TableColumn.EDeltaType.DbAutoIncrement)
                        {
                            row[i] = TargetConnection.ConvertForWrite(table.Columns[i], transform[ordinal]);
                        }
                    }
                }
            }

            if (WriterResult != null)
            {
                switch (operation)
                {
                    case 'C':
                        WriterResult.IncrementRowsCreated();
                        break;
                    case 'U':
                        WriterResult.IncrementRowsUpdated();
                        break;
                    case 'D':
                        WriterResult.IncrementRowsDeleted();
                        break;
                }
            }

            var transaction = await _transformWriterTask.StartTransaction(transactionReference);

            try
            {

                CurrentAutoIncrementKey = await _transformWriterTask.AddRecord(operation, row, cancellationToken);

                //process childNodes
                foreach (var childWriterTarget in ChildWriterTargets)
                {
                    var childTransform = (TransformNode) transform[childWriterTarget.NodeName];
                    var keyColumn = TargetTable.GetAutoIncrementColumn();

                    await childWriterTarget.WriteChildRecordsAsync(childTransform, keyColumn, CurrentAutoIncrementKey,
                        operation,
                        TargetConnection,
                        transaction,
                        updateStrategy,
                        cancellationToken);
                }

                if (transactionReference <= 0)
                {
                    _transformWriterTask.CommitTransaction();
                }
            }
            catch (Exception)
            {
                if (transactionReference <= 0)
                {
                    _transformWriterTask.RollbackTransaction();
                }

                throw;
            }
        }

        private async Task WriterFinalize(Transform inTransform, CancellationToken cancellationToken)
        {
            await _transformWriterTask.FinalizeRecords(cancellationToken);

//            if (ProfileTable != null)
//            {
//                var profileResults = inTransform.GetProfileResults();
//                if (profileResults != null)
//                {
//                    var profileExists = await ProfileConnection.TableExists(ProfileTable, cancellationToken);
//                    if (!profileExists)
//                    {
//                        await ProfileConnection.CreateTable(ProfileTable, false, cancellationToken);
//                    }
//
//                    WriterResult.ProfileTableName = ProfileTable.Name;
//
//                    try
//                    {
//                        await ProfileConnection.ExecuteInsertBulk(ProfileTable, profileResults, cancellationToken);
//                    }
//                    catch(Exception ex)
//                    {
//                        var message = $"Failed to save profile results.  {ex.Message}";
//                        var newException = new TransformWriterException(message, ex);
//                        WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, CancellationToken.None);
//                        return;
//                    }
//                }
//            }

            try
            {
                await TargetConnection.DataWriterFinish(TargetTable);
            }
            catch(Exception ex)
            {
                throw new TransformWriterException($"The transform writer failed to finish when attempting a finish on the target table {TargetTable.Name} in {TargetConnection.Name}.  {ex.Message}.", ex);
            }

            if (WriterResult != null)
            {

                //update the statistics.
                WriterResult.RowsFiltered = inTransform.TotalRowsFiltered;
                WriterResult.RowsSorted = inTransform.TotalRowsSorted;
                WriterResult.RowsRejected = inTransform.TotalRowsRejected;
                WriterResult.RowsPreserved = inTransform.TotalRowsPreserved;
                WriterResult.RowsIgnored = inTransform.TotalRowsIgnored;
                WriterResult.RowsReadPrimary = inTransform.TotalRowsReadPrimary;
                WriterResult.RowsReadReference = inTransform.TotalRowsReadReference;

                //calculate the throughput figures
                var rowsWritten = WriterResult.RowsTotal - WriterResult.RowsIgnored;

                var performance = inTransform.PerformanceSummary();
                performance.Add(new TransformPerformance(TargetTable.Name, "Write Rows", rowsWritten,
                    _transformWriterTask.WriteDataTicks.TotalSeconds));

                WriterResult.PerformanceSummary = performance;

                WriterResult.WriteTicks = _transformWriterTask.WriteDataTicks.Ticks;
                WriterResult.ReadTicks = inTransform.ReaderTimerTicks().Ticks;
                WriterResult.ProcessingTicks = inTransform.ProcessingTimerTicks().Ticks;

                WriterResult.EndTime = DateTime.Now;

                if (WriterResult.RowsTotal == 0)
                    WriterResult.MaxIncrementalValue = WriterResult.LastMaxIncrementalValue;
                else
                    WriterResult.MaxIncrementalValue = inTransform.GetMaxIncrementalValue();

                if (CurrentAutoIncrementKey != -1)
                {
                    var surrogateKey = TargetTable.GetAutoIncrementColumn();
                    if (surrogateKey != null)
                    {
                        await TargetConnection.UpdateIncrementalKey(TargetTable, surrogateKey.Name,
                            inTransform.SurrogateKey, cancellationToken);
                    }
                }
                
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, cancellationToken);
                await WriterResult.Finalize();
            }

            inTransform.Dispose();
  
        }
        
        public void Writer_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(writer);
        }

        public void Writer_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(writer);
        }
        
        public void Writer_OnFinish(TransformWriterResult writer)
        {
            OnFinish?.Invoke(writer);
        }
        
        public bool SetRunStatus(TransformWriterResult.ERunStatus newStatus, string message, Exception exception, CancellationToken cancellationToken)
        {
            var result = WriterResult == null || WriterResult.SetRunStatus(newStatus, message, exception, cancellationToken);

            foreach (var childItem in ChildWriterTargets)
            {
                result = result && childItem.SetRunStatus(newStatus, message, exception, cancellationToken);
            }
            
            return result;
        }

    }
}