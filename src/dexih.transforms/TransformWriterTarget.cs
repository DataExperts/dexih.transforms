using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using Dexih.Utils.CopyProperties;



namespace dexih.transforms
{
    public class TransformWriterTarget: IDisposable
    {        
        
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult transformWriterResult);
        public delegate void StatusUpdate(TransformWriterResult transformWriterResult);
        public delegate void Finish(TransformWriterResult transformWriterResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        public event Finish OnFinish;

        #endregion
        
        // [JsonConverter(typeof(StringEnumConverter))]
        public enum ETransformWriterMethod
        {
            Bulk = 1, 
            Transaction
        }

        #region Initialize

        public TransformWriterTarget(): this( null, null)
        {
            
        }


        public TransformWriterTarget(Connection targetConnection, Table targetTable, TransformWriterResult writerResult = null,  TransformWriterOptions writerOptions = null,  Connection rejectConnection = null, Table rejectTable = null, Connection profileConnection = null, string profileTableName = null)
        {
            TargetConnection = targetConnection;
            TargetTable = targetTable;
            RejectTable = rejectTable;
            RejectConnection = rejectConnection;
            ProfileTableName = profileTableName;
            ProfileConnection = profileConnection;
            
            WriterResult = writerResult;
            if (WriterResult != null)
            {
                WriterResult.OnStatusUpdate += Writer_OnStatusUpdate;
                WriterResult.OnProgressUpdate += Writer_OnProgressUpdate;
                WriterResult.OnFinish += Writer_OnFinish;
            }

            WriterOptions = writerOptions ?? new TransformWriterOptions();
        }
        
        #endregion

        #region Public Properties
        
        
        public TransformWriterResult WriterResult { get; private set; }
        
        public TransformWriterOptions WriterOptions { get; set; }
        
        public string NodeName { get; set; }

        public bool AddDefaultRow { get; set; } = false;
        
        [CopyReference]
        public Connection TargetConnection { get; set; }
        
        [CopyReference]
        public Table TargetTable { get; set; }

        [CopyReference]
        public Connection RejectConnection { get; set; }
        
        [CopyReference]
        public Table RejectTable { get; set; }

        [CopyReference]
        public Connection ProfileConnection { get; set; }
        
        [CopyReference]
        public string ProfileTableName { get; set; }

//        public long CurrentAutoIncrementKey { get; set; }
        
        [CopyReference]
        public List<TransformWriterTarget> ChildWriterTargets { get; set; } = new List<TransformWriterTarget>();

        public EUpdateStrategy? UpdateStrategy { get; set; }

        #endregion
        
        #region Private Properties
        
        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;
        private int _operationOrdinal;
        private bool _ordinalsInitialized = false;
        private TransformWriterTask _transformWriterTask;
        private long _autoIncrementKey = 0;
        private DateTime _maxValidFromDate = DateTime.MinValue;

        #endregion


        /// <summary>
        /// Adds a child node writer into the appropriate position in the parent/child node structure.
        /// </summary>
        /// <param name="transformWriterTarget"></param>
        /// <param name="nodePath">Array of node names that shows the path to the child node.</param>
        public void Add(TransformWriterTarget transformWriterTarget, string[] nodePath)
        {
            if (nodePath == null || nodePath.Length == 0)
            {
                throw new TransformWriterException("The node path requires a value.");
            }
            
            if (transformWriterTarget.WriterResult != null)
            {
                transformWriterTarget.WriterResult.OnStatusUpdate += Writer_OnStatusUpdate;
                transformWriterTarget.WriterResult.OnProgressUpdate += Writer_OnProgressUpdate;
                transformWriterTarget.WriterResult.OnFinish += Writer_OnFinish;
                
                transformWriterTarget.OnStatusUpdate += Writer_OnStatusUpdate;
                transformWriterTarget.OnProgressUpdate += Writer_OnProgressUpdate;
                transformWriterTarget.OnFinish += Writer_OnFinish;
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
                
                childWriterTarget.Add(transformWriterTarget, nodePath.Skip(1).ToArray());
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
            var items = new List<TransformWriterTarget> { this};

            foreach (var childNode in ChildWriterTargets)
            {
                items.AddRange(childNode.GetAll());
            }

            return items;
        }

        /// <summary>
        /// Initializes all target tables, including truncating, and creating missing tables.
        /// </summary>
        /// <param name="transformWriterMethod"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task InitializeAsync(ETransformWriterMethod transformWriterMethod, CancellationToken cancellationToken = default)
        {
   
            if (TargetConnection != null && TargetTable != null)
            {
                //if the table doesn't exist, create it.  
                var tableExistsResult = await TargetConnection.TableExists(TargetTable, cancellationToken);
                if (!tableExistsResult)
                {
                    await TargetConnection.CreateTable(TargetTable, false, cancellationToken);
                }

                await TargetConnection.DataWriterStart(TargetTable);

                if (WriterOptions.CheckTarget(TargetTable.Name))
                {
                    switch (WriterOptions.TargetAction)
                    {
                        case TransformWriterOptions.ETargetAction.Truncate:
                            await TargetConnection.TruncateTable(TargetTable, cancellationToken);
                            break;
                        case TransformWriterOptions.ETargetAction.DropCreate:
                            await TargetConnection.CreateTable(TargetTable, true, cancellationToken);
                            break;
                        case TransformWriterOptions.ETargetAction.CreateNotExists:
                            await TargetConnection.CreateTable(TargetTable, false, cancellationToken);
                            break;
                    }
                }

            }

            switch (transformWriterMethod)
            {
                case ETransformWriterMethod.Bulk:
                    _transformWriterTask = new TransformWriterTaskBulk(WriterOptions.CommitSize);
                    break;
                case ETransformWriterMethod.Transaction:
                    _transformWriterTask = new TransformWriterTaskTransaction();
                    break;
            }

            _transformWriterTask.Initialize(TargetTable, TargetConnection, RejectTable, RejectConnection);
            

            foreach(var childWriterTarget in ChildWriterTargets)
            {
                await childWriterTarget.InitializeAsync(transformWriterMethod, cancellationToken);
            }

            _ordinalsInitialized = false;
        }

        private async Task<long> GetIncrementalKey(CancellationToken cancellationToken = default)
        {
            if (_autoIncrementKey > 0)
            {
                return _autoIncrementKey;
                
            }
            // get the last surrogate key it there is one on the table.
            var autoIncrement = TargetTable.GetColumn(EDeltaType.AutoIncrement);
            if (autoIncrement != null)
            {
                return await TargetConnection.GetMaxValue<long>(TargetTable, autoIncrement, cancellationToken);
            }

            return -1;
        }

        private async Task<DateTime> GetMaxValidToDate(CancellationToken cancellationToken = default)
        {
            if (_maxValidFromDate > DateTime.MinValue)
            {
                return _maxValidFromDate;

            }
            // get the last surrogate key it there is one on the table.
            var tableColumn = TargetTable.GetColumn(EDeltaType.ValidToDate);
            if (tableColumn != null)
            {
                return await TargetConnection.GetMaxValue<DateTime>(TargetTable, tableColumn, cancellationToken);
            }

            return DateTime.MinValue;
        }

        public Task WriteRecordsAsync(Transform transform, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(transform, EUpdateStrategy.Append, ETransformWriterMethod.Bulk, cancellationToken);
        }

        public Task WriteRecordsAsync(Transform transform, EUpdateStrategy? updateStrategy, CancellationToken cancellationToken)
        {
            return WriteRecordsAsync(transform, updateStrategy, ETransformWriterMethod.Bulk, cancellationToken);
        }
        
        public Task WriteRecordsAsync(Transform transform, ETransformWriterMethod transformWriterMethod, CancellationToken cancellationToken = default)
        {
            return WriteRecordsAsync(transform, EUpdateStrategy.Append, transformWriterMethod, cancellationToken);
        }
        
        /// <summary>
        /// Writes records from the transform to the target table (and child nodes).
        /// This will add a delta transform to perform delta and add auditing column values.
        /// </summary>
        /// <param name="transform"></param>
        /// <param name="updateStrategy"></param>
        /// <param name="transformWriterMethod"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="TransformWriterException"></exception>
        public async Task WriteRecordsAsync(Transform transform, EUpdateStrategy? updateStrategy = null, ETransformWriterMethod transformWriterMethod = ETransformWriterMethod.Bulk, CancellationToken cancellationToken = default)
        {
            try
            {
                if (WriterResult != null)
                {
                    await WriterResult.Initialize(cancellationToken);

                    var sourceReader = transform.GetSourceReader();
                    sourceReader.OnReaderProgress += Reader_ReadProgress;
                }

                var updateResult = SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
                if (!updateResult)
                {
                    throw new TransformWriterException("Failed to start the transform writer.");
                }

                try
                {
                    await InitializeAsync(transformWriterMethod, cancellationToken);
                }
                catch (Exception ex)
                {
                    var message = $"Failed to initialize the transform writer.  {ex.Message}";
                    throw new TransformWriterException(message, ex);
                }

                if (TargetConnection != null && TargetTable != null)
                {
                    using (var targetReader = TargetConnection.GetTransformReader(TargetTable))
                    {
                        if (updateStrategy != null)
                        {
                            var autoIncrementKey = await GetIncrementalKey(cancellationToken);

                            transform = new TransformDelta(transform, targetReader, updateStrategy.Value, autoIncrementKey,
                                AddDefaultRow, false, new DeltaValues('C'));
                            transform.SetEncryptionMethod(EEncryptionMethod.EncryptDecryptSecureFields,
                                WriterOptions?.GlobalSettings?.EncryptionKey);

                            if (!await transform.Open(WriterResult?.AuditKey ?? 0, null, cancellationToken))
                            {
                                throw new TransformWriterException($"Failed to open the data reader {transform.Name}.");
                            }
                        }

                        await ProcessRecords(transform, targetReader, -1, updateStrategy, cancellationToken);
                    }
                }
                else
                {
                    // if there are no target tables, then just read all the records.
                    // this may be necessary if there are functions in the transforms which perform write actions.
                    var firstRead = true;
                    if (!await transform.Open(WriterResult?.AuditKey ?? 0, null, cancellationToken))
                    {
                        throw new TransformWriterException($"Failed to open the data reader {transform.Name}.");
                    }
                    
                    while (await transform.ReadAsync(cancellationToken))
                    {
                        if (firstRead)
                        {
                            if (WriterResult != null)
                            {
                                var runStatusResult = WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running,
                                    null, null);
                                if (!runStatusResult)
                                {
                                    return;
                                }
                            }

                            firstRead = false;
                        }

                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new TaskCanceledException();
                        }
                    }
                }

                await FinishTasks(transform, cancellationToken);

            }
            catch (OperationCanceledException)
            {
                SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, "The transform writer was cancelled", null,
                    CancellationToken.None);
                throw new TransformWriterException("The datalink was cancelled.");

            }
            catch (Exception ex)
            {
                var message = $"The transform writer failed.  {ex.Message}";
                var newEx = new TransformWriterException(message, ex);
                SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newEx, CancellationToken.None);
                throw newEx;
            }
            finally
            {
                transform.Close();

                // don't pass cancel token, as we want writer result updated when a cancel occurs.
                await UpdateWriterResult(transform, CancellationToken.None);
                await WriterTargetFinalize(transform, CancellationToken.None);
            }
        }

        private async Task WriteChildRecordsAsync(TransformNode transformNode, TableColumn keyColumn, DeltaValues deltaValues, Connection parentConnection, int transactionReference, EUpdateStrategy? updateStrategy, CancellationToken cancellationToken = default)
        {

            if (parentConnection != TargetConnection)
            {
                transactionReference = -1;
            }

            if (transformNode == null)
            {
                throw new TransformWriterException("The transform node was set to null.");
            }

            transformNode?.SetParentAutoIncrement(deltaValues.AutoIncrementValue);

            Transform transform = transformNode;

            using (var targetReader = TargetConnection.GetTransformReader(TargetTable))
            {
                var target = targetReader;
                
                if (keyColumn != null)
                {
                    var targetFilter = TargetTable.Columns[keyColumn];
                    if (targetFilter != null)
                    {
                        var mappings = new Mappings()
                        {
                            new MapFilter(targetFilter, deltaValues.AutoIncrementValue)
                        };
                        target = new TransformFilter(targetReader, mappings);
                    }
                }

                // if the update strategy is reload, change it to append to avoid the delta pushing a truncate on every set of child rows.
                var childUpdateStrategy = updateStrategy == EUpdateStrategy.Reload
                    ? EUpdateStrategy.Append
                    : updateStrategy;

                if (updateStrategy != null)
                {
                    var autoIncrementKey = await GetIncrementalKey(cancellationToken);

                    transform = new TransformDelta(transform, target, childUpdateStrategy.Value, autoIncrementKey,
                        AddDefaultRow, false, deltaValues);
                    transform.SetEncryptionMethod(EEncryptionMethod.EncryptDecryptSecureFields,
                        WriterOptions?.GlobalSettings?.EncryptionKey);

                    if (!await transform.Open(WriterResult?.AuditKey ?? 0, null, cancellationToken = default))
                    {
                        throw new TransformWriterException("Failed to open the data reader.");
                    }
                }

                // if parent operation is create/update then parent key hasn't changed, so process the records normally.
                await ProcessRecords(transform, target, transactionReference, childUpdateStrategy, cancellationToken);
            }
        }

        private async Task ProcessRecords(Transform transform, Transform targetTransform, int transactionReference, EUpdateStrategy? updateStrategy, CancellationToken cancellationToken = default)
        {
            var firstRead = true;

            try
            {
                while (await transform.ReadAsync(cancellationToken))
                {
                    if (firstRead && transactionReference <= 0)
                    {
                        if (WriterResult != null)
                        {
                            var runStatusResult = WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running,
                                null, null);
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
                        throw new TaskCanceledException();
                    }
                }
            }
            catch (TaskCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new TransformWriterException($"Failed to process records for the target table {targetTransform.Name}.  Message: {ex.Message}", ex);
            }

        }


        /// <summary>
        /// Caches a records from the inTransform.
        /// 
        /// </summary>
        /// <param name="transform"></param>
        /// <param name="updateStrategy"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="transactionReference"></param>
        /// <returns>The operation, and the tableCache if the rows have been exceeded.</returns>
        private async Task WriteRecord(Transform transform, int transactionReference, EUpdateStrategy? updateStrategy, CancellationToken cancellationToken = default)
        {
            // initialize the ordinal lookups if this is the first write.
            if (!_ordinalsInitialized)
            {
                try
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

                    _operationOrdinal = transform.CacheTable.GetOrdinal(EDeltaType.DatabaseOperation);
                    _ordinalsInitialized = true;
                }
                catch (Exception ex)
                {
                    throw new TransformWriterException($"Failed to initialize the column ordinals.  Message: {ex.Message}", ex);
                }
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
                    var rejectColumn = transform.CacheTable.GetOrdinal(EDeltaType.RejectedReason);
                    var rejectReason = rejectColumn > 0 ? transform[rejectColumn].ToString() : "No reject reason found.";
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
                        if (table.Columns[i].DeltaType != EDeltaType.DbAutoIncrement)
                        {
                            row[i] = transform[ordinal]; // TargetConnection.ConvertForWrite(table.Columns[i], transform[ordinal]).value;
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


            try
            {
                var transaction = await _transformWriterTask.StartTransaction(transactionReference);

                var currentKey = await _transformWriterTask.AddRecord(operation, row, cancellationToken);

                // if the operation is a create, the newKey will be the latest.
                if (operation == 'C')
                {
                    _autoIncrementKey = currentKey;
                }

                //process childNodes
                foreach (var childWriterTarget in ChildWriterTargets)
                {
                    var childTransform = (TransformNode) transform[childWriterTarget.NodeName];
                    var keyColumn = TargetTable.GetAutoIncrementColumn();

                    var deltaValues = transform.GetDeltaValues();
                    deltaValues.AutoIncrementValue = currentKey;

                    await childWriterTarget.WriteChildRecordsAsync(childTransform, keyColumn, deltaValues, 
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

        private async Task FinishTasks(Transform inTransform, CancellationToken cancellationToken = default)
        {
            try
            {
                if (_transformWriterTask != null)
                {
                    await _transformWriterTask.FinalizeWrites(cancellationToken);
                }
            }
            catch (Exception ex)
            {
                WriterResult?.SetRunStatus(TransformWriterResult.ERunStatus.Failed,
                    $"Failed to read record.  Message: {ex.Message}", ex);
                throw;
            }

            if (!string.IsNullOrEmpty(ProfileTableName) && inTransform != null)
            {
                var profileTable = inTransform.GetProfileTable(ProfileTableName);
                var profileResults = inTransform.GetProfileResults();
                if (profileResults != null)
                {
                    var profileExists = await ProfileConnection.TableExists(profileTable, cancellationToken);
                    if (!profileExists)
                    {
                        await ProfileConnection.CreateTable(profileTable, false, cancellationToken);
                    }

                    WriterResult.ProfileTableName = profileTable.Name;

                    try
                    {
                        await ProfileConnection.ExecuteInsertBulk(profileTable, profileResults, cancellationToken);
                    }
                    catch(Exception ex)
                    {
                        var message = $"Failed to save profile results.  {ex.Message}";
                        var newException = new TransformWriterException(message, ex);
                        WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException);
                        return;
                    }
                }
            }

            try
            {
                if (TargetConnection != null)
                {
                    await TargetConnection?.DataWriterFinish(TargetTable);
                }
            }
            catch(Exception ex)
            {
                var newEx = new TransformWriterException($"Failed finishing final write tasks.  Message: {ex.Message}", ex);
                throw newEx;
            }
        }

        private async Task UpdateWriterResult(Transform inTransform, CancellationToken cancellationToken = default)
        {
            if (WriterResult != null)
            {
                //update the statistics.
                WriterResult.RowsFiltered += inTransform.TotalRowsFiltered;
                WriterResult.RowsSorted += inTransform.TotalRowsSorted;
                WriterResult.RowsRejected += inTransform.TotalRowsRejected;
                WriterResult.RowsPreserved += inTransform.TotalRowsPreserved;
                WriterResult.RowsIgnored += inTransform.TotalRowsIgnored;
                WriterResult.RowsReadPrimary = inTransform.TotalRowsReadPrimary;
                WriterResult.RowsReadReference += inTransform.TotalRowsReadReference;

                //calculate the throughput figures
                var rowsWritten = WriterResult.RowsTotal - WriterResult.RowsIgnored;

                var performance = inTransform.PerformanceSummary();
                if (performance != null)
                {
                    performance.Add(new TransformPerformance(TargetTable?.Name, rowsWritten,
                        _transformWriterTask?.WriteDataTicks.TotalSeconds ?? 0));

                    WriterResult.PerformanceSummary = performance;
                }

                WriterResult.WriteTicks += _transformWriterTask?.WriteDataTicks.Ticks ?? 0;
                WriterResult.ReadTicks += inTransform.ReaderTimerTicks().Ticks;
                WriterResult.ProcessingTicks += inTransform.ProcessingTimerTicks().Ticks;

                if (WriterResult.RowsTotal == 0)
                    WriterResult.MaxIncrementalValue = WriterResult.LastMaxIncrementalValue;
                else
                    WriterResult.MaxIncrementalValue = inTransform.GetMaxIncrementalValue();

            }
            
            // update the autoincrement value for databases which don't have native datatype.
            var autoIncrementValue = inTransform.AutoIncrementValue;
            if (autoIncrementValue > 0)
            {
                var surrogateKey = TargetTable.GetAutoIncrementColumn();
                if (surrogateKey != null)
                {
                    await TargetConnection.UpdateMaxValue(TargetTable, surrogateKey.Name, autoIncrementValue, cancellationToken);
                }
            }

            var maxValidFrom = inTransform.MaxValidTo;
            if (maxValidFrom > DateTime.MinValue)
            {
                var validFrom = TargetTable.GetColumn(EDeltaType.ValidFromDate);
                if (validFrom != null)
                {
                    await TargetConnection.UpdateMaxValue(TargetTable, validFrom.Name, maxValidFrom, cancellationToken);
                }
            }

            // inTransform.Dispose();
        }

        private async Task WriterTargetFinalize(Transform transform, CancellationToken cancellationToken = default)
        {
            await FinishTasks(transform, cancellationToken);

            if (WriterResult != null)
            {
                if (WriterResult.IsRunning)
                {
                    WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null);
                }
                await WriterResult.CompleteDatabaseWrites();
            }

            foreach (var childWriterTarget in ChildWriterTargets)
            {
                await childWriterTarget.WriterTargetFinalize(transform, cancellationToken);
            }
            
        }

        private void Writer_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(writer);
        }

        public void Writer_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(writer);
        }

        private void Writer_OnFinish(TransformWriterResult writer)
        {
            OnFinish?.Invoke(writer);
        }

        private void Reader_ReadProgress(long rows)
        {
            if (rows == 1)
            {
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running, "", null);
                return;
            }
            WriterResult.IncrementRowsReadPrimary(1);
        }
        
        public bool SetRunStatus(TransformWriterResult.ERunStatus newStatus, string message, Exception exception, CancellationToken cancellationToken = default)
        {
            var result = WriterResult == null || WriterResult.SetRunStatus(newStatus, message, exception);

            foreach (var childItem in ChildWriterTargets)
            {
                result = result && childItem.SetRunStatus(newStatus, message, exception, cancellationToken);
            }
            
            return result;
        }

        public void Dispose()
        {
            _transformWriterTask?.Dispose();
            WriterResult?.Dispose();

            if (ChildWriterTargets != null)
            {
                foreach (var childWriter in ChildWriterTargets)
                {
                    childWriter.Dispose();
                }
            }
        }
    }
}