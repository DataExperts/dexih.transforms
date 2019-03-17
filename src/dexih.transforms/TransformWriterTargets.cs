//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Runtime.InteropServices.WindowsRuntime;
//using System.Threading;
//using System.Threading.Tasks;
//using dexih.functions;
//using dexih.transforms.Exceptions;
//
//namespace dexih.transforms
//{
//    public class TransformWriterTargets
//    {
//        #region Events
//        public delegate void ProgressUpdate(TransformWriterResult transformWriterResult);
//        public delegate void StatusUpdate(TransformWriterResult transformWriterResult);
//        public delegate void Finish(TransformWriterResult transformWriterResult);
//
//        public event ProgressUpdate OnProgressUpdate;
//        public event StatusUpdate OnStatusUpdate;
//        public event Finish OnFinish;
//
//        #endregion
//        
//        private bool _writeOpen;
//        
//        public string NodeName { get; set; }
//        public Transform Transform { get; set; }
//        public TransformWriterTargets Parent { get; set; }
//        
//        public TransformWriterResult WriterResult { get; set; }
//        
//        /// <summary>
//        /// Items in the current node.
//        /// </summary>
//        public List<TransformWriterTarget> Items { get; } = new List<TransformWriterTarget>();
//        public List<TransformWriterTargets> ChildNodes { get; set; } = new List<TransformWriterTargets>();
//
//        public void Add(TransformWriterTarget transformWriterTarget)
//        {
//            var path = transformWriterTarget.NodePath;
//            var childNodes = ChildNodes;
//
//            if (path == null || path.Length == 0)
//            {
//                if (transformWriterTarget.WriterResult != null)
//                {
//                    WriterResult.ChildResults.Add(transformWriterTarget.WriterResult);
//                }
//                
//                Items.Add(transformWriterTarget);
//                return;
//            }
//
//            TransformWriterTargets childNode = null;
//            TransformWriterTargets parentNode = this;
//
//            var parentWriterResult = WriterResult;
//            
//            foreach (var item in path)
//            {
//                childNode = childNodes.SingleOrDefault(c => c.NodeName == item);
//                if (childNode == null)
//                {
//                    childNode = new TransformWriterTargets {Parent = parentNode, NodeName = item, WriterResult = new TransformWriterResult()};
//                    ChildNodes.Add(childNode);
//                }
//
//                if (childNode.WriterResult != null)
//                {
//                    parentWriterResult = childNode.WriterResult;
//                }
//
//                childNodes = childNode.ChildNodes;
//                parentNode = childNode;
//            }
//
//            if (transformWriterTarget.WriterResult != null)
//            {
//                parentWriterResult.ChildResults.Add(transformWriterTarget.WriterResult);
//            }
//            
//            childNode.Items.Add(transformWriterTarget);
//        }
//        
//        /// <summary>
//        /// Get all added TransformWriterTarget items in the hierarchy
//        /// </summary>
//        /// <returns></returns>
//        public List<TransformWriterTarget> GetAll()
//        {
//            var items = Items.ToList();
//
//            foreach (var childNode in ChildNodes)
//            {
//                items.AddRange(childNode.GetAll());
//            }
//
//            return items;
//        }
//
//        public long? GetSurrogateKey(string name)
//        {
//            if (Parent == null) return null;
//
//            foreach (var item in Parent.GetAll())
//            {
//                if (item.KeyName == name)
//                {
//                    return item.KeyValue;
//                }
//            }
//
//            return Parent.GetSurrogateKey(name);
//        }
//
//        /// <summary>
//        /// Initialize all the writer results, and set the progress events.
//        /// </summary>
//        /// <param name="parentAuditKey"></param>
//        /// <param name="auditConnection"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public async Task<bool> Initialize(CancellationToken cancellationToken)
//        {
//            var result = true;
//            
//            if (WriterResult != null)
//            {
//                await WriterResult.Initialize(cancellationToken);
//                result = WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Initialised, null, null, cancellationToken);
//                WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
//                WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
//                WriterResult.OnFinish += Datalink_OnFinish;
//            }
//            
//            foreach (var item in Items)
//            {
//                if (item.WriterResult != null)
//                {
//                    if (item.WriterOptions?.ResetIncremental ?? false)
//                    {
//                        item.WriterResult.LastMaxIncrementalValue = item.WriterOptions?.ResetIncrementalValue ?? 0;
//                        
//                    }
//
//                    await item.WriterResult.Initialize(cancellationToken);
//                    result = result && item.WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Initialised, null, null, cancellationToken);
//                    item.WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
//                    item.WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
//                    item.WriterResult.OnFinish += Datalink_OnFinish;
//                }
//            }
//
//            if (ChildNodes == null) return result;
//            
//            foreach (var childItem in ChildNodes)
//            {
//                result = result && await childItem.Initialize(cancellationToken);
//                childItem.OnStatusUpdate += Datalink_OnStatusUpdate;
//                childItem.OnProgressUpdate += Datalink_OnProgressUpdate;
//                childItem.OnFinish += Datalink_OnFinish;
//            }
//
//            return result;
//        }
//
//        public bool SetRunStatus(TransformWriterResult.ERunStatus newStatus, string message, Exception exception, CancellationToken cancellationToken)
//        {
//            var result = WriterResult == null || WriterResult.SetRunStatus(newStatus, message, exception, cancellationToken);
//            
//            foreach (var item in Items)
//            {
//                item.WriterResult.ParentAuditKey = WriterResult?.AuditKey ?? 0;
//                result = result && item.WriterResult.SetRunStatus(newStatus, message, exception, cancellationToken);
//            }
//
//            if (ChildNodes != null)
//            {
//                foreach (var childItem in ChildNodes)
//                {
//                    result = result && childItem.SetRunStatus(newStatus, message, exception, cancellationToken);
//                }
//            }
//
//            return result;
//        }
//
//       public async Task Finalize()
//        {
//            await WriterResult.Finalize();
//            
//            foreach (var item in Items)
//            {
//                item.WriterResult.ParentAuditKey = WriterResult?.AuditKey ?? 0;
//                await item.WriterResult.Finalize();
//            }
//
//            if (ChildNodes != null)
//            {
//                foreach (var childItem in ChildNodes)
//                {
//                    await childItem.Finalize();
//                }
//            }
//        }
//       
//         /// <summary>
//        /// Writes the records for the writerTargets.  Also adds delta transforms based on the update strategy.
//        /// </summary>
//        /// <param name="inTransform"></param>
//        /// <param name="writerTargets"></param>
//        /// <param name="updateStrategy"></param>
//        /// <param name="addDefaultRow"></param>
//        /// <param name="encryptionKey"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        /// <exception cref="TransformWriterException"></exception>
//        public async Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTargets writerTargets, TransformDelta.EUpdateStrategy updateStrategy, bool addDefaultRow, string encryptionKey, CancellationToken cancellationToken)
//        {
//            try
//            {
//                writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
//
//                var writeTasks = new List<Task<bool>>();
//
//                if (writerTargets.Items.Count > 1 && writerTargets.ChildNodes.Count > 0)
//                {
//                    throw new TransformWriterException("Only one top node writer can be used when there are child node writers.");
//                }
//
//                // create a delta for each top level item
//                foreach (var target in writerTargets.Items)
//                {
//                    var targetReader = target.TargetConnection.GetTransformReader(target.TargetTable);
//
//                    Transform sourceReader;
//                    if (writerTargets.Items.Count > 1)
//                    {
//                        sourceReader = inTransform.GetThread();
//                    }
//                    else
//                    {
//                        sourceReader = inTransform;
//                    }
//
//                    var transformDelta = new TransformDelta(sourceReader, targetReader,
//                        updateStrategy, target.CurrentAutoIncrementKey, addDefaultRow);
//
//                    transformDelta.SetEncryptionMethod(Transform.EEncryptionMethod.EncryptDecryptSecureFields,
//                        encryptionKey);
//
//                    if (!await transformDelta.Open(target.WriterResult?.AuditKey ?? 0, null, cancellationToken))
//                    {
//                        throw new TransformWriterException("Failed to open the data reader.");
//                    }
//
//                    var writer = new TransformWriter();
//                    var targets = new TransformWriterTargets {WriterResult = writerTargets.WriterResult};
//                    targets.Add(target);
//
//                    targets.ChildNodes = writerTargets.ChildNodes;
//
//                    writeTasks.Add(writer.WriteRecordsAsync(transformDelta, targets, cancellationToken));
//                }
//
//                await Task.WhenAll(writeTasks);
//
//                foreach (var task in writeTasks)
//                {
//                    var runDatalink = task.Result;
//
//                    if (!runDatalink)
//                    {
//                        if (writerTargets.WriterResult.RunStatus == TransformWriterResult.ERunStatus.Abended)
//                        {
//                            throw new TransformWriterException($"Running datalink failed.");
//                        }
//
//                        if (writerTargets.WriterResult.RunStatus == TransformWriterResult.ERunStatus.Cancelled)
//                        {
//                            throw new TransformWriterException($"Running datalink was cancelled.");
//                        }
//
//                        throw new TransformWriterException("Datalink ended unexpectedly.");
//                    }
//                }
//
//                return true;
//            }
//            finally
//            {
//                await writerTargets.Finalize();
//            }
//        }
//        
//        /// <summary>
//        /// Writes all record from the inTransform to the target table and reject table.
//        /// </summary>
//        /// <param name="inTransform">Transform to read data from</param>
//        /// <param name="writerTargets"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public async Task<bool> WriteRecordsAsync(Transform inTransform, CancellationToken cancellationToken)
//        {
//            var updateResult = SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
//
//            if (!updateResult)
//            {
//                return false;
//            }
//
//            try
//            {
//                await WriteStart(cancellationToken);
//            }
//            catch (Exception ex)
//            {
//                var message = $"The transform writer failed to start.  {ex.Message}";
//                var newException = new TransformWriterException(message, ex);
//                SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
//                return false;
//            }
//
//            var firstRead = true;
//
//            Transform = inTransform;
//
//            while (await inTransform.ReadAsync(cancellationToken))
//            {
//                if (firstRead)
//                {
//                    var runStatusResult = SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, cancellationToken);
//                    if (!runStatusResult)
//                    {
//                        return false;
//                    }
//                    firstRead = false;
//                }
//
//                await WriteRecord(cancellationToken);
//
//                if (cancellationToken.IsCancellationRequested)
//                {
//                    var runStatusResult = SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, cancellationToken);
//                    return runStatusResult;
//                }
//            }
//
//            await WriteFinalize(cancellationToken);
//            
//            var setRunStatusResult = SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, cancellationToken);
//
//            await Finalize();
//            
//            return setRunStatusResult;
//        }
//        
//        public async Task WriteStart(CancellationToken cancellationToken)
//        {
//
//            if (_writeOpen)
//            {
//                throw new TransformWriterException("Transform write failed to start, as a previous operation is still running.");
//            }
//
//            // create any missing tables.
//            foreach (var writerTarget in GetAll())
//            {
//                await writerTarget.WriterInitialize(cancellationToken);
//            }
//
//            _writeOpen = true;
//
//        }
//
//        public async Task WriteRecord(CancellationToken cancellationToken)
//        {
//            if (_writeOpen == false)
//            {
//                throw new TransformWriterException($"Transform write failed to write record as the WriteStart has not been called.");
//            }
//
//            // insert the row for the current writer targets
//            foreach (var writerTarget in Items)
//            {
//                await writerTarget.WriteRecord(this, cancellationToken);
//            }
//
//            if (ChildNodes == null || ChildNodes.Count == 0)
//            {
//                return;
//            }
//
//            // loop through any child nodes, and recurse to write more records.
//            foreach (var writerChild in ChildNodes)
//            {
//                var transform = (Transform) Transform[writerChild.NodeName];
//                if (transform != null)
//                {
//                    await transform.Open(cancellationToken);
//                    writerChild.Transform = transform;
//
//                    while (await transform.ReadAsync(cancellationToken))
//                    {
//                        await writerChild.WriteRecord(cancellationToken);
//                    }
//
//                    await writerChild.WriteFinalize(cancellationToken);
//                }
//            }
//        }
//
//        public async Task WriteFinalize(CancellationToken cancellationToken)
//        {
//            // insert the row for the current writer targets
//            foreach (var writerTarget in Items)
//            {
//                await writerTarget.WriterFinalize(Transform, cancellationToken);
//            }
//
//            _writeOpen = false;
//        }
//        
//        public void Datalink_OnProgressUpdate(TransformWriterResult writer)
//        {
//            UpdateStats();
//            OnProgressUpdate?.Invoke(writer);
//        }
//
//        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
//        {
//            UpdateStats();
//            OnStatusUpdate?.Invoke(writer);
//        }
//        
//        public void Datalink_OnFinish(TransformWriterResult writer)
//        {
//            UpdateStats();
//            OnFinish?.Invoke(writer);
//        }
//
//        public void UpdateStats()
//        {
//            WriterResult.RowsTotal = 0;
//            WriterResult.Failed = 0;
//            WriterResult.Passed = 0;
//            WriterResult.ReadTicks = 0;
//            WriterResult.WriteTicks = 0;
//            WriterResult.RowsSorted = 0;
//            WriterResult.RowsCreated = 0;
//            WriterResult.RowsDeleted = 0;
//            WriterResult.RowsIgnored = 0;
//            WriterResult.RowsUpdated = 0;
//            WriterResult.RowsFiltered = 0;
//            WriterResult.RowsRejected = 0;
//            WriterResult.RowsPreserved = 0;
//            WriterResult.RowsReadPrimary = 0;
//            WriterResult.RowsReadReference = 0;
//
//            foreach (var target in Items)
//            {
//                WriterResult.RowsTotal += target.WriterResult.RowsTotal;
//                WriterResult.Failed += target.WriterResult.Failed;
//                WriterResult.Passed += target.WriterResult.Passed;
//                WriterResult.ReadTicks += target.WriterResult.ReadTicks;
//                WriterResult.WriteTicks += target.WriterResult.WriteTicks;
//                WriterResult.RowsSorted += target.WriterResult.RowsSorted;
//                WriterResult.RowsCreated += target.WriterResult.RowsCreated;
//                WriterResult.RowsDeleted += target.WriterResult.RowsDeleted;
//                WriterResult.RowsIgnored += target.WriterResult.RowsIgnored;
//                WriterResult.RowsUpdated += target.WriterResult.RowsUpdated;
//                WriterResult.RowsFiltered += target.WriterResult.RowsFiltered;
//                WriterResult.RowsRejected += target.WriterResult.RowsRejected;
//                WriterResult.RowsPreserved += target.WriterResult.RowsPreserved;
//                WriterResult.RowsReadPrimary += target.WriterResult.RowsReadPrimary;
//                WriterResult.RowsReadReference += target.WriterResult.RowsReadReference;
//            }
//
//            foreach (var node in ChildNodes)
//            {
//                WriterResult.RowsTotal += node.WriterResult.RowsTotal;
//                WriterResult.Failed += node.WriterResult.Failed;
//                WriterResult.Passed += node.WriterResult.Passed;
//                WriterResult.ReadTicks += node.WriterResult.ReadTicks;
//                WriterResult.WriteTicks += node.WriterResult.WriteTicks;
//                WriterResult.RowsSorted += node.WriterResult.RowsSorted;
//                WriterResult.RowsCreated += node.WriterResult.RowsCreated;
//                WriterResult.RowsDeleted += node.WriterResult.RowsDeleted;
//                WriterResult.RowsIgnored += node.WriterResult.RowsIgnored;
//                WriterResult.RowsUpdated += node.WriterResult.RowsUpdated;
//                WriterResult.RowsFiltered += node.WriterResult.RowsFiltered;
//                WriterResult.RowsRejected += node.WriterResult.RowsRejected;
//                WriterResult.RowsPreserved += node.WriterResult.RowsPreserved;
//                WriterResult.RowsReadPrimary += node.WriterResult.RowsReadPrimary;
//                WriterResult.RowsReadReference += node.WriterResult.RowsReadReference;
//            }
//        }
//        
//    }
//}