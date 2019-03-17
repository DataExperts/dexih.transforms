//using System;
//using System.Collections.Generic;
//using System.Threading;
//using System.Threading.Tasks;
//using dexih.functions;
//using dexih.transforms.Exceptions;
//
//namespace dexih.transforms
//{
//    /// <summary>
//    /// The base class for writing rows from a transform to target tables.
//    /// </summary>
//    public class TransformWriter
//    {
//        private bool _writeOpen;
//
////        public Task<bool> WriteRecordsAsync(TransformWriterResult writerResult, Transform inTransform, TransformWriterTarget.ETransformWriterMethod transformWriterMethod,  Table targetTable, Connection targetConnection, string[] nodePath, CancellationToken cancellationToken)
////        {
////            return WriteRecordsAsync(writerResult, inTransform, transformWriterMethod, targetTable, targetConnection, null, null, nodePath, 1000, cancellationToken);
////        }
////
////        public Task<bool> WriteRecordsAsync(TransformWriterResult writerResult, Transform inTransform, TransformWriterTarget.ETransformWriterMethod transformWriterMethod,  Table targetTable, Connection targetConnection, string[] nodePath, int commitSize, CancellationToken cancellationToken)
////        {
////            return WriteRecordsAsync(writerResult, inTransform, transformWriterMethod, targetTable, targetConnection, null, null, nodePath, commitSize, cancellationToken);
////        }
////
////        public Task<bool> WriteRecordsAsync(TransformWriterResult writerResult, Transform inTransform, TransformWriterTarget.ETransformWriterMethod transformWriterMethod, Table targetTable, Connection targetConnection,
////             Connection rejectConnection, Table rejectTable, string[] nodePath, int commitSize, CancellationToken cancellationToken)
////        {
////            var writerTarget = new TransformWriterTarget(transformWriterMethod, writerResult, targetConnection, targetTable, rejectConnection, rejectTable, nodePath, commitSize);
////            return WriteRecordsAsync(inTransform, writerTarget, cancellationToken);
////        }
//        
//        public Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTarget writerTarget, CancellationToken cancellationToken)
//        {
//            var writerTargets = new TransformWriterTargets();
//            writerTargets.Add(writerTarget);
//            return WriteRecordsAsync(inTransform, writerTargets, cancellationToken);
//        }
//
//        /// <summary>
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
//        public async Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTargets writerTargets,
//            CancellationToken cancellationToken)
//        {
//            var updateResult = writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
//            if (!updateResult)
//            {
//                return false;
//            }
//
//            try
//            {
//                
//                await WriteStart(inTransform, writerTargets, cancellationToken);
//            }
//            catch (Exception ex)
//            {
//                var message = $"The transform writer failed to start.  {ex.Message}";
//                var newException = new TransformWriterException(message, ex);
//                writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
//                return false;
//            }
//
//            var firstRead = true;
//
//            writerTargets.Transform = inTransform;
//
//            while (await inTransform.ReadAsync(cancellationToken))
//            {
//                if (firstRead)
//                {
//                    var runStatusResult = writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, cancellationToken);
//                    if (!runStatusResult)
//                    {
//                        return false;
//                    }
//                    firstRead = false;
//                }
//
//                await WriteRecord(writerTargets, cancellationToken);
//
//                if (cancellationToken.IsCancellationRequested)
//                {
//                    var runStatusResult = writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, cancellationToken);
//                    return runStatusResult;
//                }
//            }
//
//            await WriteFinalize(writerTargets, cancellationToken);
//            
//            var setRunStatusResult = writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, cancellationToken);
//
//            await writerTargets.Finalize();
//            
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
//            // create any missing tables.
//            foreach (var writerTarget in writerTargets.GetAll())
//            {
//                await writerTarget.WriterInitialize(cancellationToken);
//            }
//
//            _writeOpen = true;
//
//        }
//
//        public async Task WriteRecord(TransformWriterTargets writerTargets, CancellationToken cancellationToken)
//        {
//            if (_writeOpen == false)
//            {
//                throw new TransformWriterException($"Transform write failed to write record as the WriteStart has not been called.");
//            }
//
//            // insert the row for the current writer targets
//            foreach (var writerTarget in writerTargets.Items)
//            {
//                await writerTarget.WriteRecord(writerTargets, cancellationToken);
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
//                if (transform != null)
//                {
//                    await transform.Open(cancellationToken);
//                    writerChild.Transform = transform;
//
//                    while (await transform.ReadAsync(cancellationToken))
//                    {
//                        await WriteRecord(writerChild, cancellationToken);
//                    }
//
//                    await WriteFinalize(writerChild, cancellationToken);
//                }
//            }
//        }
//
//        public async Task WriteFinalize(TransformWriterTargets writerTargets, CancellationToken cancellationToken)
//        {
//            // insert the row for the current writer targets
//            foreach (var writerTarget in writerTargets.Items)
//            {
//                await writerTarget.WriterFinalize(writerTargets.Transform, cancellationToken);
//            }
//
//            _writeOpen = false;
//        }
//        
//    }
//}