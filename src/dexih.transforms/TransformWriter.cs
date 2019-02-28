using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    /// <summary>
    /// The base class for writing rows from a transform to target tables.
    /// </summary>
    public class TransformWriter
    {
        private bool _writeOpen;

        public Task<bool> WriteRecordsAsync(TransformWriterResult writerResult, Transform inTransform, TransformWriterTarget.ETransformWriterMethod transformWriterMethod,  Table targetTable, Connection targetConnection, CancellationToken cancellationToken)
        {
            return WriteRecordsAsync(writerResult, inTransform, transformWriterMethod, targetTable, targetConnection, null, null, 1000, cancellationToken);
        }

        public Task<bool> WriteRecordsAsync(TransformWriterResult writerResult, Transform inTransform, TransformWriterTarget.ETransformWriterMethod transformWriterMethod,  Table targetTable, Connection targetConnection, int commitSize, CancellationToken cancellationToken)
        {
            return WriteRecordsAsync(writerResult, inTransform, transformWriterMethod, targetTable, targetConnection, null, null, commitSize, cancellationToken);
        }

        public Task<bool> WriteRecordsAsync(TransformWriterResult writerResult, Transform inTransform, TransformWriterTarget.ETransformWriterMethod transformWriterMethod, Table targetTable, Connection targetConnection,
             Connection rejectConnection, Table rejectTable, int commitSize, CancellationToken cancellationToken)
        {
            var writerTarget = new TransformWriterTarget(transformWriterMethod, writerResult, targetConnection, targetTable, rejectConnection, rejectTable, commitSize);
            return WriteRecordsAsync(inTransform, writerTarget, cancellationToken);
        }
        
        public Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTarget writerTarget, CancellationToken cancellationToken)
        {
            var writerTargets = new TransformWriterTargets();
            writerTargets.Add(writerTarget);
            return WriteRecordsAsync(inTransform, writerTargets, cancellationToken);
        }
        
        /// <summary>
        /// Writes all record from the inTransform to the target table and reject table.
        /// </summary>
        /// <param name="inTransform">Transform to read data from</param>
        /// <param name="writerTargets"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTargets writerTargets,
            CancellationToken cancellationToken)
        {
            var updateResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
            if (!updateResult)
            {
                return false;
            }

            try
            {
                
                await WriteStart(inTransform, writerTargets, cancellationToken);
            }
            catch (Exception ex)
            {
                var message = $"The transform writer failed to start.  {ex.Message}";
                var newException = new TransformWriterException(message, ex);
                await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
                return false;
            }

            var firstRead = true;

            writerTargets.Transform = inTransform;

            while (await inTransform.ReadAsync(cancellationToken))
            {
                if (firstRead)
                {
                    var runStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, CancellationToken.None);
                    if (!runStatusResult)
                    {
                        return false;
                    }
                    firstRead = false;
                }

                await WriteRecord(writerTargets, cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                {
                    var runStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, CancellationToken.None);
                    return runStatusResult;
                }
            }

            await WriteFinalize(writerTargets, cancellationToken);
            
            var setRunStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, CancellationToken.None);
            return setRunStatusResult;
        }
        
        public async Task WriteStart(Transform inTransform, TransformWriterTargets writerTargets, CancellationToken cancellationToken)
        {

            if (_writeOpen)
            {
                throw new TransformWriterException("Transform write failed to start, as a previous operation is still running.");
            }

            // create any missing tables.
            foreach (var writerTarget in writerTargets.GetAll())
            {
                await writerTarget.WriterInitialize(cancellationToken);
            }

            _writeOpen = true;

        }

        public async Task WriteRecord(TransformWriterTargets writerTargets, CancellationToken cancellationToken)
        {
            if (_writeOpen == false)
            {
                throw new TransformWriterException($"Transform write failed to write record as the WriteStart has not been called.");
            }

            // insert the row for the current writer targets
            foreach (var writerTarget in writerTargets.Items)
            {
                await writerTarget.WriteRecord(writerTargets, cancellationToken);
            }

            if (writerTargets.ChildNodes == null || writerTargets.ChildNodes.Count == 0)
            {
                return;
            }

            // loop through any child nodes, and recurse to write more records.
            foreach (var writerChild in writerTargets.ChildNodes)
            {
                var transform = (Transform) writerTargets.Transform[writerChild.NodeName];
                await transform.Open(cancellationToken);
                writerChild.Transform = transform;

                while (await transform.ReadAsync(cancellationToken))
                {
                    await WriteRecord(writerChild, cancellationToken);
                }
            }
        }

        public async Task WriteFinalize(TransformWriterTargets writerTargets, CancellationToken cancellationToken)
        {
            // insert the row for the current writer targets
            foreach (var writerTarget in writerTargets.Items)
            {
                await writerTarget.WriterFinalize(writerTargets.Transform, cancellationToken);
            }

            if (writerTargets.ChildNodes == null || writerTargets.ChildNodes.Count == 0)
            {
                _writeOpen = false;
                return;
            }

            // loop through any child nodes, and recurse to write more records.
            if (!writerTargets.Transform.IsReaderFinished)
            {
                foreach (var writerChild in writerTargets.ChildNodes)
                {
                    var transform = (Transform) writerTargets.Transform[writerChild.NodeName];
                    await transform.Open(cancellationToken);
                    writerChild.Transform = transform;

                    while (await transform.ReadAsync(cancellationToken))
                    {
                        await WriteFinalize(writerChild, cancellationToken);
                    }
                }
            }

            _writeOpen = false;
        }
        
    }
}