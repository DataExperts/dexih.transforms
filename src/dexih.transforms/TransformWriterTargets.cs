using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public class TransformWriterTargets
    {
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult transformWriterResult);
        public delegate void StatusUpdate(TransformWriterResult transformWriterResult);
        public delegate void Finish(TransformWriterResult transformWriterResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        public event Finish OnFinish;

        #endregion
        
        public string NodeName { get; set; }
        public Transform Transform { get; set; }
        public TransformWriterTargets Parent { get; set; }
        
        public TransformWriterResult WriterResult { get; set; }
        
        /// <summary>
        /// Items in the current node.
        /// </summary>
        public List<TransformWriterTarget> Items { get; } = new List<TransformWriterTarget>();
        public List<TransformWriterTargets> ChildNodes { get; } = new List<TransformWriterTargets>();

        public void Add(TransformWriterTarget transformWriterTarget)
        {
            var path = transformWriterTarget.ColumnPath;
            var childNodes = ChildNodes;

            if (path == null || path.Length == 0)
            {
                Items.Add(transformWriterTarget);
                return;
            }

            TransformWriterTargets childNode = null;
            TransformWriterTargets parentNode = this;
            
            foreach (var item in path)
            {
                childNode = childNodes.SingleOrDefault(c => c.NodeName == item);
                if (childNode == null)
                {
                    childNode = new TransformWriterTargets {Parent = parentNode, NodeName = item};
                    ChildNodes.Add(childNode);
                }
                childNodes = childNode.ChildNodes;
                parentNode = childNode;
            }
            
            childNode.Items.Add(transformWriterTarget);
        }
        
        /// <summary>
        /// Get all added TransformWriterTarget items in the hierarchy
        /// </summary>
        /// <returns></returns>
        public List<TransformWriterTarget> GetAll()
        {
            var items = Items.ToList();

            foreach (var childNode in ChildNodes)
            {
                items.AddRange(childNode.GetAll());
            }

            return items;
        }

        public long? GetSurrogateKey(string name)
        {
            if (Parent == null) return null;

            foreach (var item in Parent.GetAll())
            {
                if (item.KeyName == name)
                {
                    return item.KeyValue;
                }
            }

            return Parent.GetSurrogateKey(name);
        }

        /// <summary>
        /// Initialize all the writer results, and set the progress events.
        /// </summary>
        /// <param name="parentAuditKey"></param>
        /// <param name="auditConnection"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> Initialize(long parentAuditKey, Connection auditConnection, CancellationToken cancellationToken)
        {
            var result = true;
            
            if (WriterResult != null)
            {
                WriterResult.ParentAuditKey = parentAuditKey;
                if(WriterResult.ResetIncremental) WriterResult.LastMaxIncrementalValue = WriterResult.ResetIncrementalValue;
                
                await auditConnection.InitializeAudit(WriterResult, cancellationToken);
                result = await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Initialised, null, null, cancellationToken);
                WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
                WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
                WriterResult.OnFinish += Datalink_OnFinish;
            }
            
            foreach (var item in Items)
            {
                if (item.WriterResult != null)
                {
                    item.WriterResult.ParentAuditKey = WriterResult?.AuditKey ?? parentAuditKey;
                    if(item.WriterResult.ResetIncremental) item.WriterResult.LastMaxIncrementalValue = item.WriterResult.ResetIncrementalValue;

                    await auditConnection.InitializeAudit(WriterResult, cancellationToken);
                    result = result && await item.WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Initialised, null, null, cancellationToken);
                    item.WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
                    item.WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
                    item.WriterResult.OnFinish += Datalink_OnFinish;
                }
            }

            if (ChildNodes == null) return result;
            
            foreach (var childItem in ChildNodes)
            {
                result = result && await childItem.Initialize(WriterResult?.AuditKey ?? parentAuditKey, auditConnection, cancellationToken);
                childItem.OnStatusUpdate += Datalink_OnStatusUpdate;
                childItem.OnProgressUpdate += Datalink_OnProgressUpdate;
                childItem.OnFinish += Datalink_OnFinish;
            }

            return result;
        }

        public async Task<bool> SetRunStatus(TransformWriterResult.ERunStatus newStatus, string message, Exception exception, CancellationToken cancellationToken)
        {
            var result = WriterResult == null || await WriterResult.SetRunStatus(newStatus, message, exception, cancellationToken);
            
            foreach (var item in Items)
            {
                result = result && await item.WriterResult.SetRunStatus(newStatus, message, exception, cancellationToken);
            }

            if (ChildNodes != null)
            {
                foreach (var childItem in ChildNodes)
                {
                    result = result && await childItem.SetRunStatus(newStatus, message, exception, cancellationToken);
                }
            }

            return result;
        }
        
        public void Datalink_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(writer);
        }

        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(writer);
        }
        
        public void Datalink_OnFinish(TransformWriterResult writer)
        {
            OnFinish?.Invoke(writer);
        }
        
    }
}