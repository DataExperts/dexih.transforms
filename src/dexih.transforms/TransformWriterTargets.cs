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
                if (transformWriterTarget.WriterResult != null)
                {
                    WriterResult.ChildResults.Add(transformWriterTarget.WriterResult);
                }
                
                Items.Add(transformWriterTarget);
                return;
            }

            TransformWriterTargets childNode = null;
            TransformWriterTargets parentNode = this;

            var parentWriterResult = WriterResult;
            
            foreach (var item in path)
            {
                childNode = childNodes.SingleOrDefault(c => c.NodeName == item);
                if (childNode == null)
                {
                    childNode = new TransformWriterTargets {Parent = parentNode, NodeName = item};
                    ChildNodes.Add(childNode);
                }

                if (childNode.WriterResult != null)
                {
                    parentWriterResult = childNode.WriterResult;
                }

                childNodes = childNode.ChildNodes;
                parentNode = childNode;
            }

            if (transformWriterTarget.WriterResult != null)
            {
                parentWriterResult.ChildResults.Add(transformWriterTarget.WriterResult);
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
        public async Task<bool> Initialize(CancellationToken cancellationToken)
        {
            var result = true;
            
            if (WriterResult != null)
            {
                if(WriterResult.ResetIncremental) WriterResult.LastMaxIncrementalValue = WriterResult.ResetIncrementalValue;

                await WriterResult.Initialize(cancellationToken);
                result = await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Initialised, null, null, cancellationToken);
                WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
                WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
                WriterResult.OnFinish += Datalink_OnFinish;
            }
            
            foreach (var item in Items)
            {
                if (item.WriterResult != null)
                {
                    await item.WriterResult.Initialize(cancellationToken);
                    result = result && await item.WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Initialised, null, null, cancellationToken);
                    item.WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
                    item.WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
                    item.WriterResult.OnFinish += Datalink_OnFinish;
                }
            }

            if (ChildNodes == null) return result;
            
            foreach (var childItem in ChildNodes)
            {
                result = result && await childItem.Initialize(cancellationToken);
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
                item.WriterResult.ParentAuditKey = WriterResult?.AuditKey ?? 0;
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
            UpdateStats();
            OnProgressUpdate?.Invoke(writer);
        }

        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
        {
            UpdateStats();
            OnStatusUpdate?.Invoke(writer);
        }
        
        public void Datalink_OnFinish(TransformWriterResult writer)
        {
            UpdateStats();
            OnFinish?.Invoke(writer);
        }

        public void UpdateStats()
        {
            WriterResult.RowsTotal = 0;
            WriterResult.Failed = 0;
            WriterResult.Passed = 0;
            WriterResult.ReadTicks = 0;
            WriterResult.WriteTicks = 0;
            WriterResult.RowsSorted = 0;
            WriterResult.RowsCreated = 0;
            WriterResult.RowsDeleted = 0;
            WriterResult.RowsIgnored = 0;
            WriterResult.RowsUpdated = 0;
            WriterResult.RowsFiltered = 0;
            WriterResult.RowsRejected = 0;
            WriterResult.RowsPreserved = 0;
            WriterResult.RowsReadPrimary = 0;
            WriterResult.RowsReadReference = 0;

            foreach (var target in Items)
            {
                WriterResult.RowsTotal += target.WriterResult.RowsTotal;
                WriterResult.Failed += target.WriterResult.Failed;
                WriterResult.Passed += target.WriterResult.Passed;
                WriterResult.ReadTicks += target.WriterResult.ReadTicks;
                WriterResult.WriteTicks += target.WriterResult.WriteTicks;
                WriterResult.RowsSorted += target.WriterResult.RowsSorted;
                WriterResult.RowsCreated += target.WriterResult.RowsCreated;
                WriterResult.RowsDeleted += target.WriterResult.RowsDeleted;
                WriterResult.RowsIgnored += target.WriterResult.RowsIgnored;
                WriterResult.RowsUpdated += target.WriterResult.RowsUpdated;
                WriterResult.RowsFiltered += target.WriterResult.RowsFiltered;
                WriterResult.RowsRejected += target.WriterResult.RowsRejected;
                WriterResult.RowsPreserved += target.WriterResult.RowsPreserved;
                WriterResult.RowsReadPrimary += target.WriterResult.RowsReadPrimary;
                WriterResult.RowsReadReference += target.WriterResult.RowsReadReference;
            }

            foreach (var node in ChildNodes)
            {
                WriterResult.RowsTotal += node.WriterResult.RowsTotal;
                WriterResult.Failed += node.WriterResult.Failed;
                WriterResult.Passed += node.WriterResult.Passed;
                WriterResult.ReadTicks += node.WriterResult.ReadTicks;
                WriterResult.WriteTicks += node.WriterResult.WriteTicks;
                WriterResult.RowsSorted += node.WriterResult.RowsSorted;
                WriterResult.RowsCreated += node.WriterResult.RowsCreated;
                WriterResult.RowsDeleted += node.WriterResult.RowsDeleted;
                WriterResult.RowsIgnored += node.WriterResult.RowsIgnored;
                WriterResult.RowsUpdated += node.WriterResult.RowsUpdated;
                WriterResult.RowsFiltered += node.WriterResult.RowsFiltered;
                WriterResult.RowsRejected += node.WriterResult.RowsRejected;
                WriterResult.RowsPreserved += node.WriterResult.RowsPreserved;
                WriterResult.RowsReadPrimary += node.WriterResult.RowsReadPrimary;
                WriterResult.RowsReadReference += node.WriterResult.RowsReadReference;
            }
        }
        
    }
}