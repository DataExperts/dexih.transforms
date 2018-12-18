using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    /// <summary>
    /// Transform to flatten an array property into the parent row.
    /// </summary>
    [Transform(
        Name = "Flatten Node",
        Description = "Flatten an node into the repeating rows for the parent property.",
        TransformType = TransformAttribute.ETransformType.FlattenNode
    )]
    public class TransformFlattenNode : Transform
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="inTransform"></param>
        /// <param name="mappings"></param>
        /// <param name="node">The node to flatten</param>
        public TransformFlattenNode(Transform inTransform, Mappings mappings, TableColumn node)
        {
            _node = node;

            Mappings = mappings;
            SetInTransform(inTransform);
        }

        private int _nodeOrdinal;
        private bool _writeCache;
        private int _writeCachePosition;
        private TableColumn _node;

        private object[] _cacheRow;
        private Transform _childTransform;

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
            if (!returnValue) return false;

            // convert the array path to a sequence of ordinals, to improve performance
            _nodeOrdinal = PrimaryTransform.CacheTable.Columns.GetOrdinal(_node.Name);

            var flattenedColumns = new TableColumns();
            var sourceColumns = PrimaryTransform.CacheTable.Columns;

            for (var i = 0; i < sourceColumns.Count; i++)
            {
                if (i == _nodeOrdinal)
                {
                    var nodeColumn = sourceColumns[i];
                    if (nodeColumn.ChildColumns != null)
                    {
                        foreach (var childColumn in nodeColumn.ChildColumns)
                        {
                            var col = childColumn.Copy();
                            // add a column group to the flattened column to ensure duplicate names can be distinguished when flattened.
                            col.ColumnGroup = (string.IsNullOrEmpty(col.ColumnGroup) ? "" : ".") + nodeColumn.Name;
                            flattenedColumns.Add(col);
                        }
                    }
                }
                else
                {
                    flattenedColumns.Add(sourceColumns[i]);
                }
            }
            
            CacheTable = new Table("flattened", flattenedColumns, null);
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            var childTransform = _childTransform;
            var hasChildRecords = false;

            if (childTransform != null)
            {
                hasChildRecords = await childTransform.ReadAsync(cancellationToken);
            }

            // no records in the child transform, then get the next parent record.
            if (!hasChildRecords)
            {
                if (!await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    return null;
                }

                childTransform = (Transform) PrimaryTransform[_node];

                await childTransform.ReadAsync(cancellationToken);
                _childTransform = childTransform;
            }

            var outputRow = new object[FieldCount];
            var pos = 0;

            for (var i = 0; i < PrimaryTransform.FieldCount; i++)
            {
                if (i == _nodeOrdinal)
                {
                    if (!childTransform.IsReaderFinished)
                    {
                        for (var j = 0; j < childTransform.FieldCount; j++)
                        {
                            outputRow[pos++] = childTransform[j];
                        }
                    }
                    else
                    {
                        pos += childTransform.FieldCount;
                    }
                }
                else
                {
                    outputRow[pos++] = PrimaryTransform[i];
                }
            }

            return outputRow;
        }



        public override string Details()
        {
            return "Flatten Array";
        }

        public override bool ResetTransform()
        {
            return true;
        }
    }
}