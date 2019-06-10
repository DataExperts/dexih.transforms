using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    /// <summary>
    /// Simple transform used by the node mapping to provide a bridge when source transforms change.
    /// </summary>
    public class TransformNode : Transform
    {
        public TransformNode()
        {
            
        }
        
        private object[] _parentRow;
        private Table _primaryTable;
        private Table _parentTable;
        private int _parentAutoIncrementOrdinal = -1;
        private readonly List<int> _parentTableSkipOrdinals = new List<int>();

        public override string TransformName { get; } = "Node Transform";
        public override string TransformDetails => "";
        
        // public override Table CacheTable { get; protected set; }
        
        public void SetTable(Table table, Table parentTable)
        {
            _primaryTable = table?.Copy();
            _parentTable = parentTable?.Copy();
            BuildCacheTable();
        }

        public void SetParentTable(Table parentTable)
        {
            _parentTable = parentTable?.Copy();
            BuildCacheTable();
        }

        private void BuildCacheTable()
        {
            
            var newTable = new Table("Node");

            if (_primaryTable != null)
            {
                foreach (var col in _primaryTable.Columns.Where(c => !c.IsParent))
                {
                    newTable.Columns.Add(col);
                }
            }

            _parentTableSkipOrdinals.Clear();
            if (_parentTable != null)
            {
                for(var i = 0; i< _parentTable.Columns.Count; i++)
                {
                    var col = _parentTable.Columns[i];
                    
                    if (col.DeltaType == TableColumn.EDeltaType.DatabaseOperation)
                    {
                        _parentTableSkipOrdinals.Add(i);
                        continue;
                    }

                    var parentCol = col.Copy();
                    parentCol.ColumnGroup = string.IsNullOrEmpty(parentCol.ColumnGroup) ? "parent" : "parent." + parentCol.ColumnGroup;
                    parentCol.IsParent = true;

                    if (parentCol.IsAutoIncrement()) parentCol.DeltaType = TableColumn.EDeltaType.TrackingField;
                    newTable.Columns.Add(parentCol);
                }
                _parentAutoIncrementOrdinal = _parentTable.GetAutoIncrementOrdinal();
            }

            CacheTable = newTable;

            if (PrimaryTransform?.BaseFieldCount > FieldCount)
            {
                throw new TransformException("Issue");
            }
        }
        
        public void SetParentRow(object[] parentRow)
        {
            _parentRow = parentRow.ToArray();
        }

        public void SetParentAutoIncrement(object value)
        {
            if (_parentRow != null && _parentAutoIncrementOrdinal >= 0)
            {
                _parentRow[_parentAutoIncrementOrdinal] = value;
            }
        }

        // not used as the ReadAsync is overridden.
        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            // BuildCacheTable();
            
            if (PrimaryTransform.BaseFieldCount > FieldCount)
            {
                throw new TransformException("Issue");
            }
            
            var result = await PrimaryTransform.ReadAsync(cancellationToken);
            if (!result)
            {
                return null;
            }

            var row = new object[FieldCount];
            var pos = 0;

            for (var i = 0; i < PrimaryTransform.BaseFieldCount; i++)
            {
                row[pos++] = PrimaryTransform[CacheTable[i]];
            }

            if (_parentRow != null)
            {
                for(var i = 0; i< _parentRow.Length; i++)
                {
                    if (_parentTableSkipOrdinals.Contains(i))
                    {
                        continue;
                    }

                    if (pos >= row.Length)
                    {
                        throw new TransformException("The transform node failed as the parent row contains too many fields.");
                    }
                    row[pos++] = _parentRow[i];
                }
            }

            return row;
        }

        public override bool ResetTransform()
        {
            return PrimaryTransform?.ResetTransform() ?? true;
        }
    }
}