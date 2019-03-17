using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms
{
    /// <summary>
    /// Simple transform used by the node mapping to provide a bridge when source transforms change.
    /// </summary>
    public class TransformNode : Transform
    {
        private object[] _parentRow;
        private Table _parentTable;
        private int _parentAutoIncrementOrdinal = -1;

        public override string TransformName { get; } = "Node Transform";
        public override string TransformDetails => "";
        
        public void SetTable(Table table, Table parentTable)
        {
            _parentTable = parentTable;
            
            var newTable = new Table("Node");

            foreach (var col in table.Columns)
            {
                newTable.Columns.Add(col);
            }

            if (_parentTable != null)
            {
                foreach (var col in _parentTable.Columns)
                {
                    var parentCol = col.Copy();
                    parentCol.ColumnGroup = string.IsNullOrEmpty(parentCol.ColumnGroup) ? "parent" : "parent." + parentCol.ColumnGroup;
                    parentCol.IsParent = true;
                    newTable.Columns.Add(parentCol);
                }
                _parentAutoIncrementOrdinal = _parentTable.GetAutoIncrementOrdinal();
            }

            CacheTable = newTable;
        }
        
        public void SetParentRow(object[] parentRow)
        {
            _parentRow = parentRow;
        }

        public void SetParentAutoIncrement(object value)
        {
            if (_parentRow != null && _parentAutoIncrementOrdinal >= 0)
            {
                _parentRow[_parentAutoIncrementOrdinal] = value;
            }
        }

        public override Task<bool> Open(long auditKey, SelectQuery query = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            return Task.FromResult(true);
        }
        
        // not used as the ReadAsync is overridden.
        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            var result = await PrimaryTransform.ReadAsync(cancellationToken);
            if (!result)
            {
                return null;
            }

            var row = new object[FieldCount];
            var pos = 0;

            for (var i = 0; i < PrimaryTransform.FieldCount; i++)
            {
                row[pos++] = PrimaryTransform[i];
            }

            if (_parentRow != null)
            {
                foreach (var item in _parentRow)
                {
                    if (pos < row.Length) row[pos++] = item;
                    // row[pos++] = item;
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