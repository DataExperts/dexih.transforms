using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    /// <summary>
    /// Simple transform used by the node mapping to provide a bridge when source transforms change.
    /// </summary>
    public class TransformNode : Transform
    {
        private object[] _parentRow;
        private Table _parentTable;

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
                    parentCol.IsParent = true;
                    newTable.Columns.Add(parentCol);
                }
            }

            CacheTable = newTable;

        }
        
        public void SetParentRow(object[] parentRow)
        {
            _parentRow = parentRow;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery query = null, CancellationToken cancellationToken = default)
        {
            if (PrimaryTransform == null)
            {
                throw new TransformException("The transform node cannot be opened as a primary transform is not set.");
            }
            
            var openResult = await PrimaryTransform.Open(auditKey, query, cancellationToken);
            return openResult;
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
                    row[pos++] = item;
                }
            }

            return row;
        }

        public override string Details()
        {
            return "Internal";
        }

        public override bool ResetTransform()
        {
            return PrimaryTransform?.ResetTransform() ?? true;
        }
    }
}