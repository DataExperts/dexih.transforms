using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms.Mapping
{
    public class MapUnGroup: Mapping
    {
        public MapUnGroup()
        {
        }

        public MapUnGroup(TableColumn nodeColumn, ICollection<TableColumn> columns = null)
        {
            NodeColumn = nodeColumn;

            Columns = columns ?? NodeColumn.ChildColumns.Select(column =>
            {
                var newColumn = column.Copy();
                newColumn.Name = nodeColumn.Name + "." + newColumn.Name;
                return newColumn;
            }).ToArray();
        }
        
        public TableColumn NodeColumn { get; set; }
        public ICollection<TableColumn> Columns;
        public int _nodeColumnOrdinal = -1;
        public List<int> _outputOrdinals;

        private Transform _transform;
        private Transform _transformThread;
        
        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            if (NodeColumn == null) return;
            
            _nodeColumnOrdinal = table.GetOrdinal(NodeColumn);
        }

        public override void AddOutputColumns(Table table)
        {
            _outputOrdinals = new List<int>();
            foreach (var column in Columns)
            {
                _outputOrdinals.Add(AddOutputColumn(table, column));
            }
        }

        public override async Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            var transform = (Transform) row[_nodeColumnOrdinal];
            if (transform == null) return false;

            if (transform.CurrentRow ==null )
            {
                _transform = transform.PrimaryTransform.PrimaryTransform;
                _transformThread = transform.GetThread();
            }

            return await _transformThread.ReadAsync(cancellationToken);
        }

        public override void MapOutputRow(object[] row)
        {
            var hasRow = _transformThread.CurrentRow != null;
            for (var i = 0; i < _outputOrdinals.Count; i++)
            {
                row[_outputOrdinals[i]] = hasRow ? _transformThread[i] : null;
            }
        }

        public override object GetOutputTransform(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override string Description()
        {
            return $"UnGroup {NodeColumn.Name}";
        }
    }
}