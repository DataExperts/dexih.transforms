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

            _columns = columns ?? NodeColumn.ChildColumns.Where(c=> !c.IsParent).Select(column =>
            {
                var newColumn = column.Copy();
                newColumn.Name = nodeColumn.Name + "." + newColumn.Name;
                return newColumn;
            }).ToArray();
        }

        private TableColumn NodeColumn { get; set; }
        private readonly ICollection<TableColumn> _columns;
        private int _nodeColumnOrdinal = -1;
        private List<int> _outputOrdinals;

        private Transform _transform;
        
        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            if (NodeColumn == null) return;
            
            _nodeColumnOrdinal = table.GetOrdinal(NodeColumn);
        }

        public override void AddOutputColumns(Table table)
        {
            _outputOrdinals = new List<int>();
            foreach (var column in _columns)
            {
                _outputOrdinals.Add(AddOutputColumn(table, column));
            }
        }

        public override async Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            var transform = (Transform) row[_nodeColumnOrdinal];
            if (transform == null) return false;

            if (_transform == null || _transform.IsReaderFinished)
            {
                // _transform = transform.PrimaryTransform.GetThread();
                _transform = transform.GetThread();
                await _transform.Open(cancellationToken);
            }

            return await _transform.ReadAsync(cancellationToken);
        }

        public override void MapOutputRow(object[] row)
        {
            var hasRow = _transform.CurrentRow != null;
            for (var i = 0; i < _outputOrdinals.Count; i++)
            {
                row[_outputOrdinals[i]] = hasRow ? _transform[i] : null;
            }
        }

        public override object GetOutputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override string Description()
        {
            return $"UnGroup {NodeColumn.Name}";
        }
        
        public override IEnumerable<TableColumn> GetRequiredColumns()
        {
            if (NodeColumn == null)
            {
                return new TableColumn[0];
            }

            return new[] {NodeColumn};
        }
    }
}