using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms.Mapping
{
    public class MapJoinNode : Mapping
    {
        /// <summary>
        /// Broker transform that site between the source transform, and target transforms.
        /// </summary>
        public TransformNode Transform { get; } = new TransformNode();

        public MapJoinNode(TableColumn nodeColumn, Table joinTable)
        {
            NodeColumn = nodeColumn;
            Transform.SetTable(joinTable, null);
        }

        public TableColumn NodeColumn;
        public Transform InputTransform { get; set; }
        protected int NodeOrdinal = -1;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            NodeColumn.ChildColumns = new TableColumns();
            if (joinTable?.Columns != null)
            {
                foreach (var column in joinTable.Columns)
                {
                    var col = column.Copy();
                    col.ReferenceTable = "";
                    NodeColumn.ChildColumns.Add(col);
                }
            }
        }

        public override void AddOutputColumns(Table table)
        {
            NodeOrdinal = AddOutputColumn(table, NodeColumn);
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row,
            object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data)
        {
            data[NodeOrdinal] = Transform;
        }

        public override object GetOutputTransform(object[] row = null)
        {
            var transform = Transform;
            transform.Reset(true);
            transform.SetInTransform(InputTransform, transform.ReferenceTransform);
            return transform;
        }

        public override string Description()
        {
            return $"Node Join ({NodeColumn?.Name}";
        }

        public override void Reset(EFunctionType functionType)
        {
        }
    }
}