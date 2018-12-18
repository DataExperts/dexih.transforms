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
        protected Transform OutputTransform { get; set; }
        protected int NodeOrdinal = -1;
        protected int OutputOrdinal = -1;

        public override void InitializeColumns(Table table, Table joinTable = null)
        {
            if (OutputTransform == null)
            {
                NodeColumn.ChildColumns = joinTable?.Columns;
            }
            else
            {
                NodeColumn.ChildColumns = OutputTransform.CacheTable.Columns;
            }
        }

        public override void AddOutputColumns(Table table)
        {
            NodeOrdinal = AddOutputColumn(table, NodeColumn);
        }

        public void SetOutputTransform(Transform transform)
        {
            OutputTransform = transform;
            Transform.SetTable(transform.CacheTable, null);
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row,
            object[] joinRow = null)
        {
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data)
        {
            if (OutputTransform == null)
            {
                data[NodeOrdinal] = Transform;
            }

            data[NodeOrdinal] = OutputTransform;
        }

        public override object GetOutputTransform(object[] row = null)
        {
            var transform = OutputTransform ?? Transform;
            transform.Reset();
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