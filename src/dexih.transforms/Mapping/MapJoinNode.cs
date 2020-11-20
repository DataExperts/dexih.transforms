using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.Mapping
{
    public class MapJoinNode : Mapping
    {
        /// <summary>
        /// Broker transform that site between the source transform, and target transforms.
        /// </summary>
        public sealed override TransformNode Transform { get; } = new TransformNode() {Name = "Join Node"} ;

        public MapJoinNode(TableColumn nodeColumn, Table joinTable)
        {
            NodeColumn = nodeColumn;
            Transform.SetTable(joinTable, null);
        }

        public readonly TableColumn NodeColumn;
        public Transform InputTransform { get; set; }
        private int _nodeOrdinal = -1;

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
            _nodeOrdinal = AddOutputColumn(table, NodeColumn);
        }

        public override Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row,
            object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data)
        {
            data[_nodeOrdinal] = Transform;
        }

        public override object GetOutputValue(object[] row = null)
        {
//            var transform = Transform;
//            transform.Reset(true);
//            transform.PrimaryTransform = InputTransform;
//            // transform.SetInTransform(InputTransform, transform.ReferenceTransform);
//            return transform;

            return InputTransform;
        }

        public override string Description()
        {
            return $"Node Join ({NodeColumn?.Name}";
        }

        public override void Reset(EFunctionType functionType)
        {
        }
        
        // public override IEnumerable<SelectColumn> GetRequiredColumns(bool includeAggregate)
        // {
        //     return new SelectColumn[0];
        // }

        public override IEnumerable<SelectColumn> GetRequiredReferenceColumns()
        {
            if (NodeColumn != null)
            {
                yield return new SelectColumn(NodeColumn);
            }
        }
        
        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            return false;
        }

    }
}