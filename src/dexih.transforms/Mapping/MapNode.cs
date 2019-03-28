using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms.Mapping
{
    public class MapNode: Mapping
    {
        
   public MapNode() {}
        
        /// <summary>
        /// Broker transform that site between the source transform, and target transforms.
        /// </summary>
        public sealed override TransformNode Transform { get; } = new TransformNode();

        public MapNode(TableColumn inputColumn, Table parentTable)
        {
            InputColumn = inputColumn;
            OutputColumn = inputColumn.Copy();

            var table = new Table("node", InputColumn.ChildColumns);
            Transform.SetTable(table, parentTable);
        }

        public Transform OutputTransform { private get; set; }
        public TableColumn InputColumn { get; set; }
        public TableColumn OutputColumn { get; set; }

        private int _inputOrdinal = -1;
        private int _outputOrdinal = -1;

        private Transform _cachedTransform;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            if (InputColumn == null) return;
            
            _inputOrdinal = table.GetOrdinal(InputColumn.TableColumnName());

            if (_inputOrdinal < 0)
            {
                throw new Exception($"Could not find the column {InputColumn.TableColumnName()} when mapping the node.");
            }
        }

        public override void AddOutputColumns(Table table)
        {
            OutputColumn.ChildColumns = OutputTransform.CacheTable.Columns;
            _outputOrdinal = AddOutputColumn(table, OutputColumn);
        }

        public override async Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            Transform.PrimaryTransform = (Transform) row[_inputOrdinal];
            await Transform.Open(0, null, cancellationToken);
            Transform.SetParentRow(row);
            
            OutputTransform?.Reset(false);

            if (OutputTransform == null)
            {
                _cachedTransform = new TransformCache(Transform);
            }
            else
            {
                _cachedTransform = new TransformCache(OutputTransform);
            }

            // await _cachedTransform.Open(cancellationToken);

            return true;
        }

        public override void MapOutputRow(object[] data)
        {
            data[_outputOrdinal] = _cachedTransform;
        }

        public override object GetOutputValue(object[] row = null)
        {
            throw new NotImplementedException();
        }

        public override string Description()
        {
            return $"Node Mapping ({InputColumn?.Name} => {OutputColumn?.Name}";
        }

        public override void Reset(EFunctionType functionType)
        {
            
        }

    }

}