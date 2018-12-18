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
        public TransformNode Transform { get; } = new TransformNode();

        public MapNode(TableColumn inputColumn, Table parentTable)
        {
            InputColumn = inputColumn;
            OutputColumn = inputColumn;

            var table = new Table("node", InputColumn.ChildColumns, null);
            Transform.SetTable(table, parentTable);
        }

        public Transform InputTransform { get; set; }
        public Transform OutputTransform { get; set; }
        public TableColumn InputColumn { get; set; }
        public TableColumn OutputColumn { get; set; }

        protected int InputOrdinal = -1;
        protected int OutputOrdinal = -1;

        protected object[] RowData;

        public override void InitializeColumns(Table table, Table joinTable = null)
        {
            if (InputColumn == null) return;
            
            InputOrdinal = table.GetOrdinal(InputColumn);
        }

        public override void AddOutputColumns(Table table)
        {
            OutputOrdinal = AddOutputColumn(table, OutputColumn);
        }

        public override async Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            RowData = row;
            
            Transform.PrimaryTransform = (Transform) row[InputOrdinal];
            await Transform.Open(0, null, CancellationToken.None);
            Transform.SetParentRow(row);

            OutputTransform?.Reset();
            return true;
        }

        public override void MapOutputRow(object[] data)
        {
            if (OutputTransform == null)
            {
                data[OutputOrdinal] = Transform;
            }

            data[OutputOrdinal] = OutputTransform;
        }

        public override object GetOutputTransform(object[] row = null)
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