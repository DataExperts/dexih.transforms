using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.Mapping
{
    public class MapGroupNode : Mapping
    {
        /// <summary>
        /// Broker transform that site between the source transform, and target transforms.
        /// </summary>
        public override TransformNode Transform { get; } = new TransformNode() {Name = "Internal Group Node"};

        public MapGroupNode(TableColumn nodeColumn)
        {
            NodeColumn = nodeColumn;
        }

        public TableColumn NodeColumn;
        public Mappings GroupMappings;
        public Table GroupTable;

        private Queue<object[]> _cachedRows = new Queue<object[]>();

        protected int NodeOrdinal = -1;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            NodeColumn.ChildColumns = new TableColumns();
            
            if (mappings == null) return;

            GroupMappings = mappings;
            GroupTable = GroupMappings.Initialize(table);
            foreach (var column in GroupTable.Columns)
            {
                var col = column.Copy();
                col.ReferenceTable = "";
                NodeColumn.ChildColumns.Add(col);
            }
        }

        public override void AddOutputColumns(Table table)
        {
            NodeOrdinal = AddOutputColumn(table, NodeColumn);
        }


        public override async Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row,
            object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            // process the current row
            await GroupMappings.ProcessInputData(row, cancellationToken);

            //create a cached current row.
            var cacheRow = new object[GroupTable.Columns.Count];
            GroupMappings.MapOutputRow(cacheRow);
            _cachedRows.Enqueue(cacheRow);

            return true;
        }

        public override void MapOutputRow(object[] data)
        {
            GroupTable.Data = new TableCache();

            foreach (var row in _cachedRows)
            {
                GroupTable.AddRow(row);
            }
            
            _cachedRows.Clear();
            
            var transform = new ReaderMemory(GroupTable);
            data[NodeOrdinal] = transform;
        }

        public override object GetOutputValue(object[] row = null)
        {
            throw new System.NotImplementedException();
        }

        public override async Task<bool> ProcessResultRowAsync(FunctionVariables functionVariables, object[] row,
            EFunctionType functionType, CancellationToken cancellationToken)
        {
            await ProcessGroupChange(cancellationToken);
            return false;
        }
        
        private async Task ProcessGroupChange(CancellationToken cancellationToken)
        {
            // if the group has changed, update all cached rows with aggregate functions.
            if (_cachedRows != null && _cachedRows.Any())
            {
                var index = 0;
                List<(int index, object[] row)> additionalRows = null;
                foreach (var row in _cachedRows)
                {
                    var (moreRows, ignore) = await GroupMappings.ProcessAggregateRow(new FunctionVariables() {Index = index}, row, EFunctionType.Aggregate, cancellationToken);

                    // if the aggregate function wants to provide more rows, store them in a separate collection.
                    while (moreRows && !ignore)
                    {
                        var rowCopy = new object[GroupTable.Columns.Count];
                        row.CopyTo(rowCopy, 0);
                        (moreRows, ignore)  = await GroupMappings.ProcessAggregateRow(new FunctionVariables() {Index = index}, row, EFunctionType.Aggregate, cancellationToken);

                        if (additionalRows == null)
                        {
                            additionalRows = new List<(int index, object[] row)>();
                        }
                        
                        additionalRows.Add((index, rowCopy));
                    }
                    
                    index++;
                }
                
                // merge the new rows in with existing cache
                if (additionalRows != null)
                {
                    var newQueue = new Queue<object[]>();
                    index = 0;
                    var additionalRowsIndex = 0;
                    foreach (var row in _cachedRows)
                    {
                        while (additionalRowsIndex < additionalRows.Count && index <= additionalRows[additionalRowsIndex].index)
                        {
                            newQueue.Enqueue(additionalRows[additionalRowsIndex++].row);
                        }

                        newQueue.Enqueue(row);
                    }

                    _cachedRows = newQueue;
                }
                
                GroupMappings.Reset(EFunctionType.Aggregate);
            }
        }

        public override string Description()
        {
            return $"Node Join ({NodeColumn?.Name}";
        }

        public override void Reset(EFunctionType functionType)
        {
        }

        public override IEnumerable<SelectColumn> GetRequiredColumns(bool includeAggregate)
        {
            yield return new SelectColumn(NodeColumn);
        }
        
        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            return false;
        }
    }
}