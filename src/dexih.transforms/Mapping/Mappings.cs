using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Exceptions;
using dexih.functions.Query;

namespace dexih.transforms.Mapping
{
    public class Mappings : List<Mapping>
    {
        public Mappings(bool passThroughColumns = true)
        {
            PassThroughColumns = passThroughColumns;
        }

        private bool _doPassThroughColumns;
        
        /// <summary>
        /// Pass through any unmapped columns (if groupRows is true, passThrough will be set to false)
        /// </summary>
        public bool PassThroughColumns {
            get => _doPassThroughColumns && !GroupRows;
            set => _doPassThroughColumns = value;
        }
        
        /// <summary>
        /// (Group transform only) group rows together.  If false, original grain will be used.
        /// </summary>
        public bool GroupRows { get; set; }

        private List<TableColumn> _passThroughColumns;

        /// <summary>
        /// Dictionary stores intput-output ordinal mapping for passthrough columns.
        /// </summary>
        private Dictionary<int, int> _passThroughOrdinals;
        
        private MapGroupNode _groupNode;

        private Task<bool>[] _tasks;

        private object[] _rowData;

        private Mappings _primaryMappings;
        private Mappings _detailMappings;
        
        // empty function variables, so save recreating.
        private readonly FunctionVariables _functionVariables = new FunctionVariables();

        // Create a table object containing the output columns for the mappings.
        public Table Initialize(Table inputTable, Table joinTable = null, string joinTableAlias = null, bool mapAllJoinColumns = true)
        {
            var table = new Table("Mapping");

            // if there is a group node, split the mappings up into a primary, and detail.
            _groupNode = this.OfType<MapGroupNode>().FirstOrDefault();

            if (_groupNode != null)
            {
                _primaryMappings = new Mappings(false);
                _detailMappings = new Mappings(_doPassThroughColumns);
                foreach (var mapping in this)
                {
                    switch (mapping)
                    {
                        case MapGroupNode _:
                        case MapGroup _:
                            _primaryMappings.Add(mapping);
                            break;
                        default:
                            _detailMappings.Add(mapping);
                            break;
                    }
                }
                
                _groupNode.InitializeColumns(inputTable, joinTable, _detailMappings);
            }
            else
            {
                _primaryMappings = this;
                _detailMappings = null;
            }

            foreach (var mapping in _primaryMappings)
            {
                mapping.InitializeColumns(inputTable, joinTable, _detailMappings);
            }
            
            foreach (var mapping in _primaryMappings)
            {
                mapping.AddOutputColumns(table);
            }

            if (PassThroughColumns && _groupNode == null)
            {
                _passThroughOrdinals = new Dictionary<int, int>();
                _passThroughColumns = new List<TableColumn>();
                var targetOrdinal = table.Columns.Count - 1;

                var inputColumns = inputTable.Columns.Where(c => c.DeltaType != EDeltaType.IgnoreField).ToArray();
                
                for(var i = 0; i < inputColumns.Length; i++)
                {
                    var column = inputColumns[i];
                    
                    if(column.IsParent) { continue; }

                    var parentOrdinal = table.Columns.GetOrdinal(column, true);
                    if (parentOrdinal < 0)
                    {
                        targetOrdinal++;
                        table.Columns.Add(column.Copy());
                        _passThroughColumns.Add(column);
                        _passThroughOrdinals.Add(i, targetOrdinal);
                    }
                }

                if (joinTable != null)
                {
                    var mapArrays = _primaryMappings.OfType<MapJoinNode>().ToArray();
                    
                    if (mapArrays.Length > 1)
                    {
                        throw new Exception("The mappings contain more than one node mapping.");
                    }

                    // if there is an array column, link the join table into this.
                    if (mapArrays.Length == 1)
                    {
                       //  var mapNode = mapArrays[0];
                       //  mapNode.InputColumn.ChildColumns = joinTable.Columns;
                    }
                    else
                    {
                        // add the join columns to the main table.
                        var joinColumns = joinTable.Columns.Where(c => c.DeltaType != EDeltaType.IgnoreField).ToArray();
                        
                        for (var i = 0; i < joinColumns.Length; i++)
                        {
                            var column = joinColumns[i];

                            var newColumn = column.Copy();
                            newColumn.ReferenceTable = joinTableAlias;
                            newColumn.IsIncrementalUpdate = false;

                            var ordinal = table.GetOrdinal(newColumn, true);
                            if (mapAllJoinColumns || ordinal < 0)
                            {
                                targetOrdinal++;
                                table.Columns.Add(newColumn);
                            }
                        }
                    }
                }
                
            }
            
            if (inputTable.OutputSortFields != null)
            {
                //pass through the previous sort order, however limit to fields which have been mapped.
                var fields = new Sorts();
                foreach (var t in inputTable.OutputSortFields)
                {
                    var found = false;
                    foreach (var mapping in _primaryMappings)
                    {
                        if (mapping is MapColumn mapColumn)
                        {
                            if (mapColumn.InputColumn.Compare(t.Column))
                            {
                                fields.Add(new Sort(mapColumn.OutputColumn, t.Direction));
                                found = true;
                                break;
                            }
                        }
                        
                        if (mapping is MapGroup mapGroup)
                        {
                            if (mapGroup.InputColumn.Compare(t.Column))
                            {
                                fields.Add(new Sort(mapGroup.InputColumn, t.Direction));
                                found = true;
                                break;
                            }
                        }
                    }

                    if (!found && PassThroughColumns && _passThroughColumns != null)
                    {
                        var column = _passThroughColumns.SingleOrDefault(c => c.Compare(t.Column));
                        if (column != null)
                        {
                            fields.Add(new Sort(column, t.Direction));
                        }
                    }
                }

                table.OutputSortFields = fields;
            }


            foreach (var mapping in this.Where(c => c.Transform != null))
            {
                mapping.Transform?.SetParentTable(table);
            }
            
            _tasks = new Task<bool>[_primaryMappings.Count];

            return table;
        }


        /// <summary>
        /// Run any open function logic such as preloading caches.
        /// </summary>
        /// <returns></returns>
        public async Task Open(Table inputTable, Table joinTable = null)
        {
            foreach (var mapping in _primaryMappings)
            {
                // re-initialize the columns as the ordinals might change between initialize call and open call
                mapping.InitializeColumns(inputTable, joinTable, _detailMappings);
                
                await mapping.Open();
            }
        }

        /// <summary>
        /// Returns an array containing only the group field values for the input row.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public object[] GetGroupValues(object[] row = null)
        {
            var groups = _primaryMappings.OfType<MapGroup>().Select(c=>c.GetOutputValue(row)).ToArray();
            return groups;
        }

        /// <summary>
        /// Returns the series value + count
        /// </summary>
        /// <param name="count"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public object GetSeriesValue(int count, object[] row = null)
        {
            var series = _primaryMappings.OfType<MapSeries>().FirstOrDefault()?.NextValue(count, row);
            return series;
        }

        public void CreateFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            foreach (var map in _primaryMappings)
            {
                map.ProcessFillerRow(row, fillerRow, seriesValue);
            }
        }

        /// <summary>
        /// Creates an empty output row with any series values advanced by the count.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public void ProcessNextSeriesOutput(int count, object[] row = null)
        {
            foreach (var map in _primaryMappings)
            {
                if (map is MapSeries mapSeries)
                {
                    mapSeries.ProcessNextValueOutput(count, row);
                }

                if (map is MapGroup)
                {
                    map.MapOutputRow(row);
                }

//                if (map is MapFunction || map is MapAggregate)
//                {
//                    map.ProcessResultRow(0, row);
//                }
            }
        }

        /// <summary>
        /// Returns an array containing only the group field values for the join table.
        /// </summary>
        /// <returns></returns>
        public object[] GetJoinPrimaryKey()
        {
            var inputs = _primaryMappings.OfType<MapJoin>().Select(c=>c.GetOutputValue()).ToArray();
            return inputs;
        }

        /// <summary>
        /// Returns an array containing only the group field values for the join table.
        /// </summary>
        /// <param name="joinRow"></param>
        /// <returns></returns>
        public object[] GetJoinReferenceKey(object[] joinRow)
        {
            var joins = _primaryMappings.OfType<MapJoin>().Select(c=>c.GetJoinValue(joinRow)).ToArray();
            return joins;
        }

        /// <summary>
        /// Compares Join compare results to get an overall result.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public int GetJoinCompareResult()
        {
            foreach (var mapping in _primaryMappings.OfType<MapJoin>())
            {
                if (mapping.CompareResult != 0)
                    return mapping.CompareResult;
            }

            return 0;
        }

        public Task<(bool result, bool ignoreRow)> ProcessInputData(object[] row, CancellationToken cancellationToken)
        {
            return ProcessInputData(_functionVariables, row, null, cancellationToken);
        }

        public Task<(bool result, bool ignoreRow)> ProcessInputData(object[] row, object[] joinRow, CancellationToken cancellationToken)
        {
            return ProcessInputData(_functionVariables, row, joinRow, cancellationToken);
        }

        /// <summary>
        /// Run the processing for a new input row
        /// </summary>
        /// <param name="functionVariables"></param>
        /// <param name="row"></param>
        /// <param name="joinRow"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<(bool result, bool ignoreRow)> ProcessInputData(FunctionVariables functionVariables, object[] row, object[] joinRow, CancellationToken cancellationToken)
        {
            var result = true;
            var ignoreRow = false;

            for (var i = 0; i < _primaryMappings.Count; i++)
            {
                _tasks[i] = _primaryMappings[i].ProcessInputRowAsync(functionVariables, row, joinRow, cancellationToken);
            }

            bool[] results;

            try
            {
                results = await Task.WhenAll(_tasks);
            }
            catch (TargetInvocationException)
            {
                for (var i = 0; i < _primaryMappings.Count; i++)
                {
                    var task = _tasks[i];
                    if (task.IsFaulted)
                    {
                        if (task.Exception?.InnerException is TargetInvocationException targetInvocationException2)
                        {
                            throw new FunctionException(
                                $"The mapping {_primaryMappings[i].Description()} failed due to {targetInvocationException2.InnerException.Message}.",
                                targetInvocationException2);
                        }
                    }
                }
                throw;
            }

            for (var i = 0; i < _primaryMappings.Count; i++)
            {
                result &= results[i];
                ignoreRow = ignoreRow || _primaryMappings[i].IgnoreRow;
            }
            
            _rowData = row;

            return (result, ignoreRow);
        }
        
        /// <summary>
        /// M
        /// </summary>
        /// <param name="row"></param>
        public void MapOutputRow(object[] row)
        {
            foreach (var mapping in _primaryMappings)
            {
                mapping.MapOutputRow(row);
            }

            if (_passThroughColumns != null)
            {
                foreach (var ordinal in _passThroughOrdinals)
                {
                    row[ordinal.Value] = _rowData[ordinal.Key];
                }
            }
            
        }
        
        public async Task<(bool filter, bool ignore)> ProcessAggregateRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            var result = false;
            
            var ignoreRow = false;
            foreach (var mapping in _primaryMappings)
            {
                result = result | await mapping.ProcessResultRowAsync(functionVariables, row, functionType, cancellationToken);
                ignoreRow = ignoreRow || mapping.IgnoreRow;
            }

            return (result, ignoreRow);
        }
        
        public async Task<bool> ProcessFillerRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            var result = false;
            
            foreach (var mapping in _primaryMappings)
            {
                result = result | await mapping.ProcessFillerRow(functionVariables, row, functionType, cancellationToken);
            }

            return result;
        }
        
        public void Reset(EFunctionType functionType)
        {
            foreach (var mapping in this)
            {
                mapping.Reset(functionType);
            }
        }

        public void SetInputColumns(IEnumerable<TableColumn> inputColumns)
        {
            foreach (var mapping in this.OfType<MapInputColumn>())
            {
                mapping.SetInput(inputColumns);
            }
        }

        /// <summary>
        /// Gets the required source columns required for the mappings.
        /// </summary>
        /// <returns></returns>
        public SelectColumn[] GetRequiredColumns(bool ignorePassthrough = false, bool includeAggregate = false)
        {
            if (PassThroughColumns && !ignorePassthrough)
            {
                return null;
            }
            
            var columns = new HashSet<SelectColumn>();
            
            foreach (var mapping in this)
            {
                var cols = mapping.GetRequiredColumns(includeAggregate);
                foreach (var col in cols)
                {
                    columns.Add(col);
                }
            }

            return columns.ToArray();
        }
        
        public TableColumn[] GetRequiredReferenceColumns()
        {
            if (PassThroughColumns)
            {
                return null;
            }
            
            var columns = new HashSet<TableColumn>();
            
            foreach (var mapping in this)
            {
                var cols = mapping.GetRequiredReferenceColumns();
                foreach (var col in cols)
                {
                    columns.Add(col);
                }
            }

            return columns.ToArray();
        }
        

    }
}