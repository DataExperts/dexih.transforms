using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

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

        private List<TableColumn> _referencePassThroughColumns;

        /// <summary>
        /// Dictionary stores intput-output ordinal mapping for passthrough columns.
        /// </summary>
        private Dictionary<int, int> _referencePassThroughOrdinals;

        private Task<bool>[] _tasks;

        private object[] _rowData;
        
        // empty function variables, so save recreating.
        private readonly FunctionVariables _functionVariables = new FunctionVariables();

        // Create a table object containing the output columns for the mappings.
        public Table Initialize(Table inputTable, Table joinTable = null, string joinTableAlias = null, bool mapAllJoinColumns = true)
        {
            var table = new Table("Mapping");
            
            foreach (var mapping in this)
            {
                mapping.InitializeColumns(inputTable, joinTable);
            }
            
            foreach (var mapping in this)
            {
                mapping.AddOutputColumns(table);
            }
            
            if (PassThroughColumns)
            {
                _passThroughOrdinals = new Dictionary<int, int>();
                _passThroughColumns = new List<TableColumn>();
                var targetOrdinal = table.Columns.Count - 1;
                
                for(var i = 0; i < inputTable.Columns.Count; i++)
                {
                    var column = inputTable.Columns[i];

                    if(column.IsParent) continue; // passThrough doesn't need to map parent columns

                    var ordinal = table.GetOrdinal(column);
                    if (ordinal < 0)
                    {
                        targetOrdinal++;
                        table.Columns.Add(column.Copy());
                        _passThroughColumns.Add(column);
                        _passThroughOrdinals.Add(i, targetOrdinal);
                    }
                }

                if (joinTable != null)
                {
                    var mapArrays = this.OfType<MapJoinNode>().ToArray();
                    
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
                        _referencePassThroughColumns = new List<TableColumn>();
                        _referencePassThroughOrdinals = new Dictionary<int, int>();

                        for (var i = 0; i < joinTable.Columns.Count; i++)
                        {
                            var column = joinTable.Columns[i];

                            var newColumn = column.Copy();
                            newColumn.ReferenceTable = joinTableAlias;
                            newColumn.IsIncrementalUpdate = false;

                            var ordinal = table.GetOrdinal(newColumn.TableColumnName());
                            if (mapAllJoinColumns || ordinal < 0)
                            {
                                targetOrdinal++;
                                table.Columns.Add(newColumn);
                                _referencePassThroughColumns.Add(newColumn);
                                _referencePassThroughOrdinals.Add(i, targetOrdinal);
                            }
                        }
                    }
                }
            }
            
            if (inputTable.OutputSortFields != null)
            {
                //pass through the previous sort order, however limit to fields which have been mapped.
                var fields = new List<Sort>();
                foreach (var t in inputTable.OutputSortFields)
                {
                    var found = false;
                    foreach (var mapping in this)
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

            _tasks = new Task<bool>[Count];

            return table;
        }


        /// <summary>
        /// Run any open function logic such as preloading caches.
        /// </summary>
        /// <returns></returns>
        public async Task Open()
        {
            foreach (var mapping in this)
            {
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
            var groups = this.OfType<MapGroup>().Select(c=>c.GetOutputTransform(row)).ToArray();
            return groups;
        }
        
        /// <summary>
        /// Returns the series value + count
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public object GetSeriesValue(int count, object[] row = null)
        {
            var series = this.OfType<MapSeries>().FirstOrDefault()?.NextValue(count, row);
            return series;
        }

        public void CreateFillerRow(object[] row, object[] fillerRow, object seriesValue)
        {
            foreach (var map in this)
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
            foreach (var map in this)
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
            var inputs = this.OfType<MapJoin>().Select(c=>c.GetOutputTransform()).ToArray();
            return inputs;
        }

        /// <summary>
        /// Returns an array containing only the group field values for the join table.
        /// </summary>
        /// <param name="joinRow"></param>
        /// <returns></returns>
        public object[] GetJoinReferenceKey(object[] joinRow)
        {
            var joins = this.OfType<MapJoin>().Select(c=>c.GetJoinValue(joinRow)).ToArray();
            return joins;
        }

        /// <summary>
        /// Compares Join compare results to get an overall result.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public int GetJoinCompareResult()
        {
            foreach (var mapping in this.OfType<MapJoin>())
            {
                if (mapping.CompareResult != 0)
                    return mapping.CompareResult;
            }

            return 0;
        }

        public Task<bool> ProcessInputData(object[] row)
        {
            return ProcessInputData(_functionVariables, row, null);
        }

        public Task<bool> ProcessInputData(object[] row, object[] joinRow)
        {
            return ProcessInputData(_functionVariables, row, joinRow);
        }

        /// <summary>
        /// Run the processing for a new input row
        /// </summary>
        /// <param name="functionVariables"></param>
        /// <param name="row"></param>
        /// <param name="joinRow"></param>
        /// <returns></returns>
        public async Task<bool> ProcessInputData(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            var result = true;

            for (var i = 0; i < Count; i++)
            {
                _tasks[i] = this[i].ProcessInputRow(functionVariables, row, joinRow);
            }

            try
            {
                await Task.WhenAll(_tasks);
            }
            catch (TargetInvocationException)
            {
                for (var i = 0; i < Count; i++)
                {
                    var task = _tasks[i];
                    if (task.IsFaulted)
                    {
                        if (task.Exception?.InnerException is TargetInvocationException targetInvocationException2)
                        {
                            throw new FunctionException(
                                $"The mapping {this[i].Description()} failed due to {targetInvocationException2.InnerException.Message}.",
                                targetInvocationException2);
                        }
                    }
                }
                throw;
            }

            for (var i = 0; i < Count; i++)
            {
                result = result & _tasks[i].Result;
            }
            
            _rowData = row;

            return result;
        }
        
        /// <summary>
        /// M
        /// </summary>
        /// <param name="row"></param>
        public void MapOutputRow(object[] row)
        {
            foreach (var mapping in this)
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

        public void ProcessOutputRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            foreach (var mapping in this)
            {
                mapping.ProcessResultRow(functionVariables, row, functionType);
            }
        }
        
        public async Task<bool> ProcessAggregateRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            var result = false;
            
            foreach (var mapping in this)
            {
                result = result | await mapping.ProcessResultRow(functionVariables, row, functionType);
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
        

    }
}