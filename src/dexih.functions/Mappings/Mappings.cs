using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.Mappings
{
    public class Mappings : List<Mapping>
    {
        public Mappings(bool passThroughColumns = true)
        {
            PassThroughColumns = passThroughColumns;
        }

        private bool _doPassThroughColumns;
        
        /// <summary>
        /// Pass through any unmapped columns (if groupRows is true, passthrough will be set to false)
        /// </summary>
        public bool PassThroughColumns {
            get => _doPassThroughColumns && !GroupRows;
            set => _doPassThroughColumns = value;
        }
        
        /// <summary>
        /// (Group transform only) group rows together.  If false, original grain will be used.
        /// </summary>
        public bool GroupRows { get; set; }

        private List<TableColumn> _passthroughColumns;

        /// <summary>
        /// Dictionary stores intput-output ordinal mapping for passthrough columns.
        /// </summary>
        private Dictionary<int, int> _passthroughOrdinals;

        private List<TableColumn> _referencePassthroughColumns;

        /// <summary>
        /// Dictionary stores intput-output ordinal mapping for passthrough columns.
        /// </summary>
        private Dictionary<int, int> _referencePassthroughOrdinals;

        private object[] _rowData;
        
        // empty function variables, so save recreating.
        private FunctionVariables _functionVariables = new FunctionVariables();

        public Table Initialize(Table inputTable, Table joinTable = null, string joinTableAlias = null)
        {
            foreach (var mapping in this)
            {
                mapping.InitializeInputOrdinals(inputTable, joinTable);
            }
            
            var table = new Table("Mapping");
            foreach (var mapping in this)
            {
                mapping.AddOutputColumns(table);
            }

            if (PassThroughColumns)
            {
                _passthroughOrdinals = new Dictionary<int, int>();
                _passthroughColumns = new List<TableColumn>();
                int targetOrdinal = table.Columns.Count - 1;
                
                for(var i = 0; i < inputTable.Columns.Count; i++)
                {
                    var column = inputTable.Columns[i];
                    var ordinal = table.GetOrdinal(column.TableColumnName());
                    if (ordinal < 0)
                    {
                        targetOrdinal++;
                        table.Columns.Add(column.Copy());
                        _passthroughColumns.Add(column);
                        _passthroughOrdinals.Add(i, targetOrdinal);
                    }
                }

                if (joinTable != null)
                {
                    _referencePassthroughColumns = new List<TableColumn>();
                    _referencePassthroughOrdinals = new Dictionary<int, int>();
                    
                    for (var i = 0; i < joinTable.Columns.Count; i++)
                    {
                        var column = joinTable.Columns[i];
                        
                        var newColumn = column.Copy();
                        newColumn.ReferenceTable = joinTableAlias;
                        newColumn.IsIncrementalUpdate = false;

//                        var ordinal = table.GetOrdinal(newColumn.TableColumnName());
//                        if (ordinal < 0)
//                        {
                            targetOrdinal++;
                            table.Columns.Add(newColumn);
                            _referencePassthroughColumns.Add(newColumn);
                            _referencePassthroughOrdinals.Add(i, targetOrdinal);
//                        }
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

                    if (!found && PassThroughColumns && _passthroughColumns != null)
                    {
                        var column = _passthroughColumns.SingleOrDefault(c => c.Compare(t.Column));
                        if (column != null)
                        {
                            fields.Add(new Sort(column, t.Direction));
                        }
                    }
                }

                table.OutputSortFields = fields;
            }

            return table;
        }

        /// <summary>
        /// Returns an array containing only the group field values for the input row.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public object[] GetGroupValues(object[] row = null)
        {
            var groups = this.OfType<MapGroup>().Select(c=>c.GetInputValue(row)).ToArray();
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
            var inputs = this.OfType<MapJoin>().Select(c=>c.GetInputValue()).ToArray();
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
        public DataType.ECompareResult GetJoinCompareResult()
        {
            foreach (var mapping in this.OfType<MapJoin>())
            {
                switch (mapping.CompareResult)
                {
                    case DataType.ECompareResult.Less:
                        return DataType.ECompareResult.Less;
                    case DataType.ECompareResult.Equal:
                        continue;
                    case DataType.ECompareResult.Greater:
                        return DataType.ECompareResult.Greater;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            return DataType.ECompareResult.Equal;
        }

        public bool ProcessInputData(object[] row)
        {
            return ProcessInputData(_functionVariables, row, null);
        }

        public bool ProcessInputData(object[] row, object[] joinRow)
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
        public bool ProcessInputData(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            var result = true;
            
            foreach (var mapping in this)
            {
                result = result & mapping.ProcessInputRow(functionVariables, row, joinRow);
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

            if (_passthroughColumns != null)
            {
                foreach (var ordinal in _passthroughOrdinals)
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
        
        public bool ProcessAggregateRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            var result = false;
            
            foreach (var mapping in this)
            {
                result = result | mapping.ProcessResultRow(functionVariables, row, functionType);
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