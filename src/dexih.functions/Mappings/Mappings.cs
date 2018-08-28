using System;
using System.Collections.Generic;
using System.Linq;
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
        
        /// <summary>
        /// Pass through any unmapped columns
        /// </summary>
        public bool PassThroughColumns { get; set; }

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
                    var ordinal = table.GetOrdinal(column);
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

                        var ordinal = table.GetOrdinal(newColumn.TableColumnName());
                        if (ordinal < 0)
                        {
                            targetOrdinal++;
                            table.Columns.Add(newColumn);
                            _referencePassthroughColumns.Add(newColumn);
                            _referencePassthroughOrdinals.Add(i, targetOrdinal);
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
        public object[] GetGroupValues(object[] row)
        {
            var mapGroup = this.OfType<MapGroup>().ToArray();
            var groups = mapGroup.Select(c=>c.GetInputValue(row)).ToArray();
            return groups;
        }

        /// <summary>
        /// Returns an array containing only the group field values for the join table.
        /// </summary>
        /// <param name="row"></param>
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

        public bool ProcessInputData(object[] row, object[] joinRow = null)
        {
            var result = true;
            foreach (var mapping in this)
            {
                result = result && mapping.ProcessInputRow(row, joinRow);
            }

            _rowData = row;

            return result;
        }
        
        public void ProcessOutputRow(object[] row)
        {
            foreach (var mapping in this)
            {
                mapping.ProcessOutputRow(row);
            }

            if (_passthroughColumns != null)
            {
                foreach (var ordinal in _passthroughOrdinals)
                {
                    row[ordinal.Value] = _rowData[ordinal.Key];
                }
            }
        }

        public void ProcessAggregateRow(int index, object[] row)
        {
            foreach (var mapping in this)
            {
                mapping.ProcessResultRow(index, row);
            }
        }

        public void Reset()
        {
            foreach (var mapping in this)
            {
                mapping.Reset();
            }
        }
    }
}