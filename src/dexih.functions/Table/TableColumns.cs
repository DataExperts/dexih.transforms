using MessagePack;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace dexih.functions
{
    [MessagePackObject]
    public class TableColumns : IList<TableColumn>
    {
        private readonly List<TableColumn> _tableColumns;
        private readonly Dictionary<string, int> _columnOrdinals;
        private readonly Dictionary<TableColumn.EDeltaType, List<int>> _deltaOrdinals;
        

        public TableColumns()
        {
            _tableColumns = new List<TableColumn>();
            _columnOrdinals = new Dictionary<string, int>();
            _deltaOrdinals = new Dictionary<TableColumn.EDeltaType, List<int>>();
        }

        public TableColumns(IEnumerable<TableColumn> tableColumns)
        {
            _tableColumns = new List<TableColumn>();
            _columnOrdinals = new Dictionary<string, int>();
            _deltaOrdinals = new Dictionary<TableColumn.EDeltaType, List<int>>();

            foreach (var column in tableColumns)
                Add(column);
        }

        public TableColumn this[int index]
        {
            get => _tableColumns[index];

            set => _tableColumns[index] = value;
        }

        public TableColumn this[string columnName]
        {
            get
            {
                if (_columnOrdinals.TryGetValue(columnName, out var ordinal))
                    return _tableColumns[ordinal];
                return null;
            }

            set => _tableColumns[_columnOrdinals[columnName]] = value;
        }
        
//        public TableColumn this[string columnName, string columnGroup]
//        {
//            get
//            {
//                var name = (string.IsNullOrWhiteSpace(columnGroup) ? "" : columnGroup + ".") + columnName;
//                if (_columnOrdinals.TryGetValue(name, out var ordinal))
//                    return _tableColumns[ordinal];
//                return null;
//            }
//            set
//            {
//                var name = (string.IsNullOrWhiteSpace(columnGroup) ? "" : columnGroup + ".") + columnName;
//                _tableColumns[_columnOrdinals[name]] = value;
//            }
//        }

        public TableColumn this[TableColumn column]
        {
            get
            {
                if (_columnOrdinals.TryGetValue(column.TableColumnName(), out var ordinal))
					return _tableColumns[ordinal];

				if (_columnOrdinals.TryGetValue(column.Name, out var ordinal1))
					return _tableColumns[ordinal1];

                return null;
            }

            set => _tableColumns[_columnOrdinals[column.TableColumnName()]] = value;
        }

        public bool TryGetColumn(string columnName, out TableColumn column)
        {
            column = this[columnName];
            return column != null;
        }

        public bool TryGetColumn(TableColumn inColumn, out TableColumn column)
        {
            column = this[inColumn];
            return column != null;
        }

        public bool TryGetColumn(TableColumn.EDeltaType deltaType, out TableColumn column)
        {
            column = GetColumn(deltaType);
            return column != null;
        }

        public int GetOrdinal(TableColumn column, bool groupMustMatch = false)
        {
            var ordinal = GetOrdinal(column.TableColumnName());
            if(ordinal < 0 && !groupMustMatch) 
            {
                ordinal = GetOrdinal(column.Name);
            }

            return ordinal;
        }

        public int GetOrdinal(string columnName)
        {
            if (_columnOrdinals.TryGetValue(columnName, out var ordinal))
                return ordinal;
            return -1;
        }

//        public int GetOrdinal(string columnName, string columnGroup)
//        {
//            var name = (string.IsNullOrWhiteSpace(columnGroup) ? "" : columnGroup + ".") + columnName;
//            if (_columnOrdinals.TryGetValue(name, out var ordinal))
//                return ordinal;
//            return -1;
//        }

        public int GetOrdinal(TableColumn.EDeltaType deltaType)
        {
            if(_deltaOrdinals.TryGetValue(deltaType, out var value))
            {
                if (value.Count == 1)
                {
                    return value[0];
                }

                throw new TableException($"There are multiple columns with the delta type {deltaType}.");
            }

            return -1;
        }
        
        public IEnumerable<int> GetOrdinals(TableColumn.EDeltaType deltaType)
        {
            if(_deltaOrdinals.TryGetValue(deltaType, out var value))
            {
                return value;
            }

            return null;
        }
        
        public TableColumn GetColumn(TableColumn.EDeltaType deltaType)
        {
            var ordinal = GetOrdinal(deltaType);
            if (ordinal == -1) return null;
            return this[ordinal];
        }
        
        public TableColumn GetAutoIncrementColumn()
        {
            return GetColumn(TableColumn.EDeltaType.DbAutoIncrement) ??
                   GetColumn(TableColumn.EDeltaType.AutoIncrement);
        }

        public int GetAutoIncrementOrdinal()
        {
            var ordinal = GetOrdinal(TableColumn.EDeltaType.AutoIncrement);
            if (ordinal >= 0)
            {
                return ordinal;
            }

            return GetOrdinal(TableColumn.EDeltaType.DbAutoIncrement);
        }
        
        public TableColumn[] GetColumns(TableColumn.EDeltaType deltaType)
        {
            var ordinals = GetOrdinals(deltaType);
            if (ordinals == null)
            {
                return new TableColumn[] { };
            }
            var columns = ordinals.Select(ordinal => this[ordinal]).ToArray();
            return columns;
        }
        

        public int Count => _tableColumns.Count;

        public bool IsReadOnly => false;

        public void Add(TableColumn item)
        {
            _tableColumns.Add(item);
            var tableColumnName = item.TableColumnName();
            var ordinal = _tableColumns.Count - 1;
            
            if (tableColumnName == item.Name)
            {
                if (!_columnOrdinals.ContainsKey(item.Name))
                {
                    _columnOrdinals.Add(item.Name, ordinal);
                }
            }
            else
            {
                if (!_columnOrdinals.ContainsKey(tableColumnName))
                {
                    _columnOrdinals.Add(tableColumnName, ordinal);
                }

                if (!_columnOrdinals.ContainsKey(item.Name))
                {
                    _columnOrdinals.Add(item.Name, ordinal);
                }
            }

            if (_deltaOrdinals.TryGetValue(item.DeltaType, out var ordinals))
            {
                ordinals.Add(ordinal);
            }
            else
            {
                _deltaOrdinals.Add(item.DeltaType, new List<int>() {ordinal});
            }
        }

        public void Clear()
        {
            _tableColumns.Clear();
            _columnOrdinals.Clear();
            _deltaOrdinals.Clear();
        }

        public bool Contains(TableColumn item)
        {
            return _tableColumns.Contains(item);
        }

        public bool ContainsMatching(TableColumn column)
        {
            if (_tableColumns == null)
                return false;

			return _columnOrdinals.ContainsKey(column.TableColumnName());
        }

        public void CopyTo(TableColumn[] array, int arrayIndex)
        {
            _tableColumns.CopyTo(array, arrayIndex);
        }

        public IEnumerator<TableColumn> GetEnumerator()
        {
            return _tableColumns.GetEnumerator();
        }

        public int IndexOf(TableColumn item)
        {
            return _tableColumns.IndexOf(item);
        }

        public void Insert(int index, TableColumn item)
        {
            _tableColumns.Insert(index, item);
            RebuildOrdinals();
        }

        public bool Remove(TableColumn item)
        {
            var returnValue = _tableColumns.Remove(item);
            RebuildOrdinals();
            return returnValue;
        }

        public void RemoveAt(int index)
        {
            _tableColumns.RemoveAt(index);
            RebuildOrdinals();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _tableColumns.GetEnumerator();
        }

        private void RebuildOrdinals()
        {
            _columnOrdinals.Clear();
            _deltaOrdinals.Clear();
            
            var ordinal = 0;
            foreach (var col in _tableColumns)
            {
                var tableColumnName = col.TableColumnName();
                if (tableColumnName == col.Name)
                {
                    if (_columnOrdinals.ContainsKey(col.Name))
                    {
                        //    _columnOrdinals[item.Name] = _tableColumns.Count - 1;
                    }
                    else
                    {
                        _columnOrdinals.Add(col.Name, ordinal);
                    }
                }
                else
                {
                    if (!_columnOrdinals.ContainsKey(tableColumnName))
                    {
                        _columnOrdinals.Add(tableColumnName, ordinal);
                    }

                    if (!_columnOrdinals.ContainsKey(col.Name))
                    {
                        _columnOrdinals.Add(col.Name, ordinal);
                    }
                }
                
                if (_deltaOrdinals.TryGetValue(col.DeltaType, out var ordinals))
                {
                    ordinals.Add(ordinal);
                }
                else
                {
                    _deltaOrdinals.Add(col.DeltaType, new List<int>() {ordinal});
                }

                ordinal++;
            }
        }
        

    }
}
