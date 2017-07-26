using System.Collections;
using System.Collections.Generic;

namespace dexih.functions
{
    public class TableColumns : IList<TableColumn>
    {
        private readonly List<TableColumn> _tableColumns;
        private readonly Dictionary<string, int> _columnOrdinals;

        public TableColumns()
        {
            _tableColumns = new List<TableColumn>();
            _columnOrdinals = new Dictionary<string, int>();
        }

        public TableColumns(IEnumerable<TableColumn> tableColumns)
        {
            _tableColumns = new List<TableColumn>();
            _columnOrdinals = new Dictionary<string, int>();

            foreach (var column in tableColumns)
                Add(column);
        }

        public TableColumn this[int index]
        {
            get
            {
                return _tableColumns[index];
            }

            set
            {
                _tableColumns[index] = value;
            }
        }

        public TableColumn this[string columnName]
        {
            get
            {
                if (_columnOrdinals.ContainsKey(columnName))
                    return _tableColumns[_columnOrdinals[columnName]];
                else
                    return null;
            }

            set
            {
                _tableColumns[_columnOrdinals[columnName]] = value;
            }
        }

        public TableColumn this[TableColumn column]
        {
            get
            {
                if (_columnOrdinals.ContainsKey(column.SchemaColumnName()))
                    return _tableColumns[_columnOrdinals[column.SchemaColumnName()]];
                else
                    return null;
            }

            set
            {
                _tableColumns[_columnOrdinals[column.SchemaColumnName()]] = value;
            }
        }

        public int GetOrdinal(string columnName)
        {
            if (_columnOrdinals.ContainsKey(columnName))
                return _columnOrdinals[columnName];
            else
                return -1;
        }

        public int Count
        {
            get
            {
                return _tableColumns.Count;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public void Add(TableColumn item)
        {
            _tableColumns.Add(item);
            if (!_columnOrdinals.ContainsKey(item.SchemaColumnName()))
            {
                _columnOrdinals.Add(item.SchemaColumnName(), _tableColumns.Count - 1);
            }
            if (!_columnOrdinals.ContainsKey(item.Name))
            {
                _columnOrdinals.Add(item.Name, _tableColumns.Count - 1);
            }
        }

        public void Clear()
        {
            _tableColumns.Clear();
            _columnOrdinals.Clear();
        }

        public bool Contains(TableColumn item)
        {
            return _tableColumns.Contains(item);
        }

        public bool ContainsMatching(TableColumn column)
        {
            if (_tableColumns == null)
                return false;

            return _columnOrdinals.ContainsKey(column.SchemaColumnName());
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
            for (int i = 0; i < _tableColumns.Count; i++)
            {
                _columnOrdinals.Add(_tableColumns[i].SchemaColumnName(), i);
                if (!_columnOrdinals.ContainsKey(_tableColumns[i].Name))
                {
                    _columnOrdinals.Add(_tableColumns[i].Name, i);
                }
            }
        }
    }
}
