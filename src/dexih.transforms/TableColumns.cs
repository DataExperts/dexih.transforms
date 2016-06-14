﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class TableColumns : IList<TableColumn>
    {
        private List<TableColumn> _TableColumns;
        private Dictionary<string, int> _ColumnOrdinals;

        public TableColumns()
        {
            _TableColumns = new List<TableColumn>();
            _ColumnOrdinals = new Dictionary<string, int>();
        }

        public TableColumns(IEnumerable<TableColumn> tableColumns)
        {
            _TableColumns = new List<TableColumn>();
            _ColumnOrdinals = new Dictionary<string, int>();

            foreach (var column in tableColumns)
                Add(column);
        }

        public TableColumn this[int index]
        {
            get
            {
                return _TableColumns[index];
            }

            set
            {
                _TableColumns[index] = value;
            }
        }

        public TableColumn this[string tableName]
        {
            get
            {
                return _TableColumns[_ColumnOrdinals[tableName]];
            }

            set
            {
                _TableColumns[_ColumnOrdinals[tableName]] = value;
            }
        }

        public int GetOrdinal(string columnName)
        {
            if (_ColumnOrdinals.ContainsKey(columnName))
                return _ColumnOrdinals[columnName];
            else
                return -1;
        }

        public int Count
        {
            get
            {
                return _TableColumns.Count;
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
            _TableColumns.Add(item);
            _ColumnOrdinals.Add(item.ColumnName, _TableColumns.Count - 1);
        }

        public void Clear()
        {
            _TableColumns.Clear();
            _ColumnOrdinals.Clear();
        }

        public bool Contains(TableColumn item)
        {
            return _TableColumns.Contains(item);
        }

        public void CopyTo(TableColumn[] array, int arrayIndex)
        {
            _TableColumns.CopyTo(array, arrayIndex);
        }

        public IEnumerator<TableColumn> GetEnumerator()
        {
            return _TableColumns.GetEnumerator();
        }

        public int IndexOf(TableColumn item)
        {
            return _TableColumns.IndexOf(item);
        }

        public void Insert(int index, TableColumn item)
        {
            _TableColumns.Insert(index, item);
            RebuildOrdinals();
        }

        public bool Remove(TableColumn item)
        {
            var returnValue = _TableColumns.Remove(item);
            RebuildOrdinals();
            return returnValue;
        }

        public void RemoveAt(int index)
        {
            _TableColumns.RemoveAt(index);
            RebuildOrdinals();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _TableColumns.GetEnumerator();
        }

        private void RebuildOrdinals()
        {
            _ColumnOrdinals.Clear();
            for (int i = 0; i < _TableColumns.Count; i++)
            {
                _ColumnOrdinals.Add(_TableColumns[i].ColumnName, i);
            }
        }
    }
}
