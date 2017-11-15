using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace dexih.functions
{
    [Serializable]
    public class TableCache : IList<object[]>
    {
        private readonly int _maxRows;
        private readonly List<object[]> _data;
        private int _startIndex;

        public TableCache()
        {
            _maxRows = 0;
            _data = new List<object[]>();
            _startIndex = 0;
        }

        public TableCache(int maxRows = 0)
        {
            _maxRows = maxRows;
            _data = new List<object[]>();
            _startIndex = 0;
        }

        private int InternalIndex(int index)
        {
            return _maxRows == 0 ? index : (index + _startIndex) % _maxRows;
        }

        public object[] this[int index]
        {
            get
            {
                return _data[InternalIndex(index)];
            }

            set
            {
                _data[InternalIndex(index)] = value;
            }
        }

        public int Count
        {
            get
            {
                return _data.Count;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }



        public void Add(object[] item)
        {
            if (_maxRows <= 0 || _data.Count < _maxRows)
            {
                _data.Add(item);
            }
            else
            {
                _data[_startIndex] = item;
                _startIndex++;
                if (_startIndex > _maxRows)
                    _startIndex = 0;
            }
        }

        public void Add(IEnumerable<object[]> items)
        {
            foreach(var item in items)
            {
                Add(item);
            }
        }

        public void Clear()
        {
            _data.Clear();
            _startIndex = 0;
        }

        public bool Contains(object[] item)
        {
            return _data.Contains(item);
        }

        public void CopyTo(object[][] array, int arrayIndex)
        {
            _data.CopyTo(array, InternalIndex(arrayIndex));
        }

        public IEnumerator<object[]> GetEnumerator()
        {
            return new TableCacheEnumerator(_data, _startIndex);
        }

        public int IndexOf(object[] item)
        {
            var index = _data.IndexOf(item);

            if (index >= 0 && _maxRows > 0)
            {
                index = index - _startIndex;
                if (index < 0)
                    index = _maxRows + index;
            }

            return index;
        }

        public void Insert(int index, object[] item)
        {
            if (_maxRows == 0)
                _data.Insert(index, item);
            else
                throw new NotImplementedException("Insert is not supported with this collection.");
        }

        public bool Remove(object[] item)
        {
            if (_maxRows == 0)
                return _data.Remove(item);
            throw new NotImplementedException("Remove is not supported with this collection.");
        }

        public void RemoveAt(int index)
        {
            if (_maxRows == 0)
                _data.RemoveAt(index);
            else
                throw new NotImplementedException("RemoveAt is not supported with this collection.");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Workaround to remove DbNulls from the data, as these will not serialiaze.
        /// </summary>
        public void ClearDbNullValues()
        {
            foreach (var row in this)
            {
                for (var i = 0; i < row.Length; i++)
                {
                    if (row[i] is DBNull)
                    {
                        row[i] = null;
                    }
                }
            }
        }

 
    }

    [Serializable]
    public class TableCacheEnumerator : IEnumerator<object[]>
    {
        private List<object[]> _data;
        private int _startIndex;

        private int _enumeratorPosition;
        private bool _isFirst;
        private bool _isFinished;


        public TableCacheEnumerator(List<object[]> data, int startIndex)
        {
            _data = data;
            _startIndex = startIndex;
            _isFirst = true;
            _isFinished = false;

        }
        public object[] Current
        {
            get
            {
                if (!_isFinished)
                    return _data[_enumeratorPosition];
                return null;
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return Current;
            }
        }

        public void Dispose()
        {
            _data = null; 
        }

        public bool MoveNext()
        {
            if (_data.Count == 0 || _isFinished)
                return false;

            if (_isFirst)
            {
                _enumeratorPosition = _startIndex;
                _isFirst = false;
                return true;
            }

            _enumeratorPosition++;

            if (_enumeratorPosition >= _data.Count)
                _enumeratorPosition = 0;

            if (_enumeratorPosition == _startIndex)
            {
                _isFinished = true;
                return false;
            }

            return true;
        }

        public void Reset()
        {
            _startIndex = _enumeratorPosition;
        }

    }



}



