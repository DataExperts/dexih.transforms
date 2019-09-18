using MessagePack;
using System;
using System.Collections;
using System.Collections.Generic;

namespace dexih.functions
{
    // [MessagePackObject]
    public class TableCache : IList<object[]>
    {
        private readonly int _maxRows;
        private IList<object[]> _data;
        private int _startIndex;

        public TableCache()
        {
            _maxRows = 0;
            _data = new List<object[]>();
            _startIndex = 0;
        }

        /// <summary>
        /// </summary>
        /// <param name="maxRows">Sets the maximum rows loaded into the cache.  After this is reached every new row added, will have the
        /// oldest row drop off. Zero = unlimited cache size</param>
        public TableCache(int maxRows = 0)
        {
            _maxRows = maxRows;
            _data = new List<object[]>();
            _startIndex = 0;
        }
        
        

        /// <summary>
        /// converts the rolling cache into the actual index.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        private int InternalIndex(int index)
        {
            return _maxRows <= 0 ? index : (index + _startIndex) % _maxRows;
        }
        
        public object[] this[int index]
        {
            get => _data[InternalIndex(index)];
            set => _data[InternalIndex(index)] = value;
        }

        public int Count => _data?.Count ?? 0;

        public bool IsReadOnly => false;


        public void Add(object[] item)
        {
            if (_data == null) _data = new List<object[]>();
            
            if (_maxRows <= 0 || Count < _maxRows)
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



        public void AddRange(IEnumerable<object[]> items)
        {
            foreach(var item in items)
            {
                Add(item);
            }
        }

        public void Set(IList<object[]> data)
        {
            _data = data;
        }

        public void Clear()
        {
            _data?.Clear();
            _startIndex = 0;
        }

        public bool Contains(object[] item)
        {
            return _data?.Contains(item) ?? false;
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
            if (_maxRows <= 0)
                _data.Insert(index, item);
            else
                throw new NotImplementedException("Insert is not supported with this collection.");
        }

        public bool Remove(object[] item)
        {
            if (_maxRows <= 0)
                return _data.Remove(item);
            throw new NotImplementedException("Remove is not supported with this collection.");
        }

        public void RemoveAt(int index)
        {
            if (_maxRows <= 0)
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

    public class TableCacheEnumerator : IEnumerator<object[]>
    {
        private IList<object[]> _data;
        private int _startIndex;

        private int _enumeratorPosition;
        private bool _isFirst;
        private bool _isFinished;


        public TableCacheEnumerator(IList<object[]> data, int startIndex)
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
                return !_isFinished ? _data[_enumeratorPosition] : null;
            }
        }

        object IEnumerator.Current => Current;

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



