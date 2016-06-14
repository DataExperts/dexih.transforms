using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class TableCache : IList<object[]>
    {
        private int MaxRows;
        private List<object[]> Data;
        private int StartIndex = 0;

        public TableCache(int maxRows = 0)
        {
            MaxRows = maxRows;
            Data = new List<object[]>();
            StartIndex = 0;
        }

        private int InternalIndex(int index)
        {
            return MaxRows == 0 ? index : (index + StartIndex) % MaxRows;
        }

        public object[] this[int index]
        {
            get
            {
                return Data[InternalIndex(index)];
            }

            set
            {
                Data[InternalIndex(index)] = value;
            }
        }

        public int Count
        {
            get
            {
                return Data.Count;
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
            if (MaxRows == 0 || Data.Count < MaxRows)
            {
                Data.Add(item);
            }
            else
            {
                Data[StartIndex] = item;
                StartIndex++;
                if (StartIndex > MaxRows)
                    StartIndex = 0;
            }
        }

        public void Clear()
        {
            Data.Clear();
            StartIndex = 0;
        }

        public bool Contains(object[] item)
        {
            return Data.Contains(item);
        }

        public void CopyTo(object[][] array, int arrayIndex)
        {
            Data.CopyTo(array, InternalIndex(arrayIndex));
        }

        public IEnumerator<object[]> GetEnumerator()
        {
            return new TableCacheEnumerator(Data, StartIndex);
        }

        public int IndexOf(object[] item)
        {
            int index = Data.IndexOf(item);

            if (index >= 0 && MaxRows > 0)
            {
                index = index - StartIndex;
                if (index < 0)
                    index = MaxRows + index;
            }

            return index;
        }

        public void Insert(int index, object[] item)
        {
            if (MaxRows == 0)
                Data.Insert(index, item);
            else
                throw new NotImplementedException("Insert is not supported with this collection.");
        }

        public bool Remove(object[] item)
        {
            if (MaxRows == 0)
                return Data.Remove(item);
            else
                throw new NotImplementedException("Remove is not supported with this collection.");
        }

        public void RemoveAt(int index)
        {
            if (MaxRows == 0)
                Data.RemoveAt(index);
            else
                throw new NotImplementedException("RemoveAt is not supported with this collection.");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

    public class TableCacheEnumerator : IEnumerator<object[]>
    {
        private List<object[]> Data;
        private int StartIndex = 0;

        private int EnumeratorPosition;
        private bool IsFirst;
        private bool IsFinished;


        public TableCacheEnumerator(List<object[]> data, int startIndex)
        {
            Data = data;
            StartIndex = startIndex;
            IsFirst = true;
            IsFinished = false;

        }
        public object[] Current
        {
            get
            {
                if (!IsFinished)
                    return Data[EnumeratorPosition];
                else
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
            Data = null; 
        }

        public bool MoveNext()
        {
            if (Data.Count == 0 || IsFinished)
                return false;

            if (IsFirst)
            {
                EnumeratorPosition = StartIndex;
                IsFirst = false;
                return true;
            }

            EnumeratorPosition++;

            if (EnumeratorPosition >= Data.Count)
                EnumeratorPosition = 0;

            if (EnumeratorPosition == StartIndex)
            {
                IsFinished = true;
                return false;
            }

            return true;
        }

        public void Reset()
        {
            StartIndex = EnumeratorPosition;
        }
    }



}



