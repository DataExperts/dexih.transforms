using System;
using System.Collections;

namespace dexih.connections
{
    public class DexihFileProperties
    {
        public string FileName { get; set; }
        public DateTime LastModified { get; set; }
        public long Length { get; set; }

        public string ContentType { get; set; }

    }

    public class DexihFiles :IEnumerator
    {
        DexihFileProperties[] _files;

        // Enumerators are positioned before the first element 
        // until the first MoveNext() call. 
        int _position;

        public DexihFiles(DexihFileProperties[] files)
        {
            _files = files;
            _position = -1;
        }

        public bool MoveNext()
        {
            _position++;
            return (_position < _files.Length);
        }

        public void Reset()
        {
            _position = -1;
        }

        object IEnumerator.Current => Current;

        public DexihFileProperties Current
        {
            get
            {
                try
                {
                    return _files[_position];
                }
                catch (IndexOutOfRangeException)
                {
                    throw new InvalidOperationException();
                }
            }
        }
    }
}
