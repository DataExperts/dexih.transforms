using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms.Poco
{
    public class PocoCachedEnumerator<T>:IPocoEnumerator<T>   {
        private readonly DbDataReader _reader;
        private int _rowNumber;
        private readonly List<T> _data;
        private bool _open;
        private int _maxRow;
        private readonly PocoMapper<T> _pocoMapper;
        
        public PocoCachedEnumerator(DbDataReader reader)
        {
            _reader = reader;
            _rowNumber = -1;
            _maxRow = -1;
            _open = true;
            
            _pocoMapper = new PocoMapper<T>(reader);
            _data = new List<T>();
        }
        
        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
        {
            if (!_open && _rowNumber > _maxRow)
            {
                return false;
            }
            _rowNumber++;
            if (_rowNumber <= _maxRow)
            {
                return true;
            }

            await ReadNextAsync(cancellationToken);
            return _open;
        }

        private async Task<bool> ReadNextAsync(CancellationToken cancellationToken = default)
        {
            if (_open)
            {
                _open = await _reader.ReadAsync(cancellationToken);
                if (_open)
                {
                    var item = _pocoMapper.GetItem();
                    _data.Add(item);
                    _maxRow++;
                }
            }
            return _open;
        }
        
        public void Reset()
        {
            _rowNumber = -1;
        }

        public T Current => _data[_rowNumber];

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _reader.Dispose();
        }

        public bool MoveNext()
        {
            return AsyncHelper.RunSync(() => MoveNextAsync(CancellationToken.None));
        }

        public T this[int index]
        {
            get
            {
                if (_maxRow >= index)
                {
                    return _data[index];
                }
                else
                {
                    while (AsyncHelper.RunSync(() => ReadNextAsync(CancellationToken.None)) && _maxRow < index)
                    {}

                    if (_maxRow >= index)
                    {
                        return _data[index];
                    }

                    throw new PocoLoaderIndexOutOfBoundsException(
                        $"The index ${index} exceeded the number of items (${_maxRow}) available.");
                }
            }
        }

        public async Task<int> CountAsync(CancellationToken cancellationToken = default)
        {
            while (_open && await ReadNextAsync(cancellationToken))
            {
            }
            return _maxRow+1;
        }

        public int Count =>  AsyncHelper.RunSync(() => CountAsync(CancellationToken.None));
    }
}