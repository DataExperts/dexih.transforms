using System;
using System.Collections;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms.Poco
{
    public class PocoEnumerator<T>: IPocoEnumerator<T> 
    {
        private readonly DbDataReader _reader;
        private T _item;
        private bool _open;
        private readonly PocoMapper<T> _pocoMapper;
        
        public PocoEnumerator(DbDataReader reader)
        {
            _reader = reader;
            _open = true;
            
            _pocoMapper = new PocoMapper<T>(reader);
        }

        public bool MoveNext()
        {
            return AsyncHelper.RunSync(() => MoveNextAsync(CancellationToken.None));
        }
        
        public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                if (_open)
                {
                    _open = await _reader.ReadAsync(cancellationToken);
                    if (_open)
                    {
                        _item = _pocoMapper.GetItem();
                    }
                }
                else
                {
                    throw new PocoLoaderClosedException("The data reader is closed.");
                }

                return _open;

            }
            catch (Exception ex)
            {
                throw new PocoException("The data reader could not be read, see inner exception for details.", ex);
            }  
        }

        public T this[int index] => throw new NotImplementedException();

        public int Count => throw new NotImplementedException();
        public Task<int> CountAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new PocoLoaderNoResetException("The data reader cannot be reset.");
        }

        public T Current => _item;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _reader.Dispose();
        }
    }
}