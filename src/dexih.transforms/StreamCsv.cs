using System;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    
    /// <summary>
    /// Converts a DbDataReader into an output csv stream
    /// </summary>
    public class StreamCsv : Stream
    {
        private const int BufferSize = 50000;
        private DbDataReader _reader;
        private readonly MemoryStream _memoryStream;
        private readonly StreamWriter _streamWriter;
        private long _position;

        private readonly char[] _quoteCharacters = new char[] { '"', ' ', ',' };

        private bool _isFirst = true;

        public StreamCsv(DbDataReader reader)
        {
            _reader = reader;
            _memoryStream = new MemoryStream(BufferSize);
            _streamWriter = new StreamWriter(_memoryStream) {AutoFlush = true};
            
        }
        
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => -1;

        public override long Position { get => _position; set => throw new NotSupportedException("The position cannot be set."); }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return AsyncHelper.RunSync(() => ReadAsync(buffer, offset, count, CancellationToken.None));
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (_isFirst)
            {
                _position = 0;

                //write the file header.
                // if this is a transform, then use the dataTypes from the cache table
                if (_reader is Transform transform)
                {
                    var convertedTransform = new ReaderConvertDataTypes(new ConnectionConvertString(), transform);
                    _reader = convertedTransform;

                    var s = new string[transform.CacheTable.Columns.Count];
                    for (var j = 0; j < transform.CacheTable.Columns.Count; j++)
                    {
                        s[j] = transform.CacheTable.Columns[j].LogicalName;
                        if (string.IsNullOrEmpty(s[j]))
                        {
                            s[j] = transform.CacheTable.Columns[j].Name;
                        }
                    
                        if (s[j].Contains("\"")) //replace " with ""
                            s[j] = s[j].Replace("\"", "\"\"");
                        if (s[j].IndexOfAny(_quoteCharacters) != -1) //add "'s around any string with space or "
                            s[j] = "\"" + s[j] + "\"";
                    }
                    await _streamWriter.WriteLineAsync(string.Join(",", s));
                }
                else
                {
                    var s = new string[_reader.FieldCount];
                    for (var j = 0; j < _reader.FieldCount; j++)
                    {
                        s[j] = _reader.GetName(j);
                        if (s[j].Contains("\"")) //replace " with ""
                            s[j] = s[j].Replace("\"", "\"\"");
                        if (s[j].IndexOfAny(_quoteCharacters) != -1) //add "'s around any string with space or "
                            s[j] = "\"" + s[j] + "\"";
                    }
                    await _streamWriter.WriteLineAsync(string.Join(",", s));
                }

                _memoryStream.Position = 0;

                _isFirst = false;
            }

            if(!_reader.HasRows && _memoryStream.Position >= _memoryStream.Length)
            {
                _reader.Close();
                return 0;
            }

            var readCount = _memoryStream.Read(buffer, offset, count);

            // if the buffer already has enough content.
            if (readCount < count && count > _memoryStream.Length - _memoryStream.Position)
            {
                _memoryStream.SetLength(0);

                // populate the stream with rows, up to the buffer size.
                while (await _reader.ReadAsync(cancellationToken) )
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        _reader.Close();
                        return 0;
                    }

                    var s = new string[_reader.FieldCount];
                    for (var j = 0; j < _reader.FieldCount; j++)
                    {
                        s[j] = _reader.GetString(j);
                        if (s[j].Contains("\"")) //replace " with ""
                            s[j] = s[j].Replace("\"", "\"\"");
                        if (s[j].IndexOfAny(_quoteCharacters) != -1) //add "'s around any string with space or "
                            s[j] = "\"" + s[j] + "\"";
                    }
                    await _streamWriter.WriteLineAsync(string.Join(",", s));

                    if (_memoryStream.Length > count && _memoryStream.Length > BufferSize) break;
                }

                _memoryStream.Position = 0;

                readCount += _memoryStream.Read(buffer, readCount, count - readCount);
            }

            _position += readCount;

            return readCount;
        }
        
        public override void Close()
        {
            _streamWriter?.Close();
            _memoryStream?.Close();
            _reader?.Close();
            base.Close();
        }


        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
