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
    public class TransformCsvStream : Stream
    {
        private const int BufferSize = 50000;
        private readonly DbDataReader _reader;
        private readonly MemoryStream _memoryStream;
        private readonly StreamWriter _streamWriter;
        private long _position;
        

        public TransformCsvStream(DbDataReader reader)
        {
            _reader = reader;
            _memoryStream = new MemoryStream(BufferSize);
            _streamWriter = new StreamWriter(_memoryStream) {AutoFlush = true};
            _position = 0;

            //write the file header.
            var s = new string[reader.FieldCount];
            for (var j = 0; j < reader.FieldCount; j++)
            {
                s[j] = reader.GetName(j);
                if (s[j].Contains("\"")) //replace " with ""
                    s[j] = s[j].Replace("\"", "\"\"");
                if (s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                    s[j] = "\"" + s[j] + "\"";
            }
            _streamWriter.WriteLine(string.Join(",", s));
            _memoryStream.Position = 0;
        }
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => -1;

        public override long Position { get => _position; set => throw new NotSupportedException("The position cannot be set."); }

        public override void Flush()
        {
            return;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer, offset, count, CancellationToken.None).Result;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if(!_reader.HasRows && _memoryStream.Position >= _memoryStream.Length)
            {
                return 0;
            }

            var readCount = await _memoryStream.ReadAsync(buffer, offset, count, cancellationToken);

            // if the buffer already has enough content.
            if (readCount < count && count > _memoryStream.Length - _memoryStream.Position)
            {
                _memoryStream.SetLength(0);

                // populate the stream with rows, up to the buffer size.
                while (await _reader.ReadAsync(cancellationToken) )
                {
                    if (cancellationToken.IsCancellationRequested)
                        return 0;

                    var s = new string[_reader.FieldCount];
                    for (var j = 0; j < _reader.FieldCount; j++)
                    {
                        s[j] = _reader.GetString(j);
                        if (s[j].Contains("\"")) //replace " with ""
                            s[j] = s[j].Replace("\"", "\"\"");
                        if (s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                            s[j] = "\"" + s[j] + "\"";
                    }
                    await _streamWriter.WriteLineAsync(string.Join(",", s));

                    if (_memoryStream.Length > count && _memoryStream.Length > BufferSize) break;
                }

                _memoryStream.Position = 0;

                readCount += await _memoryStream.ReadAsync(buffer, readCount, count - readCount, cancellationToken);
            }

            _position += readCount;

            return readCount;
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
