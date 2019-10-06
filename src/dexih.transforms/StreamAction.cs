using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;


namespace dexih.transforms
{
    
    /// <summary>
    /// Creates a stream using the json result from the func
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class StreamAction<T>: Stream
    {
        private readonly MemoryStream _memoryStream;
        private readonly StreamWriter _streamWriter;
        
        private readonly Func<T> _func;
        private bool _isFirst = true;

        public StreamAction(Func<T> func)
        {
            _func = func;
            _memoryStream = new MemoryStream();
            _streamWriter = new StreamWriter(_memoryStream) {AutoFlush = true};
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => -1;

        public override long Position { get => _memoryStream?.Position ?? -1; set => throw new NotSupportedException("The position cannot be set."); }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_isFirst)
            {
                var value = _func.Invoke();
                var json = value.Serialize();
                _streamWriter.Write(json);
                _memoryStream.Position = 0;
                _isFirst = false;
            }

            var readCount = _memoryStream.Read(buffer, offset, count);
            return readCount;        
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }
            var position = Read(buffer, offset, count);
            return Task.FromResult(position);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("The Seek function is not supported.");
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("The SetLength function is not supported.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("The Write function is not supported.");
        }
        
        public override void Close()
        {
            _streamWriter?.Close();
            _memoryStream?.Close();
            base.Close();
        }
    }
}