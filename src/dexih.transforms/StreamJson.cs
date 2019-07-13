﻿using System;
 using System.Collections.Generic;
 using System.Data.Common;
using System.IO;
 using System.Linq;
 using System.Threading;
using System.Threading.Tasks;
 using Dexih.Utils.CopyProperties;
 using Dexih.Utils.Crypto;
 using Dexih.Utils.MessageHelpers;
 using Newtonsoft.Json.Linq;

 namespace dexih.transforms
{
    

    /// <summary>
    /// Writes out a simple json stream containing the headers and data for the reader.
    /// </summary>
    public class StreamJson : Stream
    {
        private const int BufferSize = 50000;
        private readonly DbDataReader _reader;
        private readonly MemoryStream _memoryStream;
        private readonly StreamWriter _streamWriter;
        private long _position;
        private readonly long _maxRows;
        private long _rowCount;
        private bool _hasRows;
        private bool _first;

        private readonly List<int> _ordinals;
        private string endWrite;

        public StreamJson(string name, DbDataReader reader, long maxRows = -1, string topNode = null)
        {
            _reader = reader;
            _memoryStream = new MemoryStream(BufferSize);
            _streamWriter = new StreamWriter(_memoryStream) {AutoFlush = true};
            _position = 0;

            _maxRows = maxRows <= 0 ? long.MaxValue : maxRows;
            _rowCount = 0;
            _hasRows = true;
            _first = true;

            _ordinals = new List<int>();
            
            // if this is a transform, ignore any parent columns (this is when for writing nodes to json).
            if (reader is Transform transform)
            {
                var columns = transform.CacheTable.Columns;
                for (var i = 0; i < columns.Count; i++)
                {
                    if (!columns[i].IsParent)
                    {
                        _ordinals.Add(i);
                    }
                }
            }
            else
            {
                _ordinals = Enumerable.Range(0, reader.FieldCount).ToList();
            }

            if(string.IsNullOrEmpty(topNode))
            {
                _streamWriter.Write("[");
                endWrite = "]";
            } else
            {
                _streamWriter.Write("{ \"" + topNode + "\": [");
                endWrite = "]}";
            }
            
            
            _memoryStream.Position = 0;
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
            return ReadAsync(buffer, offset, count, CancellationToken.None).Result;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
        {
            if(!(_hasRows || _rowCount > _maxRows) && _memoryStream.Position >= _memoryStream.Length)
            {
                _reader.Close();
                return 0;
            }

            var readCount = await _memoryStream.ReadAsync(buffer, offset, count, cancellationToken);

            // if the buffer already has enough content.
            if (readCount < count && count > _memoryStream.Length - _memoryStream.Position)
            {
                _memoryStream.SetLength(0);

                if (_first)
                {
                    try
                    {
                        _hasRows = await _reader.ReadAsync(cancellationToken);

                        if (_hasRows == false)
                        {
                            await _streamWriter.WriteAsync(endWrite);
                        }
                    }
                    catch (Exception ex)
                    {
                        var status = new ReturnValue(false, ex.Message, ex);
                        var result = Json.SerializeObject(status, "");
                        await _streamWriter.WriteAsync(endWrite + ", \"status\"=" + result + " }");
                        _hasRows = false;
                    }

                    _first = false;
                }

                // populate the stream with rows, up to the buffer size.
                while (_hasRows)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        _reader.Close();
                        return 0;
                    }

                    var jObject = new JObject();

                    foreach(var i in _ordinals)
                    {
                        if (_reader[i] is byte[])
                        {
                            jObject[_reader.GetName(i)] = "binary data not available.";
                            continue;
                        }

                        jObject[_reader.GetName(i)] = JToken.FromObject(_reader[i]);
                    }

                    var row = jObject.ToString();
                    await _streamWriter.WriteAsync(row);

                    _rowCount++;
                    try
                    {
                        _hasRows = await _reader.ReadAsync(cancellationToken);
                        
                        if (_hasRows && _rowCount < _maxRows)
                        {
                            await _streamWriter.WriteAsync(",");
                        }
                        else
                        {
                            await _streamWriter.WriteAsync(endWrite);
                            _hasRows = false;
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        var status = new ReturnValue(false, ex.Message, ex);
                        var result = Json.SerializeObject(status, "");
                        await _streamWriter.WriteAsync(endWrite + ", \"status\"=" + result + " }");
                        _hasRows = false;
                    }
                }

                _memoryStream.Position = 0;

                readCount += await _memoryStream.ReadAsync(buffer, readCount, count - readCount, cancellationToken);
            }

            _position += readCount;

            return readCount;
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
            _reader?.Close();
            base.Close();
        }
    }
    
}
