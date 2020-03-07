﻿using System;
 using System.Collections.Generic;
 using System.Data.Common;
using System.IO;
 using System.Linq;
 using System.Threading;
using System.Threading.Tasks;
 using dexih.functions.Query;
 using dexih.transforms.Exceptions;
 using NetTopologySuite.Geometries;
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
        private readonly string _topNode;

        private List<int> _ordinals;
        private string _endWrite;
        private readonly SelectQuery _selectQuery = null;

        public StreamJson(DbDataReader reader, long maxRows = -1, string topNode = null)
        {
            _reader = reader;
            _memoryStream = new MemoryStream(BufferSize);
            _streamWriter = new StreamWriter(_memoryStream) {AutoFlush = true};
            _position = 0;
            _topNode = topNode;

            _maxRows = maxRows <= 0 ? long.MaxValue : maxRows;
            _rowCount = 0;
            _hasRows = true;
            _first = true;
        }
        
        public StreamJson(DbDataReader reader, SelectQuery selectQuery, string topNode = null)
        {
            _reader = reader;
            _memoryStream = new MemoryStream(BufferSize);
            _streamWriter = new StreamWriter(_memoryStream) {AutoFlush = true};
            _position = 0;
            _topNode = topNode;
            _selectQuery = selectQuery;

            var maxRows = selectQuery?.Rows ?? -1;
            _maxRows = maxRows <= 0 ? long.MaxValue : maxRows;
            _rowCount = 0;
            _hasRows = true;
            _first = true;
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
            try
            {
                
                if (_first)
                {
                    _ordinals = new List<int>();
            
                    // if this is a transform, ignore any parent columns (this is when for writing nodes to json).
                    if (_reader is Transform transform)
                    {
                        if (!transform.IsOpen)
                        {
                            var openReturn = await transform.Open(_selectQuery, cancellationToken);
                            
                            if (!openReturn) 
                            {
                                throw new TransformException("Failed to open the transform.");
                            }
                        }
                        
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
                        _ordinals = Enumerable.Range(0, _reader.FieldCount).ToList();
                    }

                    if(string.IsNullOrEmpty(_topNode))
                    {
                        _streamWriter.Write("[");
                        _endWrite = "]";
                    } else
                    {
                        _streamWriter.Write("{ \"" + _topNode + "\": [");
                        _endWrite = "]}";
                    }

                    _memoryStream.Position = 0;
                }

                if (!(_hasRows || _rowCount > _maxRows) && _memoryStream.Position >= _memoryStream.Length)
                {
                    _reader.Close();
                    return 0;
                }
                
                var readCount = _memoryStream.Read(buffer, offset, count);

                // if the buffer already has enough content.
                if (readCount < count && count > _memoryStream.Length - _memoryStream.Position)
                {
                    _memoryStream.SetLength(0);

                    if (_first)
                    {
                        _hasRows = await _reader.ReadAsync(cancellationToken);

                        if (_hasRows == false)
                        {
                            await _streamWriter.WriteAsync(_endWrite);
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

                        foreach (var i in _ordinals)
                        {
                            if (_reader[i] is byte[])
                            {
                                jObject[_reader.GetName(i)] = "binary data not viewable.";
                                continue;
                            }

                            if (_reader[i] is Geometry geometry)
                            {
                                jObject[_reader.GetName(i)] = geometry.AsText();
                                continue;
                            }

                            if (_reader[i] is null || _reader[i] is DBNull)
                            {
                                jObject[_reader.GetName(i)] = null;
                                continue;
                            }

                            jObject[_reader.GetName(i)] = JToken.FromObject(_reader[i]);
                        }

                        var row = jObject.ToString();
                        await _streamWriter.WriteAsync(row);

                        _rowCount++;
                        _hasRows = await _reader.ReadAsync(cancellationToken);

                        if (_hasRows && _rowCount < _maxRows)
                        {
                            await _streamWriter.WriteAsync(",");
                        }
                        else
                        {
                            await _streamWriter.WriteAsync(_endWrite);
                            _hasRows = false;
                            break;
                        }
                    }

                    _memoryStream.Position = 0;

                    readCount += _memoryStream.Read(buffer, readCount, count - readCount);
                }

                _position += readCount;

                return readCount;
            } catch
            {
                _reader.Close();
                throw;
            }
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
