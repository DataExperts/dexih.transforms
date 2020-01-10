// using System;
// using System.Collections.Generic;
// using System.Data.Common;
// using System.IO;
// using System.Linq;
// using System.Threading;
// using System.Threading.Tasks;
// using Dexih.Utils.DataType;
//
//
// namespace dexih.transforms
// {
//
//     public class StreamMessagePack : Stream
//     {
//         private readonly DbDataReader _reader;
//         private MemoryStream _memoryStream;
//         private long _position;
//         private readonly long _maxRows;
//         private bool _first;
//         private readonly DataPack _data;
//
//         private readonly List<int> _ordinals;
//
//         private readonly bool _hasNodes = false;
//         
//         public StreamMessagePack(string name, DbDataReader reader, long maxRows = -1)
//         {
//             _reader = reader;
//             _memoryStream = new MemoryStream();
//             _position = 0;
//
//             _maxRows = maxRows <= 0 ? long.MaxValue : maxRows;
//             _first = true;
//
//             _ordinals = new List<int>();
//
//             _data = new DataPack
//             {
//                 Name = name
//             };
//
//             // if this is a transform, ignore any parent columns (this is when for writing nodes to json).
//             if (reader is Transform transform)
//             {
//                 var columns = transform.CacheTable.Columns;
//                 for (var i = 0; i < columns.Count; i++)
//                 {
//                     if (!columns[i].IsParent)
//                     {
//                         _ordinals.Add(i);
//                     }
//                 }
//
//                 _hasNodes = columns.Any(c => c.DataType == ETypeCode.Node);
//                 _data.Columns = columns.Select(c => new DataPackColumn(c)).ToArray();
//
//             }
//             else
//             {
//                 _ordinals = Enumerable.Range(0, reader.FieldCount).ToList();
//                 _data.Columns = Enumerable.Range(0, reader.FieldCount).Select(i =>
//                 {
//                     var fieldName = reader.GetName(i);
//                     var dataType = DataType.GetTypeCode(reader.GetFieldType(i), out _);
//                     return new DataPackColumn()
//                     {
//                         Name = fieldName, LogicalName = fieldName, DataType =  dataType, ChildColumns = null
//                     };
//                     
//                 }).ToArray();
//             }
//             
//             _memoryStream.Position = 0;
//         }
//         
//         public override void Flush()
//         {
//             throw new NotImplementedException();
//         }
//
//         public override int Read(byte[] buffer, int offset, int count)
//         {
//             return AsyncHelper.RunSync(() => ReadAsync(buffer, offset, count, CancellationToken.None));
//         }
//
//         public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
//         {
//             if (_first)
//             {
//                 _first = false;
//                 if (!_hasNodes && _reader is Transform transform)
//                 {
//                     _data.Data = await GetTransformRowsAsync(transform, cancellationToken);
//                 }
//                 else
//                 {
//                     _data.Data = await GetRowsAsync(_reader, cancellationToken);
//                 }
//                 var bytes = MessagePackSerializer.Serialize(_data);
//                 _memoryStream = new MemoryStream(bytes) {Position = 0};
//             }
//             
//             var readCount = _memoryStream.Read(buffer, offset,count);
//             _position += readCount;
//             return readCount;
//         }
//
//         private async  Task<List<object[]>> GetRowsAsync(DbDataReader reader, CancellationToken cancellationToken)
//         {
//             var rows = 0;
//             var data = new List<object[]>();
//             while (await reader.ReadAsync(cancellationToken) && rows < _maxRows)
//             {
//                     var row = new object[_reader.FieldCount];
//                     _reader.GetValues(row);
//                     if (_hasNodes)
//                     {
//                         for (var i = 0; i < row.Length; i++)
//                         {
//                             var cell = row[i];
//                             if (cell is DbDataReader childReader)
//                             {
//                                 row[i] = await GetRowsAsync(childReader, cancellationToken);
//                             }
//                         }
//                     }
//
//                     data.Add(row);
//                 rows++;
//             }
//
//             return data;
//         }
//
//         private async Task<List<object[]>> GetTransformRowsAsync(Transform reader, CancellationToken cancellationToken)
//         {
//             var rows = 0;
//             var data = new List<object[]>();
//             while (await reader.ReadAsync(cancellationToken) && rows < _maxRows)
//             {
//                 data.Add(reader.CurrentRow);
//                 rows++;
//             }
//             return data;
//         }
//         
//         public override void Close()
//         {
//             _memoryStream?.Close();
//             _reader?.Close();
//             base.Close();
//         }
//
//         public override long Seek(long offset, SeekOrigin origin)
//         {
//             throw new NotImplementedException();
//         }
//
//         public override void SetLength(long value)
//         {
//             throw new NotImplementedException();
//         }
//
//         public override void Write(byte[] buffer, int offset, int count)
//         {
//             throw new NotImplementedException();
//         }
//
//         public override bool CanRead => true;
//
//         public override bool CanSeek => false;
//
//         public override bool CanWrite => false;
//
//         public override long Length => -1;
//         public override long Position { get => _position; set => throw new NotSupportedException("The position cannot be set."); }
//
//     }
// }