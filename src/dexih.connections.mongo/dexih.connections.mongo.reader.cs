using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;
using MongoDB.Bson;
using MongoDB.Driver;

namespace dexih.connections.mongo
{
    public class ReaderMongo : Transform
    {
        private int _currentReadRow;

        private readonly ConnectionMongo _connection;
        private IAsyncCursor<BsonDocument> _documents;
        private IEnumerator<BsonDocument> _enumerator;

        public ReaderMongo(Connection connection, Table table)
        {
            _connection = (ConnectionMongo)connection;
            CacheTable = table;
        }
        
        public override string TransformName { get; } = "Azure Reader";
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            if (IsOpen)
            {
                throw new ConnectionException("The current connection is already open");
            }

            IsOpen = true;
            SelectQuery = requestQuery;
            GeneratedQuery = GetGeneratedQuery(requestQuery);

            _documents = await _connection.GetCollection(CacheTable.Name, requestQuery, cancellationToken);

            return true;
        }


        public override bool ResetTransform()
        {
            if (IsOpen)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            try
            {
                while (_enumerator == null || _enumerator.Current == null)
                {
                    if (!await _documents.MoveNextAsync(cancellationToken))
                    {
                        return null;
                    }

                    var document = _documents.Current;
                    _enumerator = document.GetEnumerator();
                    _enumerator.MoveNext();
                }
                
                var row = GetRow(_enumerator.Current);

                _enumerator.MoveNext();
                
                _currentReadRow++;

                return row;
            }
            catch (Exception ex)
            {
                throw new ConnectionException("The mongo storage table reader failed due to the following error: " + ex.Message, ex);
            }
        }

        private object[] GetRow(BsonDocument document)
        {
            var row = new object[CacheTable.Columns.Count];

            for (var i = 0; i < CacheTable.Columns.Count; i++)
            {
                var column = CacheTable.Columns[i];
                if( document.TryGetElement(column.Name, out var element))
                {
                    if (element.Value.IsValidDateTime)
                    {
                        row[i] = element.Value.ToUniversalTime().ToLocalTime();
                    }
                    else
                    {
                        var value = BsonTypeMapper.MapToDotNetValue(element.Value);
                        var converted = Operations.Parse(column.DataType, column.Rank, value);
                        row[i] = converted;
                    }
                }
            }
            
            return row;
        }

        public override async Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            Reset();
            Dispose();
            return await Open(auditKey, query, cancellationToken);
        }
    }
}
