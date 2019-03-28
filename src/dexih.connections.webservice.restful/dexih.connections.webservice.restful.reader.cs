using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using System.Linq;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.connections.webservice
{
    public class ReaderRestful : Transform
    {
        private IEnumerator<object[]> _cachedRows;

        private List<Filter> _filter;

        private WebService _restFunction;

        public ReaderRestful(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;

            _restFunction = (WebService)table;
        }

        public override string TransformName { get; } = "Restful Web Service Reader";
        public override string TransformDetails => CacheTable?.Name ?? "Unknown";


        public override Task<bool> Open(long auditKey, SelectQuery selectQuery, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;

            try
            {
                //if (_isOpen)
                //{
                //    throw new ConnectionException($"The webservice is already open");
                //}

                var rowCreator = new ReaderRowCreator();
                rowCreator.InitializeRowCreator(1, 1, 1);
                ReferenceTransform = rowCreator;

                _filter = selectQuery?.Filters;
                if (_filter == null)
                {
                    _filter = new List<Filter>();
                }
                
                IsOpen = true;

                //create a dummy inreader to allow fieldcount and other queries to work.
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Opening the web service reader failed. {ex.Message}", ex);
            }
        }


        public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            try
            {
                if(!IsOpen)
                {
                    throw new ConnectionException($"The web service is not open");
                }

                if(_cachedRows == null && await ReferenceTransform.ReadAsync(cancellationToken) == false)
                {
                    return null;
                }

                if(_cachedRows != null)
                {
                    if (!_cachedRows.MoveNext())
                    {
                        _cachedRows = null;
                        if (await ReferenceTransform.ReadAsync(cancellationToken) == false)
                        {
                            return null;
                        }
                    }
                    else
                    {
                        var row = _cachedRows.Current;
                        return row;
                    }

                }

                var rows = await ((ConnectionRestful) ReferenceConnection).LookupRow(CacheTable, _filter, cancellationToken);
                if(rows != null && rows.Any())
                {
                    _cachedRows = rows.GetEnumerator();
                    _cachedRows.MoveNext();

                    return _cachedRows.Current;
                }

                return null;

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Read web service record failed. {ex.Message}", ex);
            }
        }

        public override async Task<bool> InitializeLookup(long auditKey,SelectQuery query, CancellationToken cancellationToken = default)
        {
            Reset();
            return await Open(auditKey, query, cancellationToken);
        }

    }
}
