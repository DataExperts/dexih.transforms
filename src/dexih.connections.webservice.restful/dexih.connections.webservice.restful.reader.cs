using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Exceptions;

namespace dexih.connections.webservice.restful
{
    public class ReaderRestful : Transform
    {
        private IEnumerator<object[]> _cachedRows;

        private Filters _filter;

        public ReaderRestful(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
        }

        public override string TransformName { get; } = "Restful Web Service Reader";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            SelectQuery = requestQuery;

            try
            {
                //if (_isOpen)
                //{
                //    throw new ConnectionException($"The webservice is already open");
                //}

                var rowCreator = new ReaderRowCreator();
                rowCreator.InitializeRowCreator(1, 1, 1);
                ReferenceTransform = rowCreator;

                _filter = requestQuery?.Filters;
                if (_filter == null)
                {
                    _filter = new Filters();
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
