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
        private bool _isOpen;
        private IEnumerator<object[]> _cachedRows;

        private List<Filter> _filter;

        private WebService _restFunction;

        public ReaderRestful(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;

            _restFunction = (WebService)table;
        }

        protected override void Dispose(bool disposing)
        {
            _isOpen = false;

            base.Dispose(disposing);
        }

        public override Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
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

                _filter = query?.Filters;
                if (_filter == null)
                {
                    _filter = new List<Filter>();
                }
                
                _isOpen = true;

                //create a dummy inreader to allow fieldcount and other queries to work.
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Opening the web service reader failed. {ex.Message}", ex);
            }
        }

        public override string Details()
        {
            return "Restful WebService";
        }

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                if(!_isOpen)
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

                var rows = await LookupRowDirect(_filter, EDuplicateStrategy.All, cancellationToken);
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



        public override bool CanLookupRowDirect { get; } = true;

        /// <inheritdoc />
        public override async Task<ICollection<object[]>> LookupRowDirect(List<Filter> filters, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken)
        {
            return await ((ConnectionRestful) ReferenceConnection).LookupRow(CacheTable, filters, cancellationToken);
         }
    }
}
