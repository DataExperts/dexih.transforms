using dexih.functions;
using dexih.functions.Query;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    /// <summary>
    /// A source transform that uses a pre-populated Table as an input.
    /// </summary>
    public sealed class ReaderMemory : Transform
    {
        public Table DataTable { get; set; }

        private IList<object[]> _data;
        private int _currentRow;
        private Connection _convertConnection;
        
        // flag used to indicate if the cache has loaded, so no more records will be loaded 
        // after resets and row positions.
        private bool _cacheLoaded = false;

        #region Constructors

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="sortFields"></param>
        /// <param name="convertConnection">Connection to use to convert the data to, if null no conversion occurs.</param>
        public ReaderMemory(Table dataTable, Sorts sortFields = null, Connection convertConnection = null)
        {
            CacheTable = new Table(dataTable.Name, dataTable.Columns, new TableCache())
            {
                OutputSortFields = sortFields
            };
            
            DataTable = dataTable;
            _data = dataTable.Data;
            _convertConnection = convertConnection;
            
            Reset();

            IsOpen = true;
        }
        
        public override Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            IsOpen = true;
            SelectQuery = requestQuery;
            _data = DataTable.Data;

            if (CacheTable.OutputSortFields?.Count > 0)
            {
                GeneratedQuery = new SelectQuery()
                {
                    Sorts = CacheTable.OutputSortFields
                };
            }

            return Task.FromResult(true);
        }
        
        #endregion

        public override string TransformName { get; } = "Memory Reader";

        public override Dictionary<string, object> TransformProperties()
        {
            if (CacheTable != null)
            {
                return new Dictionary<string, object>()
                {
                    {"CacheTable", CacheTable.Name}
                };
            }

            return null;
        }
        
        public override bool ResetTransform()
        {
            _currentRow = -1;
            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_data == null)
            {
                return Task.FromResult<object[]>(null);
            }

            _currentRow++;
            while(!_cacheLoaded && _currentRow < _data.Count)
            {
                var row = _data[_currentRow];
                var filtered = SelectQuery?.EvaluateRowFilter(row, CacheTable)?? true;
                if(!filtered)
                {
                    _currentRow++;
                    continue;
                }

                if(_convertConnection != null)
                {
                    for (var i = 0; i < CacheTable.Columns.Count; i++)
                    {
                        switch (CacheTable.Columns[i].DeltaType)
                        {
                            case EDeltaType.DatabaseOperation:
                            case EDeltaType.DbAutoIncrement:
                                continue;
                        }

                        row[i] = _convertConnection.ConvertForWrite(CacheTable.Columns[i], row[i]).value;
                    }
                }
                return Task.FromResult(row);
            }

            if (CacheMethod != ECacheMethod.NoCache)
            {
                _cacheLoaded = true;
            }

            return Task.FromResult<object[]>(null);
        }

        public override bool IsClosed => _currentRow >= _data.Count;
        public override bool HasRows => _currentRow < _data.Count && _data.Count > 0;
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            Reset(true);
            _cacheLoaded = false;
            _currentRow = -1;
            return Open(auditKey, query, cancellationToken);
        }
    }
}
