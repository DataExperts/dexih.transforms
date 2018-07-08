using dexih.functions;
using dexih.functions.Query;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    /// <summary>
    /// A source transform that uses a prepopulated Table as an input.
    /// </summary>
    public sealed class ReaderMemory : Transform
    {
        private SelectQuery _selectQuery;
        private readonly TableCache _data;
        private int _currentRow;
        
        // flag used to indicate if the cache has loaded, so no more records will be loaded 
        // after resets and row repositisons.
        private bool _cacheLoaded = false;

//        public override ECacheMethod CacheMethod
//        {
//            get => ECacheMethod.NoCache;
//            protected set =>
//                throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
//        }

        #region Constructors

        public ReaderMemory(Table dataTable, List<Sort> sortFields = null)
        {
            CacheTable = new Table(dataTable.Name, dataTable.Columns, new TableCache()) {OutputSortFields = sortFields};

            _data = dataTable.Data;
            
            Reset();

            SortFields = sortFields;
        }

        public override Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            _selectQuery = query;

            return Task.FromResult(true);
        }

        public override List<Sort> SortFields { get; }

        public void Add(object[] values)
        {
            _data.Add(values);
        }

        #endregion

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override string Details()
        {
            return "Memory Table " + CacheTable.Name;
        }

        public override bool ResetTransform()
        {
            _currentRow = -1;
            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            _currentRow++;
            while(!_cacheLoaded && _currentRow < _data.Count)
            {
                var row = _data[_currentRow];
                var filtered = _selectQuery?.EvaluateRowFilter(row, CacheTable)?? true;
                if(!filtered)
                {
                    _currentRow++;
                    continue;
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
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            Reset();
            return Open(auditKey, query, cancellationToken);
        }
    }
}
