using dexih.functions;
using dexih.functions.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    /// <summary>
    /// Creates a reader that uses the filter to populate a single row defined by the filters passed to it.  
    /// This is used for datalink with a source that is a lookup.
    /// </summary>
    public class ReaderDynamic : Transform
    {
        public override ECacheMethod CacheMethod
        {
            get => ECacheMethod.PreLoadCache;
            protected set =>
                throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
        }

        #region Constructors

        public ReaderDynamic(Table dataTable, List<Sort> sortFields = null)
        {
            CacheTable = dataTable;
            CacheTable.OutputSortFields = sortFields;
            Reset();

            SortFields = sortFields;
        }

        public override List<Sort> SortFields { get; }

        public override Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            ResetTransform();
            CacheTable.Data.Clear();

            var row = new object[CacheTable.Columns.Count];
            for(var i =0; i < CacheTable.Columns.Count; i++)
            {
                var column = CacheTable.Columns[i];
                if (query?.Filters != null)
                {
                    var filter = query.Filters.SingleOrDefault(c => c.Column1 != null && c.Column1.Name == column.Name);
                    if(filter == null)
                    {
                        row[i] = column.DefaultValue;
                    }
                    else
                    {
                        row[i] = filter.Value2;
                    }
                }
                else
                {
                    row[i] = column.DefaultValue;
                }
            }

            CacheTable.Data.Add(row);

            return Task.FromResult(true);
        }

        #endregion

        public override string Details()
        {
            return "Dynamic Table " + CacheTable.Name;
        }

        public override bool ResetTransform()
        {
            CurrentRowNumber = -1;
            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            CurrentRowNumber++;
            if (CurrentRowNumber < CacheTable.Data.Count)
            {
                var row = CacheTable.Data[CurrentRowNumber];
                return Task.FromResult<object[]>(row);
            }
            return Task.FromResult<object[]>(null);
        }

        public override bool IsClosed => CurrentRowNumber >= CacheTable.Data.Count;
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            return Task.FromResult(true);
        }
    }
}
