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
            get => ECacheMethod.DemandCache;
            protected set =>
                throw new Exception("Cache method is always PreLoadCache in the ReaderDynamic and cannot be set.");
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

        public override Task<bool> Open(long auditKey, SelectQuery selectQuery, CancellationToken cancellationToken = default)
        {
            IsOpen = true;
            ResetTransform();
            CacheTable.Data.Clear();

            var row = new object[CacheTable.Columns.Count];
            for(var i =0; i < CacheTable.Columns.Count; i++)
            {
                var column = CacheTable.Columns[i];
                if (selectQuery?.Filters != null)
                {
                    var filter = selectQuery.Filters.SingleOrDefault(c => c.Column1 != null && c.Column1.Name == column.Name);
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

            CacheTable.AddRow(row);

            return Task.FromResult(true);
        }

        #endregion

        public override string TransformName { get; } = "Dynamic Row Creator";
        public override string TransformDetails => CacheTable?.Name ?? "Unknown";


        public override bool ResetTransform()
        {
            CurrentRowNumber = -1;
            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            CurrentRowNumber++;
            if (CurrentRowNumber < CacheTable.Data.Count)
            {
                var row = CacheTable.Data[CurrentRowNumber];
                return Task.FromResult(row);
            }
            return Task.FromResult<object[]>(null);
        }

        public override bool IsClosed => CurrentRowNumber >= CacheTable.Data.Count;
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            return Task.FromResult(true);
        }
    }
}
