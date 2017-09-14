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
    public class ReaderMemory : Transform
    {
        public override ECacheMethod CacheMethod
        {
            get => ECacheMethod.PreLoadCache;
            protected set =>
                throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
        }

        #region Constructors

        public ReaderMemory(Table dataTable, List<Sort> sortFields = null)
        {
            CacheTable = dataTable;
            CacheTable.OutputSortFields = sortFields;
            Reset();

            SortFields = sortFields;
        }

        public override List<Sort> SortFields { get; }

        public void Add(object[] values)
        {
            CacheTable.Data.Add(values);
        }

        #endregion

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override string Details()
        {
            return "Source Table " + CacheTable.Name;
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
    }
}
