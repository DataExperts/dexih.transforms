using dexih.functions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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
            get
            {
                return ECacheMethod.PreLoadCache;
            }
            protected set
            {
                throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
            }
        }

        List<Sort> _sortFields;

        #region Constructors
        public ReaderMemory(Table dataTable,  List<Sort> sortFields = null)
        {
            CacheTable = dataTable;
            CacheTable.OutputSortFields = sortFields;
            Reset();

            _sortFields = sortFields;
        }

        public override List<Sort> SortFields
        {
            get
            {
                return _sortFields;
            }
        }

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
            return "Source Table " + CacheTable.TableName;
        }

        public override ReturnValue ResetTransform()
        {
            CurrentRowNumber = -1;
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            return await Task.Run(() => new ReturnValue<object[]>(false, null));
            //position++;
            //if (position < CacheTable.Data.Count)
            //{
            //    var row = CacheTable.Data[position];
            //    return new ReturnValue<object[]>(true, row);
            //}
            //else
            //    return new ReturnValue<object[]>(false, null);
        }
    }
}
