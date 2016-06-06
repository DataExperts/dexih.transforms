using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace dexih.transforms
{
    /// <summary>
    /// A source transform that uses a prepopulated Table as an input.
    /// </summary>
    public class SourceTable : Transform
    {
        int position;
        object[] currentRecord;
        int recordCount;

        public override ECacheMethod CacheMethod
        {
            get
            {
                return ECacheMethod.PreLoadCache;
            }
            set
            {
                throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
            }
        }

        public override bool CanRunQueries => false;

        #region Constructors
        public SourceTable(Table dataTable,  List<Sort> sortFields = null)
        {
            CachedTable = dataTable;
            CachedTable.OutputSortFields = sortFields;
            ResetValues();
        }

        public void Add(object[] values)
        {
            CachedTable.Data.Add(values);
        }

        #endregion

       
        public override bool Initialize()
        {
            throw new NotImplementedException();
        }

        public override string Details()
        {
            return "Source Table " + CachedTable.TableName;
        }

        public override bool ResetValues()
        {
            //_iterator = DataTable.Data.GetEnumerator();
            recordCount = CachedTable.Data.Count();
            position = -1;
            return true;
        }

        public override bool Read()
        {
            //starts  a timer that can be used to measure downstream transform and database performance.
            TransformTimer.Start();
            bool returnValue = ReadRecord();
            if (returnValue) RecordCount++;
            TransformTimer.Stop();

            return returnValue;
        }

        protected override bool ReadRecord()
        {
            position++;
            if (position < recordCount)
            {
                CurrentRow = CachedTable.Data[position];
                return true;
            }
            else
                return false;
        }



    }
}
