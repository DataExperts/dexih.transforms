using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public class TransformSort : Transform
    {
        bool _alreadySorted;
        bool _firstRead;
        SortedDictionary<object[], object[]> _sortedDictionary;
        SortedDictionary<object[], object[]>.KeyCollection.Enumerator _iterator;

        public TransformSort() { }

        public TransformSort(Transform inTransform, List<Sort> sortFields)
        {
            SortFields = sortFields;
            SetInTransform(inTransform);
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();
            CacheTable.OutputSortFields = SortFields;

            _alreadySorted = SortFieldsMatch(SortFields, PrimaryTransform.SortFields);

            _firstRead = true;
            return true;
        }

        public override bool RequiresSort => false;

        public override ReturnValue Open(List<Filter> filters = null, List<Sort> sorts = null)
        {
            var returnValue = PrimaryTransform.Open(filters, RequiredSortFields());
            return returnValue;
        }


        protected override ReturnValue<object[]> ReadRecord()
        {
            if(_alreadySorted)
            {
                if (PrimaryTransform.Read())
                {
                    object[] values = new object[PrimaryTransform.FieldCount];
                    PrimaryTransform.GetValues(values);
                    return new ReturnValue<object[]>(true, values);
                }
                else
                {
                    return new ReturnValue<object[]>(false, null);
                }
            }
            if (_firstRead) //load the entire record into a sorted list.
            {
                _sortedDictionary = new SortedDictionary<object[], object[]>(new SortKeyComparer(SortFields));

                int rowcount = 0;
                while (PrimaryTransform.Read())
                {
                    object[] values = new object[PrimaryTransform.FieldCount];
                    object[] sortFields = new object[SortFields.Count + 1];

                    PrimaryTransform.GetValues(values);

                    for(int i = 0; i < sortFields.Length-1; i++)
                    {
                        sortFields[i] = PrimaryTransform[SortFields[i].Column];
                    }
                    sortFields[sortFields.Length-1] = rowcount; //add row count as last key field to ensure match rows remain in original order.

                    _sortedDictionary.Add(sortFields, values);
                    rowcount++;
                }
                _firstRead = false;
                if (rowcount == 0)
                    return new ReturnValue<object[]>(false, null);

                _iterator = _sortedDictionary.Keys.GetEnumerator();
                _iterator.MoveNext();
                return new ReturnValue<object[]>(true, _sortedDictionary[_iterator.Current]);
            }

            var success = _iterator.MoveNext();
            if (success)
                return new ReturnValue<object[]>(true, _sortedDictionary[_iterator.Current]);
            else
            {
                _sortedDictionary = null; //free up memory after all rows are read.
                return new ReturnValue<object[]>(false, null);
            }
        }

        public override ReturnValue ResetTransform()
        {
            if (_sortedDictionary != null)
                _sortedDictionary = null;
            _firstRead = true;

            return new ReturnValue(true);
        }

        public override string Details()
        {
            return "Sort: "+ String.Join(",", SortFields?.Select(c=> c.Column + " " + c.Direction.ToString()).ToArray());
        }

        public override List<Sort> RequiredSortFields()
        {
            return SortFields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }


    /// <summary>
    /// Compares the sort key fields so they are inserted into the sorted dictionary correctly.
    /// </summary>
    public class SortKeyComparer : IComparer<object[]>
    {
        
        protected List<Sort.EDirection> SortDirections;

        public SortKeyComparer(List<Sort> sortFields)
        {
            SortDirections = sortFields.Select(c => c.Direction).ToList();

            SortDirections.Add(Sort.EDirection.Ascending);
        }

        public int Compare(object[] x, object[] y)
        {
            for (int i = 0; i < x.Length; i++)
            {
                if (object.Equals(x[i], y[i])) continue;

                bool greater = false;

                if (x[i] is byte)
                    greater = (byte)x[i] > (byte)y[i];
                if (x[i] is SByte)
                    greater = (SByte)x[i] > (SByte)y[i];
                if (x[i] is UInt16)
                    greater = (UInt16)x[i] > (UInt16)y[i];
                if (x[i] is UInt32)
                    greater = (UInt32)x[i] > (UInt32)y[i];
                if (x[i] is UInt64)
                    greater = (UInt64)x[i] > (UInt64)y[i];
                if (x[i] is Int16)
                    greater = (Int16)x[i] > (Int16)y[i];
                if (x[i] is Int32)
                    greater = (Int32)x[i] > (Int32)y[i];
                if (x[i] is Int64)
                    greater = (Int64)x[i] > (Int64)y[i];
                if (x[i] is Decimal)
                    greater = (Decimal)x[i] > (Decimal)y[i];
                if (x[i] is Double)
                    greater = (Double)x[i] > (Double)y[i];
                if (x[i] is String)
                    greater = String.Compare((String)x[i], (String)y[i]) > 0;
                if (x[i] is Boolean)
                    greater = (Boolean)x[i] == true && (Boolean)y[i] == true;
                if (x[i] is DateTime)
                    greater = (DateTime)x[i] > (DateTime)y[i];

                if (SortDirections[i] == Sort.EDirection.Ascending)
                {
                    if (greater)
                        return 1;
                    else
                        return -1;
                }
                else
                {
                    if (greater)
                        return -1;
                    else
                        return 1;
                }
            }
            return 0;
        }
    }
}
