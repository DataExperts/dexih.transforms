using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Sort",
        Description = "Sort a table by one or more columns.",
        TransformType = TransformAttribute.ETransformType.Sort
    )]
    public class TransformSort : Transform
    {
        private bool _alreadySorted;
        private bool _firstRead;
        private SortedDictionary<object[], object[]> _sortedDictionary;
        private SortedDictionary<object[], object[]>.KeyCollection.Enumerator _iterator;

        private readonly List<Sort> _sortFields;

        public TransformSort()
        {
            _sortFields = new List<Sort>();
        }

        public TransformSort(Transform inTransform, List<Sort> sortFields)
        {
            _sortFields = sortFields;
            SetInTransform(inTransform);
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();
            CacheTable.OutputSortFields = _sortFields;

            _firstRead = true;
            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;


        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (query == null)
                query = new SelectQuery();

            query.Sorts = RequiredSortFields();

            var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);

            //check if the transform has already sorted the data, using sql or a presort.
            _alreadySorted = SortFieldsMatch(_sortFields, PrimaryTransform.SortFields);

            return returnValue;
        }


        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if(_alreadySorted)
            {
                if (await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    var values = new object[PrimaryTransform.FieldCount];
                    PrimaryTransform.GetValues(values);
                    return values;
                }
                else
                {
                    return null;
                }
            }
            if (_firstRead) //load the entire record into a sorted list.
            {
                _sortedDictionary = new SortedDictionary<object[], object[]>(new SortKeyComparer(_sortFields));

                var rowcount = 0;
                while (await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    var values = new object[PrimaryTransform.FieldCount];
                    var sortFields = new object[_sortFields.Count + 1];

                    PrimaryTransform.GetValues(values);

                    for(var i = 0; i < sortFields.Length-1; i++)
                    {
                        sortFields[i] = PrimaryTransform[_sortFields[i].Column];
                    }
                    sortFields[sortFields.Length-1] = rowcount; //add row count as last key field to ensure matching rows remain in original order.

                    _sortedDictionary.Add(sortFields, values);
                    rowcount++;
                    TransformRowsSorted++;
                }
                _firstRead = false;
                if (rowcount == 0)
                    return null;

                _iterator = _sortedDictionary.Keys.GetEnumerator();
                _iterator.MoveNext();
                return _sortedDictionary[_iterator.Current];
            }

            var success = _iterator.MoveNext();
            if (success)
                return _sortedDictionary[_iterator.Current];
            else
            {
                _sortedDictionary = null; //free up memory after all rows are read.
                return null;
            }
        }

        public override bool ResetTransform()
        {
            _sortedDictionary = null;
            _firstRead = true;

            return true;
        }

        public override string Details()
        {
            if (_sortFields == null)
            {
                return "";
            }
            
            return "Sort: "+ string.Join(",", _sortFields?.Select(c=> c.Column + " " + c.Direction.ToString()).ToArray());
        }

        public override List<Sort> RequiredSortFields()
        {
            return _sortFields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

        public override List<Sort> SortFields => _sortFields;
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
            for (var i = 0; i < x.Length; i++)
            {
                var compareResult = ((IComparable)x[i]).CompareTo((IComparable)y[i]);


                //if (object.Equals(x[i], y[i])) continue;

                //var greater = false;

                if ((x[i] == null || x[i] is DBNull) && (y[i] == null || y[i] is DBNull))
                {
                    continue;
                }

                //if (x[i] == null || x[i] is DBNull)
                //    greater = false;
                //else if (y[i] == null || y[i] is DBNull)
                //    greater = true;
                //else if (x[i] is byte)
                //    greater = (byte)x[i] > (byte)y[i];
                //else if (x[i] is SByte)
                //    greater = (SByte)x[i] > (SByte)y[i];
                //else if (x[i] is UInt16)
                //    greater = (UInt16)x[i] > (UInt16)y[i];
                //else if (x[i] is UInt32)
                //    greater = (UInt32)x[i] > (UInt32)y[i];
                //else if (x[i] is UInt64)
                //    greater = (UInt64)x[i] > (UInt64)y[i];
                //else if (x[i] is Int16)
                //    greater = (Int16)x[i] > (Int16)y[i];
                //else if (x[i] is Int32)
                //    greater = (Int32)x[i] > (Int32)y[i];
                //else if (x[i] is Int64)
                //    greater = (Int64)x[i] > (Int64)y[i];
                //else if (x[i] is Decimal)
                //    greater = (Decimal)x[i] > (Decimal)y[i];
                //else if (x[i] is Double)
                //    greater = (Double)x[i] > (Double)y[i];
                //else if (x[i] is String)
                //    greater = String.CompareOrdinal((String)x[i], (String)y[i]) > 0;
                //else if (x[i] is Boolean)
                //    greater = (Boolean)x[i] && (Boolean)y[i];
                //else if (x[i] is DateTime)
                //    greater = (DateTime)x[i] > (DateTime)y[i];

                if (compareResult == 0 )
                {
                    continue;
                }

                if (SortDirections[i] == Sort.EDirection.Ascending)
                {
                    return compareResult;
                }
                else
                {
                    // reverse the compare result if the sort order is to be decending.
                    return compareResult < 1 ? 1 : 0;
                }
            }
            return 0;
        }
    }
}
