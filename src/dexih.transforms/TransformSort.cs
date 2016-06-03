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
        bool _firstRead;
        SortedDictionary<object[], object[]> _sortedDictionary;
        SortedDictionary<object[], object[]>.KeyCollection.Enumerator _iterator;

        public TransformSort() { }

        public TransformSort(Transform inTransform, List<Sort> sortFields)
        {
            SortFields = sortFields;
            SetInTransform(inTransform);
        }

        public override bool Initialize()
        {
            Fields = Reader.Fields;

            _firstRead = true;
            return true;
        }

        public bool SetSortFields(List<Sort> sortFields)
        {
            SortFields = sortFields;
            return true;
        }

        public override int FieldCount => Fields.Count();

        /// <summary>
        /// checks if sort can execute against the database query.
        /// </summary>
        public override bool CanRunQueries => Reader.CanRunQueries;

        public override bool PrefersSort => true;
        public override bool RequiresSort => false;

        public override string GetName(int i)
        {
            return Reader.GetName(i);
        }

        public override int GetOrdinal(string columnName)
        {
            return Reader.GetOrdinal(columnName);
        }

        //public override DataTable GetSchemaTable()
        //{
        //    return _sorted.GetSchemaTable();
        //}

        protected override bool ReadRecord()
        {
            if (_firstRead) //load the entire record into a sorted list.
            {
                _sortedDictionary = new SortedDictionary<object[], object[]>(new SortKeyComparer(SortFields));

                int rowcount = 0;
                while (Reader.Read())
                {
                    object[] values = new object[Reader.FieldCount];
                    object[] sortFields = new object[SortFields.Count + 1];

                    Reader.GetValues(values);

                    for(int i = 0; i < sortFields.Length-1; i++)
                    {
                        sortFields[i] = Reader[SortFields[i].Column];
                    }
                    sortFields[sortFields.Length-1] = rowcount; //add row count as last key field to ensure match rows remain in original order.

                    _sortedDictionary.Add(sortFields, values);
                    rowcount++;
                }
                _firstRead = false;
                if (rowcount == 0)
                    return false;

                _iterator = _sortedDictionary.Keys.GetEnumerator();
                _iterator.MoveNext();

                return true;
            }

            return _iterator.MoveNext();
        }

        public override bool ResetValues()
        {
            return true;
        }

        public override object GetValue(int i)
        {
            return _sortedDictionary[_iterator.Current][i];
        }
        public override int GetValues(object[] values)
        {
            _sortedDictionary[_iterator.Current].CopyTo(values, 0);
            return FieldCount;
        }

        public override string Details()
        {
            return "Sort";
        }

        public override List<Sort> RequiredSortFields()
        {
            return SortFields;
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

        //sort will return rows sorted by the sortfields.
        public override List<Sort> OutputSortFields()
        {
            return SortFields;
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
                    greater = (Boolean)x[i] == false && (Boolean)y[i] == true;
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
