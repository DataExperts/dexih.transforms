using System;
using System.Collections.Generic;
using System.Linq;
using dexih.functions.Query;

namespace dexih.functions
{
    public class SortedRowsDictionary : SortedDictionary<object[], object[]>
    {
        public SortedRowsDictionary(List<Sort.EDirection> sortDirections): base(new SortKeyComparer(sortDirections))
        {
            
        }

        public SortedRowsDictionary(Sort.EDirection sortDirection = Sort.EDirection.Ascending): base(new SortKeyComparer(sortDirection))
        {
            
        }

    }
    
    /// <summary>
    /// Compares the sort key fields so they are inserted into the sorted dictionary correctly.
    /// </summary>
    public class SortKeyComparer : IComparer<object[]>
    {
        private readonly List<Sort.EDirection> _sortDirections;
        private readonly Sort.EDirection _sortDirection = Sort.EDirection.Ascending;

        public SortKeyComparer(List<Sort.EDirection> sortDirections)
        {
            _sortDirections = sortDirections;
        }

        public SortKeyComparer(Sort.EDirection sortDirection = Sort.EDirection.Ascending)
        {
            _sortDirection = sortDirection;
        }
        

        public int Compare(object[] x, object[] y)
        {
            for (var i = 0; i < x.Length; i++)
            {
                if ((x[i] == null || x[i] is DBNull) && (y[i] == null || y[i] is DBNull))
                {
                    continue;
                }                
                
                var compareResult = ((IComparable)x[i]).CompareTo((IComparable)y[i]);

                if (compareResult == 0 )
                {
                    continue;
                }

                if (_sortDirections == null || _sortDirections.Count <= i)
                {
                    return _sortDirection == Sort.EDirection.Ascending ? compareResult : -compareResult;
                }

                if (_sortDirections[i] == Sort.EDirection.Ascending)
                {
                    return compareResult;
                }
                else
                {
                    // reverse the compare result if the sort order is to be descending.
                    return -compareResult;
                }
            }
            return 0;
        }
    }
}