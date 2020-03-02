using System;
using System.Collections.Generic;
using Dexih.Utils.DataType;

namespace dexih.functions
{
    public class SortedRowsDictionary<T> : SortedDictionary<T[], object[]>
    {
        public SortedRowsDictionary(List<ESortDirection> sortDirections): base(new SortKeyComparer<T>(sortDirections))
        {
            
        }

        public SortedRowsDictionary(ESortDirection sortSortDirection = ESortDirection.Ascending): base(new SortKeyComparer<T>(sortSortDirection))
        {
            
        }

    }
    
    /// <summary>
    /// Compares the sort key fields so they are inserted into the sorted dictionary correctly.
    /// </summary>
    public class SortKeyComparer<T> : IComparer<T[]>
    {
        private readonly List<ESortDirection> _sortDirections;
        private readonly ESortDirection _sortSortDirection = ESortDirection.Ascending;

        public SortKeyComparer(List<ESortDirection> sortDirections)
        {
            _sortDirections = sortDirections;
        }

        public SortKeyComparer(ESortDirection sortSortDirection = ESortDirection.Ascending)
        {
            _sortSortDirection = sortSortDirection;
        }
        

        public int Compare(T[] x, T[] y)
        {
            for (var i = 0; i < x.Length; i++)
            {
                if ((x[i] == null || x[i] is DBNull) && (y[i] == null || y[i] is DBNull))
                {
                    continue;
                }

                var compareResult = Operations.Compare(x[i], y[i]); 

                if (compareResult == 0 )
                {
                    continue;
                }

                if (_sortDirections == null || _sortDirections.Count <= i)
                {
                    return _sortSortDirection == ESortDirection.Ascending ? compareResult : -compareResult;
                }

                if (_sortDirections[i] == ESortDirection.Ascending)
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