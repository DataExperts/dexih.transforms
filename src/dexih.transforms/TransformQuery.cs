using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    /// <summary>
    /// Applies the maximum "Rows", "Filters", and "Sort" components of a SelectQuery object.
    /// This is intended to sit on top of non-sql connections to provide simple queries.
    /// 
    /// TODO: Extend this to include groups,sorts
    /// 
    /// </summary>
    public class TransformQuery : Transform
    {
        private readonly SelectQuery _selectQuery;
        private long _rowCount;

        public TransformQuery() { }

        public TransformQuery(Transform inReader, SelectQuery selectQuery)
        {
            _selectQuery = selectQuery;
            SetInTransform(inReader);
        }

        private List<int> _fieldOrdinals;

        public override bool RequiresSort => false;

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
           
            AuditKey = auditKey;

            var pushQuery = new SelectQuery();

            if (query == null && _selectQuery != null)
            {
                pushQuery.Rows = _selectQuery.Rows;
                pushQuery.Filters = _selectQuery.Filters;
                pushQuery.Sorts = _selectQuery.Sorts;
            }
            else if(query != null)
            {
                pushQuery.Rows = _selectQuery.Rows < query.Rows && _selectQuery.Rows >= 0 ? _selectQuery.Rows : query.Rows;

                pushQuery.Filters = query.Filters;
                if (_selectQuery?.Filters != null)
                {
                    pushQuery.Filters.AddRange(_selectQuery.Filters);
                }

                if (_selectQuery?.Sorts != null && _selectQuery.Sorts.Count > 0)
                {
                    pushQuery.Sorts = _selectQuery.Sorts;
                }
                else
                {
                    pushQuery.Sorts = query.Sorts;
                }
            }
            _rowCount = 0;

            // if there are sorts, insert a sort transform.
            if (pushQuery.Sorts.Count > 0)
            {
                var sortTransform = new TransformSort(PrimaryTransform, pushQuery.Sorts);
                PrimaryTransform = sortTransform;
            }

            var returnValue = await PrimaryTransform.Open(auditKey, pushQuery, cancellationToken);
            
            if (_selectQuery?.Columns != null && _selectQuery.Columns.Count > 0)
            {
                CacheTable = new Table(PrimaryTransform.CacheTable.Name);
                _fieldOrdinals = new List<int>();

                foreach (var column in _selectQuery.Columns)
                {
                    CacheTable.Columns.Add(column.Column);
                    var ordinal = PrimaryTransform.CacheTable.GetOrdinal(column.Column.Name);
                    if (ordinal < 0)
                    {
                        throw new TransformException($"The select column {column.Column.Name} could not be found.");
                    }
                    _fieldOrdinals.Add(ordinal);
                }
            }
            else
            {
                CacheTable = PrimaryTransform.CacheTable.Copy();
                _fieldOrdinals = Enumerable.Range(0, CacheTable.Columns.Count).ToList();
            }

            CacheTable.Name = "Query";
            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return returnValue;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
                return null;

            _rowCount++;
            var showRecord = false;

            if (_selectQuery != null && (_selectQuery.Rows <= 0 || _rowCount <= _selectQuery.Rows)  && _selectQuery.Filters != null)
            {
                do //loop through the records util the filter is true
                {
                    showRecord = true;
                    var isFirst = true;

                    foreach (var filter in _selectQuery.Filters)
                    {
                        var column1Value = filter.Column1 == null
                            ? filter.Value1
                            : PrimaryTransform[filter.Column1.Name];
                        var column2Value = filter.Column2 == null
                            ? filter.Value2
                            : PrimaryTransform[filter.Column2];

                        if (isFirst)
                        {
                            showRecord = filter.Evaluate(column1Value, column2Value);
                            isFirst = false;
                        }
                        else if (filter.AndOr == Filter.EAndOr.And)
                        {
                            showRecord = showRecord && filter.Evaluate(column1Value, column2Value);
                        }
                        else
                        {
                            showRecord = showRecord || filter.Evaluate(column1Value, column2Value);
                        }
                    }

                    if (showRecord) break;

                    TransformRowsFiltered += 1;

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }
            else
            {
                showRecord = true;
            }

            object[] newRow;

            if (showRecord)
            {
                newRow = new object[_fieldOrdinals.Count];
                for(var i = 0; i < _fieldOrdinals.Count; i++)
                {
                    newRow[i] = PrimaryTransform.GetValue(_fieldOrdinals[i]);
                }
            }
            else
                newRow = null;

            return newRow;
        }

        public override bool ResetTransform()
        {
            return true;
        }

        public override string Details()
        {
            return "Query: Rows= " + _selectQuery?.Rows + ", conditions=" + _selectQuery?.Filters?.Count;
        }

        public override List<Sort> RequiredSortFields()
        {
            return _selectQuery?.Sorts;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
