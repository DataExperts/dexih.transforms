using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;

namespace dexih.transforms
{
    /// <summary>
    /// Applies the maximum "Rows" and "Filters" functions of a SelectQuery object.
    /// This is intended to sit on top of non-sql connections to provide query type of functions.
    /// TODO: Extend this to include groups,sorts
    /// 
    /// </summary>
    public class TransformQuery : Transform
    {
        private SelectQuery _selectQuery;
        private long _rowCount;

        public TransformQuery() { }

        public TransformQuery(Transform inReader, SelectQuery selectQuery)
        {
            _selectQuery = selectQuery;
            SetInTransform(inReader);
        }

        public List<Function> Conditions
        {
            get
            {
                return Functions;
            }
            set
            {
                Functions = value;
            }
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();
            CacheTable.Name = "Query";
            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;

            if(query != null)
            {
                // if a selectquery is included in the open, then merge the two.
                if(_selectQuery == null)
                {
                    _selectQuery = query;
                }
                else
                {
                    if(query.Rows > 0 && _selectQuery.Rows > 0 && query.Rows < _selectQuery.Rows)
                    {
                        _selectQuery.Rows = query.Rows;
                    }
                    if(query.Filters != null)
                    {
                        if(_selectQuery.Filters == null)
                        {
                            _selectQuery.Filters = query.Filters;
                        }
                        else
                        {
                            _selectQuery.Filters.Concat(query.Filters);
                        }

                    }
                }
            }
            _rowCount = 0;

            var returnValue = await PrimaryTransform.Open(auditKey, query, cancelToken);
            return returnValue;
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            if (await PrimaryTransform.ReadAsync(cancellationToken)== false)
                return new ReturnValue<object[]>(false, null);

            _rowCount++;
            bool showRecord = false;

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

            object[] newRow;

            if (showRecord)
            {
                newRow = new object[FieldCount];
                PrimaryTransform.GetValues(newRow);
            }
            else
                newRow = null;

            return new ReturnValue<object[]>(showRecord, newRow);
        }



        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true); // nothing to reset.
        }

        public override string Details()
        {
            return "Query: Rows= " + _selectQuery?.Rows + ", conditions=" + _selectQuery?.Filters?.Count;
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
