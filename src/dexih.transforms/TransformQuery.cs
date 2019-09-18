using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;

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
        private long _rowCount;

        private SelectQuery _selectQuery;

        public TransformQuery() { }

        public TransformQuery(Transform inReader, SelectQuery selectQuery)
        {
            _selectQuery = selectQuery;
            SetInTransform(inReader);
        }

        private List<int> _fieldOrdinals;

        public override bool RequiresSort => false;

        public override string TransformName { get; } = "Transform Query";
        
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        
        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
           
            AuditKey = auditKey;
            IsOpen = true;

            SelectQuery = new SelectQuery();

            if (selectQuery == null && _selectQuery != null)
            {
                SelectQuery.Rows = _selectQuery.Rows;
                SelectQuery.Filters = _selectQuery.Filters;
                SelectQuery.Sorts = _selectQuery.Sorts;
                SelectQuery.Groups = _selectQuery.Groups;
                SelectQuery.Columns = _selectQuery.Columns;
            }
            else if(selectQuery != null)
            {
                SelectQuery.Rows = _selectQuery.Rows < selectQuery.Rows && _selectQuery.Rows >= 0 ? _selectQuery.Rows : selectQuery.Rows;

                SelectQuery.Filters = selectQuery.Filters;
                if (_selectQuery?.Filters != null)
                {
                    SelectQuery.Filters.AddRange(_selectQuery.Filters);
                }

                if (_selectQuery?.Sorts != null && _selectQuery.Sorts.Count > 0)
                {
                    SelectQuery.Sorts = _selectQuery.Sorts;
                }
                else
                {
                    SelectQuery.Sorts = selectQuery.Sorts;
                }

                if (_selectQuery?.Groups != null && _selectQuery.Groups.Count > 0)
                {
                    SelectQuery.Groups = _selectQuery.Groups;
                }
                else
                {
                    SelectQuery.Groups = selectQuery.Groups;
                }
                
                if (_selectQuery?.Columns != null && _selectQuery.Columns.Count > 0)
                {
                    SelectQuery.Columns = _selectQuery.Columns;
                }
                else
                {
                    SelectQuery.Columns = selectQuery.Columns;
                }
            }
            
            _rowCount = 0;

            // if there are sorts, insert a sort transform.
            if (SelectQuery.Sorts.Count > 0)
            {
                var sortTransform = new TransformSort(PrimaryTransform, SelectQuery.Sorts)
                {
                    Name = "Internal Sort"
                };
                PrimaryTransform = sortTransform;
            }
            
            // if there are aggregates ,insert a group transform.
            if (SelectQuery.Columns.Any(c => c.Aggregate != null) || SelectQuery.Groups?.Count > 0)
            {
                if (SelectQuery.Columns.Any(c => c.Aggregate == null))
                {
                    throw new TransformException("The query transform failed as there was a mix of aggregate and non aggregate columns in the one query.");
                }

                var mappings = new Mappings(false);
                foreach(var group in SelectQuery.Groups)
                {
                    mappings.Add(new MapGroup(group));
                }

                foreach (var column in SelectQuery.Columns.Where(c => c.Aggregate != null))
                {
                    mappings.Add(new MapAggregate(column.Column, column.Column, column.Aggregate.Value));
                }

                var groupTransform = new TransformGroup(PrimaryTransform, mappings)
                {
                    Name = "Internal Group"
                };
                PrimaryTransform = groupTransform;
            }

            var returnValue = await PrimaryTransform.Open(auditKey, SelectQuery, cancellationToken);
            
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
                CacheTable = PrimaryTransform.CacheTable.Copy(false, true);
                _fieldOrdinals = CacheTable.Columns.Select(c => PrimaryTransform.CacheTable.GetOrdinal(c)).ToList();
            }

            CacheTable.Name = "Query";
            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return returnValue;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
                return null;

            _rowCount++;
            var showRecord = false;

            if (SelectQuery != null && (SelectQuery.Rows <= 0 || _rowCount <= SelectQuery.Rows)  && SelectQuery.Filters != null)
            {
                do //loop through the records util the filter is true
                {
                    showRecord = true;
                    var isFirst = true;

                    foreach (var filter in SelectQuery.Filters)
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


        public override Sorts RequiredSortFields()
        {
            return SelectQuery?.Sorts;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
