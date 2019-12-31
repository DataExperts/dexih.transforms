using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;

namespace dexih.transforms
{
    [Transform(
        Name = "Filter",
        Description = "Filter incoming rows based on a set of conditions.",
        TransformType = ETransformType.Filter
    )]
    public class TransformFilter : Transform
    {
        // mappings after any already filtered data has been excluded
        private Mappings _requiredMappings;
        
        public TransformFilter() { }

        public TransformFilter(Transform inReader, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inReader);
        }
        
        public override string TransformName { get; } = "Filter";
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        
        public override bool RequiresSort => false;
       
        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            selectQuery = selectQuery?.CloneProperties<SelectQuery>() ?? new SelectQuery();

            if (selectQuery.Columns != null && selectQuery.Columns.Count > 0)
            {
                var requiredColumns = Mappings?.GetRequiredColumns(true)?.ToList();

                if (requiredColumns == null)
                {
                    selectQuery.Columns = null;
                }
                else
                {
                    foreach (var column in requiredColumns)
                    {
                        if (!selectQuery.Columns.Exists(c => c.Column.Name == column.Column.Name))
                        {
                            selectQuery.Columns.Add(column);
                        }
                    }
                }
            }

            if (selectQuery.Filters == null)
                selectQuery.Filters = new Filters();

            if (Mappings != null)
            {
                //add any of the conditions that can be translated to filters
                foreach (var condition in Mappings.OfType<MapFunction>())
                {
                    var filter = condition.GetFilterFromFunction();
                    if (filter != null)
                    {
                        filter.AndOr = EAndOr.And;
                        selectQuery.Filters.Add(filter);
                    }
                }

                foreach (var filterPair in Mappings.OfType<MapFilter>())
                {
                    if (filterPair.Column2 == null)
                    {
                        var filter = new Filter(filterPair.Column1, filterPair.Operator, filterPair.Value2);
                        selectQuery.Filters.Add(filter);
                    }
                    else
                    {
                        var filter = new Filter(filterPair.Column1, filterPair.Operator, filterPair.Column2);
                        selectQuery.Filters.Add(filter);
                    }
                }
            }

            SetSelectQuery(selectQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);

            // the generated query will be the same, but with the filters added.
            GeneratedQuery = PrimaryTransform.GeneratedQuery.CloneProperties<SelectQuery>(true) ?? new SelectQuery();
            if (GeneratedQuery.IsGroup())
            {
                foreach (var filter in selectQuery.Filters)
                {
                    GeneratedQuery.GroupFilters.Add(filter);
                }
            }
            else
            {
                foreach (var filter in selectQuery.Filters)
                {
                    GeneratedQuery.Filters.Add(filter);
                }
            }

            // required mapping are any ones not already completed.
            if (Mappings != null)
            {
                _requiredMappings = Mappings.NonQueryMappings(PrimaryTransform.GeneratedQuery);
                _requiredMappings.Initialize(PrimaryTransform.CacheTable);
            }

            // the generated query will be the child query, plus any additional filters.
            GeneratedQuery = PrimaryTransform.GeneratedQuery.CloneProperties<SelectQuery>() ?? new SelectQuery();
            foreach (var filter in selectQuery.Filters)
            {
                GeneratedQuery.Filters.Add(filter);
            }
            
            CacheTable = PrimaryTransform.CacheTable;

            return returnValue;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (!await PrimaryTransform.ReadAsync(cancellationToken))
            {
                return null;
            }

            var showRecord = true;

            if (_requiredMappings != null && ( _requiredMappings.OfType<MapFunction>().Any() || _requiredMappings.OfType<MapFilter>().Any()))
            {
                do //loop through the records util the filter is true
                {
                    bool ignoreRow;
                    (showRecord, ignoreRow) = await _requiredMappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);

                    if (!showRecord)
                    {
                        TransformRowsFiltered += 1;
                    }
                    else if (ignoreRow)
                    {
                        TransformRowsIgnored += 1;
                    }
                    else
                    {
                        break;
                    }

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            object[] newRow;

            if (showRecord)
            {
                newRow = PrimaryTransform.CurrentRow;
            }
            else
            {
                newRow = null;
            }

            return newRow;
        }

        public override bool ResetTransform()
        {
            return true;
        }



        public override Sorts RequiredSortFields()
        {
            return null;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
