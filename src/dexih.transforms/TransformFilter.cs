using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
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

        public TransformFilter(Transform inReader, Filters filters)
        {
            Mappings = new Mappings();
            foreach(var filter in filters)
            {
                if (filter.Column1 != null && filter.Column2 != null)
                {
                    Mappings.Add(new MapFilter(filter.Column1, filter.Column2, filter.Operator));
                }
                else if(filter.Column1 != null)
                {
                    Mappings.Add(new MapFilter(filter.Column1, filter.Value2, filter.Operator));
                }
            }

            SetInTransform(inReader);
        }
        
        public override string TransformName { get; } = "Filter";
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        
        public override bool RequiresSort => false;
       
        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            requestQuery = requestQuery?.CloneProperties() ?? new SelectQuery();
            
            if (requestQuery.Filters == null)
                requestQuery.Filters = new Filters();

            if (Mappings != null)
            {
                //add any of the conditions that can be translated to filters
                foreach (var condition in Mappings.OfType<MapFunction>())
                {
                    var filter = condition.GetFilterFromFunction();
                    if (filter != null)
                    {
                        filter.AndOr = EAndOr.And;
                        requestQuery.Filters.Add(filter);
                    }
                }

                foreach (var filterPair in Mappings.OfType<MapFilter>())
                {
                    if (filterPair.Column1 == null && filterPair.Column2 == null)
                    {
                        throw new TransformException($"The filter {Name} failed as a filter must contain at least one column.");
                    }

                    var filter = new Filter()
                    {
                        Column1 = filterPair.Column1,
                        Value1 = filterPair.Column1 == null ? filterPair.Value1 : null,
                        Column2 = filterPair.Column2,
                        Value2 = filterPair.Column2 == null ? filterPair.Value2 : null,
                        Operator = filterPair.Operator,
                        CompareDataType = filterPair.Column1?.DataType ?? filterPair.Column2.DataType
                    };
                    requestQuery.Filters.Add(filter);
                    
                    // if (filterPair.Column2 == null)
                    // {
                    //     var filter = new Filter(filterPair.Column1, filterPair.Operator, filterPair.Value2);
                    //     requestQuery.Filters.Add(filter);
                    // }
                    // else
                    // {
                    //     var filter = new Filter(filterPair.Column1, filterPair.Operator, filterPair.Column2);
                    //     requestQuery.Filters.Add(filter);
                    // }
                }
            }

            SetRequestQuery(requestQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, requestQuery, cancellationToken);

            GeneratedQuery = GetGeneratedQuery(requestQuery);
            
            // required mapping are any ones not already completed.
            if (Mappings != null)
            {
                _requiredMappings = Mappings.NonQueryMappings(PrimaryTransform.GeneratedQuery);
                _requiredMappings.Initialize(PrimaryTransform.CacheTable);
            }
            
            CacheTable = PrimaryTransform.CacheTable;

            return returnValue;
        }

        protected override SelectQuery GetGeneratedQuery(SelectQuery requestQuery)
        {
            if (requestQuery == null) return null;

            var generatedQuery = PrimaryTransform.GeneratedQuery.CloneProperties() ?? new SelectQuery();
            
            if (generatedQuery.IsGroup())
            {
                foreach (var filter in requestQuery.Filters)
                {
                    generatedQuery.GroupFilters.Add(filter);
                }
            }
            else
            {
                foreach (var filter in requestQuery.Filters)
                {
                    generatedQuery.Filters.Add(filter);
                }
            }
            
            return generatedQuery;
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
