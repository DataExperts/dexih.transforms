using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    [Transform(
        Name = "Lookup",
        Description = "Looks up a value in a database or external service.",
        TransformType = ETransformType.Lookup
    )]
    public class TransformLookup : Transform
    {
        private int _primaryFieldCount;
        private int _referenceFieldCount;

        private IEnumerator<object[]> _lookupCache;

        public TransformLookup() { }

        public TransformLookup(Transform primaryTransform, Transform joinTransform, Mappings mappings, EDuplicateStrategy joinDuplicateResolution, EJoinNotFoundStrategy joinNotFoundStrategy, string referenceTableAlias)
        {
            Mappings = mappings;
            ReferenceTableAlias = referenceTableAlias;
            JoinDuplicateStrategy = joinDuplicateResolution;
            JoinNotFoundStrategy = joinNotFoundStrategy;

            SetInTransform(primaryTransform, joinTransform);
        }
        
        public override string TransformName { get; } = "Lookup";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            IsOpen = true;
            
            SetRequestQuery(requestQuery, true);
            SelectQuery.Columns = null;
            
            _primaryFieldCount = PrimaryTransform.FieldCount;
            _referenceFieldCount = ReferenceTransform.FieldCount;

            ReferenceTransform.SetCacheMethod(ECacheMethod.LookupCache, 1000);
            
            var returnValue = await PrimaryTransform.Open(auditKey, SelectQuery, cancellationToken);
            
            GeneratedQuery = new SelectQuery()
            {
                Sorts = PrimaryTransform.SortFields,
                Filters = PrimaryTransform.Filters
            };
            
            return returnValue;
        }


        public override bool RequiresSort => false;

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            // if there is a previous lookup cache, then just populated that as the next row.
            while(_lookupCache != null && _lookupCache.MoveNext())
            {
                var lookup = _lookupCache.Current;
                var (validRow, ignoreRow) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, lookup, cancellationToken);
                
                if(!validRow)
                {
                    TransformRowsFiltered += 1;
                    continue;
                }

                if (ignoreRow)
                {
                    TransformRowsIgnored += 1;
                    continue;
                }

                if(JoinDuplicateStrategy == EDuplicateStrategy.Abend)
                {
                    throw new DuplicateJoinKeyException($"The join transform {Name} failed as the selected columns on the lookup {ReferenceTransform?.CacheTable?.Name} are not unique.  To continue when duplicates occur set the join strategy to first, last or all.", ReferenceTableAlias, Mappings.GetJoinPrimaryKey());
                }

                var newRow = new object[FieldCount];
                var pos1 = 0;
                for (var i = 0; i < _primaryFieldCount; i++)
                {
                    newRow[pos1] = PrimaryTransform[i];
                    pos1++;
                }

                for (var i = 0; i < _referenceFieldCount; i++)
                {
                    newRow[pos1] = lookup[i];
                    pos1++;
                }

                return newRow;
            }

            _lookupCache = null;

            //read a new row from the primary table.
            while (await PrimaryTransform.ReadAsync(cancellationToken))
            {
                var newRow = await GetLookup(cancellationToken);
                if (newRow != null)
                {
                    return newRow;
                }
            }

            return null;
        }

        public async Task<object[]> GetLookup(CancellationToken cancellationToken)
        {
            //load in the primary table values
            var newRow = new object[FieldCount];
            var pos = 0;
            for (var i = 0; i < _primaryFieldCount; i++)
            {
                newRow[pos] = PrimaryTransform[i];
                pos++;
            }

            //set the values for the lookup
            var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
            if (ignore)
            {
                TransformRowsIgnored += 1;
            }


            // create a select query with filters set to the values of the current row
            var selectQuery = new SelectQuery
            {
                Filters = new Filters(Mappings.OfType<MapJoin>().Select(c => new Filter()
                {
                    Column1 = c.JoinColumn,
                    CompareDataType = ETypeCode.String,
                    Operator = c.Compare,
                    Value2 = c.GetOutputValue()
                }))
            };

            var lookupResult = await ReferenceTransform.Lookup(selectQuery, JoinDuplicateStrategy ?? EDuplicateStrategy.Abend, cancellationToken);
            var lookupFound = false;

            if (lookupResult != null)
            {
                _lookupCache = lookupResult.GetEnumerator();

                while (_lookupCache.MoveNext())
                {
                    var lookup = _lookupCache.Current;

                    //set the values for the lookup
                    var (validRow, ignoreRow) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, lookup, cancellationToken);

                    if (!validRow)
                    {
                        TransformRowsFiltered += 1;
                        continue;
                    }

                    if (ignoreRow)
                    {
                        TransformRowsIgnored += 1;
                        continue;
                    }

                    lookupFound = true;

                    for (var i = 0; i < _referenceFieldCount; i++)
                    {
                        newRow[pos] = lookup[i];
                        pos++;
                    }

                    if (JoinDuplicateStrategy == EDuplicateStrategy.First)
                    {
                        _lookupCache = null;
                        break;
                    }

                    if (JoinDuplicateStrategy == EDuplicateStrategy.All)
                    {
                        break;
                    }
                }

            }

            if (!lookupFound)
            {
                switch (JoinNotFoundStrategy)
                {
                    case EJoinNotFoundStrategy.Abend:
                        throw new JoinNotFoundException($"The lookup transform {Name} failed as a matching row on the join table {ReferenceTransform?.CacheTable?.Name} are was not found.  To continue, set the join not found strategy to continue.", ReferenceTableAlias, Mappings.GetJoinPrimaryKey());
                    case EJoinNotFoundStrategy.Filter:
                        return null;
                }
                _lookupCache = null;
            }

            return newRow;
        }

        public override bool ResetTransform()
        {
            return true;
        }


        public override Sorts RequiredSortFields()
        {
            var fields = new Sorts();
            return fields;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }
    }
}
