using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;

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

        public TransformLookup(Transform primaryTransform, Transform joinTransform, Mappings mappings, string referenceTableAlias)
        {
            Mappings = mappings;
            //JoinPairs = joinPairs;
            ReferenceTableAlias = referenceTableAlias;

            SetInTransform(primaryTransform, joinTransform);
        }
        
        public override string TransformName { get; } = "Lookup";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            IsOpen = true;
            
            SetSelectQuery(selectQuery, true);

            _primaryFieldCount = PrimaryTransform.FieldCount;
            _referenceFieldCount = ReferenceTransform.FieldCount;

            ReferenceTransform.SetCacheMethod(ECacheMethod.LookupCache, 1000);
            
            var returnValue = await PrimaryTransform.Open(auditKey, SelectQuery, cancellationToken);
            return returnValue;
        }


        public override bool RequiresSort => false;

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            object[] newRow = null;

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

                newRow = new object[FieldCount];
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

            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                return null;
            }

            //load in the primary table values
            newRow = new object[FieldCount];
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
                Filters = Mappings.OfType<MapJoin>().Select(c => new Filter()
                {
                    Column1 = c.JoinColumn,
                    CompareDataType = ETypeCode.String,
                    Operator = ECompare.IsEqual,
                    Value2 = c.GetOutputValue()
                }).ToList()
            };

            var lookupResult = await ReferenceTransform.Lookup(selectQuery, JoinDuplicateStrategy?? EDuplicateStrategy.Abend, cancellationToken);
            if (lookupResult != null)
            {
                _lookupCache = lookupResult.GetEnumerator();

                var lookupFound = false;
                while (_lookupCache.MoveNext())
                {
                    var lookup = _lookupCache.Current;

                    //set the values for the lookup
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

                    lookupFound = true;

                    for (var i = 0; i < _referenceFieldCount; i++)
                    {
                        newRow[pos] = lookup[i];
                        pos++;
                    }
                }
                if(!lookupFound)
                {
                    _lookupCache = null;
                }
            }
            else
            {
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
