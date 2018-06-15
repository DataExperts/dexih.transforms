using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;
using dexih.transforms.Transforms;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    [Transform(
        Name = "Lookup",
        Description = "Looks up a value in a database or external service.",
        TransformType = TransformAttribute.ETransformType.Lookup
    )]
    public class TransformLookup : Transform
    {
        private int _primaryFieldCount;
        private int _referenceFieldCount;

        private string _referenceTableName;

        private IEnumerator<object[]> _lookupCache;

        public TransformLookup() { }

        public TransformLookup(Transform primaryTransform, Transform joinTransform, List<JoinPair> joinPairs, string referenceTableAlias)
        {
            JoinPairs = joinPairs;
            ReferenceTableAlias = referenceTableAlias;

            SetInTransform(primaryTransform, joinTransform);
        }

        public override bool InitializeOutputFields()
        {
            if (ReferenceTransform == null)
                throw new Exception("There must be a lookup table specified");

            CacheTable = new Table("Lookup");

            var pos = 0;
            foreach (var column in PrimaryTransform.CacheTable.Columns)
            {
                CacheTable.Columns.Add(column.Copy());
                pos++;
            }
            foreach (var column in ReferenceTransform.CacheTable.Columns)
            {
                var newColumn = column.Copy();
                newColumn.ReferenceTable = ReferenceTableAlias;
                newColumn.IsIncrementalUpdate = false;

                //if a column of the same name exists, append a 1 to the name
                //if (CacheTable.Columns.SingleOrDefault(c => c.Name == column.TableColumnName()) != null)
                //{
                //    throw new Exception("The lookup could not be initialized as the column " + column.TableColumnName() + " is ambiguous.");
                //}
                CacheTable.Columns.Add(newColumn);
                pos++;
            }

            _referenceTableName = string.IsNullOrEmpty(ReferenceTransform.ReferenceTableAlias) ? ReferenceTransform.CacheTable.Name : ReferenceTransform.ReferenceTableAlias;

            _primaryFieldCount = PrimaryTransform.FieldCount;
            _referenceFieldCount = ReferenceTransform.FieldCount;

            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }


        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;


        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            object[] newRow = null;

            // if there is a previous lookup cache, then just populated that as the next row.
            if(_lookupCache != null && _lookupCache.MoveNext())
            {
                newRow = new object[FieldCount];
                var pos1 = 0;
                for (var i = 0; i < _primaryFieldCount; i++)
                {
                    newRow[pos1] = PrimaryTransform[i];
                    pos1++;
                }

                var lookup = _lookupCache.Current;
                for (var i = 0; i < _referenceFieldCount; i++)
                {
                    newRow[pos1] = lookup[i];
                    pos1++;
                }

                return newRow;
            }
            else
            {
                _lookupCache = null;
            }

            if (await PrimaryTransform.ReadAsync(cancellationToken)== false)
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
            var filters = new List<Filter>();
            for (var i = 0; i < JoinPairs.Count; i++)
            {
                var value = JoinPairs[i].SourceColumn == null ? JoinPairs[i].JoinValue : PrimaryTransform[JoinPairs[i].SourceColumn];

                filters.Add(new Filter
                {
                    Column1 = JoinPairs[i].JoinColumn,
                    CompareDataType = ETypeCode.String,
                    Operator = Filter.ECompare.IsEqual,
                    Value2 = value
                });
            }

            var lookupResult = await ReferenceTransform.LookupRow(filters, JoinDuplicateStrategy?? EDuplicateStrategy.Abend, cancellationToken);
            if (lookupResult != null)
            {
                _lookupCache = lookupResult.GetEnumerator();

                if (_lookupCache.MoveNext())
                {
                    var lookup = _lookupCache.Current;
                    for (var i = 0; i < _referenceFieldCount; i++)
                    {
                        newRow[pos] = lookup[i];
                        pos++;
                    }
                }
                else
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

        public override string Details()
        {
            return "Lookup Service";
        }

        public override List<Sort> RequiredSortFields()
        {
            var fields = new List<Sort>();
            return fields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }
    }
}
