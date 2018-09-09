using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading;
using dexih.functions;
using dexih.functions.Mappings;
using dexih.functions.Query;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Sort",
        Description = "Sort a table by one or more columns.",
        TransformType = TransformAttribute.ETransformType.Sort
    )]
    public class TransformSort : Transform
    {
        private bool _alreadySorted;
        private bool _firstRead;
        private SortedRowsDictionary _sortedDictionary;
        private SortedDictionary<object[], object[]>.KeyCollection.Enumerator _iterator;

        private readonly List<Sort> _sortFields;

        public TransformSort()
        {
            _sortFields = new List<Sort>();
        }

        public TransformSort(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inTransform);
            
            _sortFields = Mappings.OfType<MapSort>().Select(c => new Sort(c.InputColumn, c.Direction)).ToList();
        }

        public TransformSort(Transform inTransform, List<Sort> sortFields)
        {
            SetInTransform(inTransform);
            _sortFields = sortFields;
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();
            CacheTable.OutputSortFields = _sortFields;

            _firstRead = true;
            return true;
        }

        public override bool RequiresSort => false;

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (query == null)
                query = new SelectQuery();

            query.Sorts = RequiredSortFields();

            var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);

            //check if the transform has already sorted the data, using sql or a presort.
            _alreadySorted = SortFieldsMatch(_sortFields, PrimaryTransform.SortFields);

            return returnValue;
        }


        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if(_alreadySorted)
            {
                if (await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    var values = new object[PrimaryTransform.FieldCount];
                    PrimaryTransform.GetValues(values);
                    return values;
                }
                else
                {
                    return null;
                }
            }
            if (_firstRead) //load the entire record into a sorted list.
            {
                _sortedDictionary = new SortedRowsDictionary(_sortFields.Select(c=>c.Direction).ToList());

                var rowcount = 0;
                while (await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    var values = new object[PrimaryTransform.FieldCount];
                    var sortFields = new object[_sortFields.Count + 1];

                    PrimaryTransform.GetValues(values);

                    for(var i = 0; i < sortFields.Length-1; i++)
                    {
                        sortFields[i] = PrimaryTransform[_sortFields[i].Column];
                    }
                    sortFields[sortFields.Length-1] = rowcount; //add row count as last key field to ensure matching rows remain in original order.

                    _sortedDictionary.Add(sortFields, values);
                    rowcount++;
                    TransformRowsSorted++;
                }
                _firstRead = false;
                if (rowcount == 0)
                    return null;

                _iterator = _sortedDictionary.Keys.GetEnumerator();
                _iterator.MoveNext();
                return _sortedDictionary[_iterator.Current];
            }

            var success = _iterator.MoveNext();
            if (success)
                return _sortedDictionary[_iterator.Current];
            else
            {
                _sortedDictionary = null; //free up memory after all rows are read.
                return null;
            }
        }

        public override bool ResetTransform()
        {
            _sortedDictionary = null;
            _firstRead = true;

            return true;
        }

        public override string Details()
        {
            if (_sortFields == null)
            {
                return "";
            }
            
            return "Sort: "+ string.Join(",", _sortFields?.Select(c=> c.Column + " " + c.Direction.ToString()).ToArray());
        }

        public override List<Sort> RequiredSortFields()
        {
            return _sortFields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

        public override List<Sort> SortFields => _sortFields;
    }



}
