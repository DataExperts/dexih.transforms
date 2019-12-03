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
        Name = "Sort",
        Description = "Sort a table by one or more columns.",
        TransformType = ETransformType.Sort
    )]
    public class TransformSort : Transform
    {
        public override bool CanPushAggregate => PrimaryTransform?.CanPushAggregate ?? false;

        private bool _alreadySorted;
        private bool _firstRead;
        private SortedRowsDictionary<object> _sortedDictionary;
        private SortedDictionary<object[], object[]>.KeyCollection.Enumerator _iterator;

        private Sorts _sortFields;

        public TransformSort()
        {
        }

        public TransformSort(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inTransform);
        }

        public TransformSort(Transform inTransform, Sorts sortFields)
        {
            Mappings = new Mappings();
            foreach(var sortField in sortFields)
            {
                Mappings.Add(new MapSort(sortField.Column, sortField.Direction));
            }

            SetInTransform(inTransform);
            _sortFields = sortFields;
        }

        public TransformSort(Transform inTransform, string columnName, Sort.EDirection direction = Sort.EDirection.Ascending)
        {
            var column = new TableColumn(columnName);
            Mappings = new Mappings {new MapSort(column, direction)};
            SetInTransform(inTransform);
        }

        public override bool RequiresSort => false;
        
        public override string TransformName { get; } = "Sort";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            _firstRead = true;

            if (_sortFields == null)
            {
                _sortFields = new Sorts(Mappings.OfType<MapSort>().Select(c => new Sort(c.InputColumn, c.Direction)));
            }

            AuditKey = auditKey;
            IsOpen = true;

            selectQuery = selectQuery?.CloneProperties<SelectQuery>() ?? new SelectQuery();

            selectQuery.Sorts = RequiredSortFields();

            SetSelectQuery(selectQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);

            CacheTable = PrimaryTransform.CacheTable.Copy();
            CacheTable.OutputSortFields = _sortFields;


            //check if the transform has already sorted the data, using sql or a presort.
            _alreadySorted = SortFieldsMatch(_sortFields, PrimaryTransform.SortFields);

            return returnValue;
        }


        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
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
                _sortedDictionary = new SortedRowsDictionary<object>(_sortFields.Select(c=>c.Direction).ToList());

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


        public override Sorts RequiredSortFields()
        {
            return _sortFields;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

        public override Sorts SortFields => _sortFields;
    }



}
