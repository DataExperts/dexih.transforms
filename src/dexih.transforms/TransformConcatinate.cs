using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    [Transform(
        Name = "Concatenate",
        Description = "Concatenate (union) the two data streams together.",
        TransformType = ETransformType.Concatenate
        )]
    public class TransformConcatenate : Transform
    {
        private Task<bool> _primaryReadTask;
        private Task<bool> _referenceReadTask;

        private bool _primaryMoreRecords;
        private bool _referenceMoreRecords;
        private List<int> _primarySortOrdinals;
        private List<int> _referenceSortOrdinals;

        private bool _sortedMerge = false;

        private readonly List<int> _primaryMappings = new List<int>();
        private readonly List<int> _referenceMappings = new List<int>();

        public TransformConcatenate() { }

        public TransformConcatenate(Transform primaryTransform, Transform concatenateTransform)
        {
            SetInTransform(primaryTransform, concatenateTransform);
        }

        private bool _firstRead;

      public override bool RequiresSort => false;

      public override string TransformName { get; } = "Concatenate Rows";

      public override Dictionary<string, object> TransformProperties()
      {
          return null;
      }

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            if (selectQuery == null)
            {
                selectQuery = new SelectQuery();
            }
            else
            {
                selectQuery = selectQuery.CloneProperties<SelectQuery>(true);
            }
            
            var primarySorts = new List<Sort>();
            var referenceSorts = new List<Sort>();
            
            //we need to translate filters and sorts to source column names before passing them through.
            if (selectQuery?.Sorts != null)
            {
                foreach (var sort in selectQuery.Sorts)
                {
                    if (sort.Column != null)
                    {
                        var column = PrimaryTransform.CacheTable[sort.Column.Name];
                        if (column != null)
                        {
                            primarySorts.Add(new Sort(column, sort.Direction));
                        }
                        
                        column = ReferenceTransform.CacheTable[sort.Column.Name];
                        if (column != null)
                        {
                            referenceSorts.Add(new Sort(column, sort.Direction));
                        }
                    }
                }
            }

            SelectQuery = selectQuery;

            var primaryQuery = new SelectQuery() {Sorts = primarySorts};
            var referenceQuery = new SelectQuery() {Sorts = referenceSorts};
            
            var returnValue = await PrimaryTransform.Open(auditKey, primaryQuery, cancellationToken);
            if (!returnValue)
                return false;

            returnValue = await ReferenceTransform.Open(auditKey, referenceQuery, cancellationToken);

            if (ReferenceTransform == null)
                throw new Exception("There must a concatenate transform specified.");

            CacheTable = new Table("Concatenated");

            var pos = 0;
            foreach (var column in PrimaryTransform.CacheTable.Columns)
            {
                CacheTable.Columns.Add(column.Copy());
                _primaryMappings.Add(pos);
                pos++;
            }

            foreach (var column in ReferenceTransform.CacheTable.Columns)
            {
                var ordinal = CacheTable.GetOrdinal(column.Name);
                if (ordinal < 0)
                {
                    CacheTable.Columns.Add(column.Copy());
                    ordinal = pos;
                    pos++;
                }
                _referenceMappings.Add(ordinal);
            }
            
            _firstRead = true;
            
            //if the primary & reference transforms are sorted, we will merge sort the items.
            if (PrimaryTransform.SortFields != null && ReferenceTransform.SortFields != null)
            {
                var newSortFields = new List<Sort>();
                _primarySortOrdinals = new List<int>();
                _referenceSortOrdinals = new List<int>();
                
                var index = 0;
                var referenceSortFields = ReferenceTransform.SortFields;
                
                foreach (var sortField in PrimaryTransform.SortFields)
                {
                    if (referenceSortFields.Count <= index)
                    {
                        break;
                    }

                    var referenceSortField = referenceSortFields[index];
                    if (sortField.Column.Name == referenceSortField.Column.Name &&
                        sortField.Direction == referenceSortField.Direction)
                    {
                        newSortFields.Add(sortField);
                        
                        _primarySortOrdinals.Add(PrimaryTransform.CacheTable.GetOrdinal(sortField.Column.Name));
                        _referenceSortOrdinals.Add(ReferenceTransform.CacheTable.GetOrdinal(sortField.Column.Name));
                    }
                    else
                    {
                        break;
                    }

                    index++;
                }
                
                if (newSortFields.Count > 0)
                {
                    _sortedMerge = true;
                    CacheTable.OutputSortFields = newSortFields;
                }
            }
            
            return returnValue;
        }
        
        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            // sorted merge will concatenate 2 sorted incoming datasets, and maintain the sort order.
            if (_sortedMerge)
            {
                if (_firstRead)
                {
                    // read one row for each reader.
                    var primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                    var referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                    await Task.WhenAll(primaryReadTask, referenceReadTask);
                    
                    if (primaryReadTask.IsFaulted)
                    {
                        throw primaryReadTask.Exception;
                    }

                    if (referenceReadTask.IsFaulted)
                    {
                        throw referenceReadTask.Exception;
                    }

                    _primaryMoreRecords = primaryReadTask.Result;
                    _referenceMoreRecords = referenceReadTask.Result;
                    
                    _firstRead = false;
                }

                if (_primaryReadTask != null)
                {
                    _primaryMoreRecords = await _primaryReadTask;
                }

                if (_referenceReadTask != null)
                {
                    _referenceMoreRecords = await _referenceReadTask;
                }

                if (!_primaryMoreRecords && !_referenceMoreRecords)
                {
                    return null;
                }

                var newRow = new object[FieldCount];
                
                // no more primary records, then just read from the reference
                if (!_primaryMoreRecords)
                {
                    var returnValue = CreateRecord(ReferenceTransform, _referenceMappings);
                    _primaryReadTask = null;
                    _referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                    return returnValue;
                } 
                // no more reference record ,just read from the primary.
                else if (!_referenceMoreRecords)
                {
                    var returnValue = CreateRecord(PrimaryTransform, _primaryMappings);
                    PrimaryTransform.GetValues(newRow);
                    _referenceReadTask = null;
                    _primaryReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                    return returnValue;
                }
                else
                {
                    //more records in both, then compare the rows and take the next in sort order.
                    
                    var usePrimary = true;
                    
                    for (var i = 0; i < _primarySortOrdinals.Count; i++)
                    {
                        var compareResult = Operations.Compare(
                            PrimaryTransform.CacheTable.Columns[_primarySortOrdinals[i]].DataType,
                            PrimaryTransform[_primarySortOrdinals[i]], ReferenceTransform[_referenceSortOrdinals[i]]);

                        if ((compareResult > 0 &&
                             SortFields[i].Direction == Sort.EDirection.Ascending) ||
                            (compareResult < 0 &&
                             SortFields[i].Direction == Sort.EDirection.Descending))
                        {
                            usePrimary = false;
                            break;
                        }
                    }

                    if (usePrimary)
                    {
                        var returnValue = CreateRecord(PrimaryTransform, _primaryMappings);
                        _primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                        return returnValue;
                    }
                    else
                    {
                        var returnValue = CreateRecord(ReferenceTransform, _referenceMappings);
                        _referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                        return returnValue;
                    }
                }
                
            } else
            {
                // if no sorting specified, concatenate will be in any order as the records arrive.
                if (_firstRead)
                {
                    _primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                    _referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                    _firstRead = false;
                }

                if (_primaryReadTask != null && _referenceReadTask != null)
                {
                    await Task.WhenAny(_primaryReadTask, _referenceReadTask);

                    if (_primaryReadTask.IsCanceled || _referenceReadTask.IsCanceled ||
                        cancellationToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException("The read record task was cancelled");
                    }

                    if (_primaryReadTask.IsFaulted)
                    {
                        throw _primaryReadTask.Exception;
                    }

                    if (_referenceReadTask.IsFaulted)
                    {
                        throw _referenceReadTask.Exception;
                    }

                    if (_primaryReadTask.IsCompleted)
                    {
                        var result = _primaryReadTask.Result;
                        if (result)
                        {
                            var returnValue = CreateRecord(PrimaryTransform, _primaryMappings);
                            _primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                            return returnValue;
                        }
                        _primaryReadTask = null;
                    }

                    if (_referenceReadTask.IsCompleted)
                    {
                        var result = _referenceReadTask.Result;
                        if (result)
                        {
                            var returnValue = CreateRecord(ReferenceTransform, _referenceMappings);
                            _referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                            return returnValue;
                        }
                        _primaryReadTask = null;
                    }
                }

                if (_primaryReadTask != null)
                {
                    var result = await _primaryReadTask;
                    if (result)
                    {
                        var returnValue = CreateRecord(PrimaryTransform, _primaryMappings);
                        _primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                        return returnValue;
                    }
                    _primaryReadTask = null;
                }

                if (_referenceReadTask != null)
                {
                    var result = await _referenceReadTask;
                    if (result)
                    {
                        var returnValue = CreateRecord(ReferenceTransform, _referenceMappings);
                        _referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                        return returnValue;
                    }
                    _referenceReadTask = null;
                }
            }

            return null;
        }

        private object[] CreateRecord(Transform transform, List<int> mappings)
        {
            var newRow = new object[CacheTable.Columns.Count];

            for(var i = 0; i< mappings.Count; i++)
            {
                newRow[mappings[i]] = transform[i];
            }

            return newRow;
        }
        

        public override bool ResetTransform()
        {
            return true;
        }



    }

}
