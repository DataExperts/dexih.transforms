using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Parameter;
using dexih.transforms.Exceptions;
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
        Name = "Join",
        Description = "Join two tables by first loading the secondary table into memory.  This is fast when the secondary table is not large.",
        TransformType = ETransformType.Join
    )]
    public class TransformJoin : Transform
    {
        public TransformJoin() { }

        public TransformJoin(Transform primaryTransform, Transform joinTransform, Mappings mappings, EDuplicateStrategy joinDuplicateResolution, EJoinNotFoundStrategy joinNotFoundStrategy, TableColumn joinSortField, string referenceTableAlias)
        {
            Mappings = mappings;
            ReferenceTableAlias = referenceTableAlias;
            JoinDuplicateStrategy = joinDuplicateResolution;
            JoinNotFoundStrategy = joinNotFoundStrategy;
            JoinSortField = joinSortField;

            SetInTransform(primaryTransform, joinTransform);
        }

        private bool _firstRead = true;
        private SortedDictionary<object[], List<object[]>> _joinHashData; //stores all the reference data grouped by the join keys (used for hashjoin).

        private List<object[]> _groupData;
        private bool _writeGroup; //indicates a group is being written out
        private int _writeGroupPosition; //indicates the position in the group.
        private bool _joinReaderOpen;
        private bool _groupsOpen;
        private string _referenceTableName;
        private Table _referenceTable;

        private bool _containsJoinColumns;

        private int _nodeColumnOrdinal = -1;
        private MapJoinNode _nodeMapping;

        private int _primaryValidFromOrdinal = -1;
        private int _primaryValidToOrdinal = -1;
        private int _referenceValidFromOrdinal = -1;
        private int _referenceValidToOrdinal = -1;

        private int _validFromOrdinal = -1;
        private int _validToOrdinal = -1;

        private bool _cacheLoaded = false;


        public enum EJoinAlgorithm
        {
            Sorted = 1, Hash
        }
        public EJoinAlgorithm JoinAlgorithm { get; private set; }

        private int _primaryFieldCount;
        private int _referenceFieldCount;

        // list of mappings which are not standard join/equal mappings.
        private Mappings _additionalJoins;
        
        public override string TransformName { get; } = "Join";

        public override Dictionary<string, object> TransformProperties()
        {
            return new Dictionary<string, object>()
            {
                {"JoinAlgorithm", JoinAlgorithm.ToString()},
            };
        }

        protected override Table InitializeCacheTable(bool mapAllReferenceColumns)
        {
            Table table;
            if (Mappings == null)
            {
                table = PrimaryTransform.CacheTable.Copy();
            } else
            {
                table = Mappings.Initialize(PrimaryTransform?.CacheTable, ReferenceTransform?.CacheTable, ReferenceTransform?.ReferenceTableAlias, mapAllReferenceColumns);
            }

            if (JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
            {
                var col = table.Columns.First(c => c.DeltaType == EDeltaType.ValidFromDate);
                table.Columns.Remove(col);

                col = table.Columns.First(c => c.DeltaType == EDeltaType.ValidToDate);
                table.Columns.Remove(col);
            }

            return table;
        }

        private Task<bool> InitializeOutputFields()
        {
            if (ReferenceTransform == null)
            {
                throw new Exception("There must a join table specified.");
            }

            _referenceTableName = string.IsNullOrEmpty(ReferenceTransform.ReferenceTableAlias) ? ReferenceTransform.CacheTable.Name : ReferenceTransform.ReferenceTableAlias;
            _referenceTable = ReferenceTransform.CacheTable.Copy();

            _additionalJoins = new Mappings();
            
            foreach(var mapping in Mappings)
            {
                if(mapping is MapJoin mapJoin && mapJoin.InputColumn != null && mapJoin.JoinColumn != null && mapJoin.Compare == ECompare.IsEqual)
                {
                    continue;
                }
                if (mapping is MapFilter) continue;

                _additionalJoins.Add(mapping);
            }

            // add a filter transform when for any mappings which involve primary table only

           var primaryFilters = new Mappings();

            foreach (var mapping in Mappings)
            {
                if (mapping is MapFilter mapFilter)
                {
                    // if either filter columns reference the joinTable, then not a primary filter.
                    if (mapFilter.Column1 != null && mapFilter.Column1.ReferenceTable == _referenceTableName ||
                        mapFilter.Column2 != null && mapFilter.Column2.ReferenceTable == _referenceTableName)
                    {
                        continue;
                    }

                    primaryFilters.Add(mapFilter.Copy());
                }

                if (mapping is MapFunction mapFunction)
                {
                    var isPrimaryFilter = true;
                    foreach (var parameter in mapFunction.Parameters.Inputs)
                    {
                        if (parameter is ParameterJoinColumn || parameter is ParameterColumn parameterColumn && parameterColumn.Column?.ReferenceTable == _referenceTableName)
                        {
                            isPrimaryFilter = false;
                            break;
                        }

                        if (parameter is ParameterArray parameterArray)
                        {
                            foreach (var arrayParameter in parameterArray.Parameters)
                            {
                                if (arrayParameter is ParameterJoinColumn || arrayParameter is ParameterColumn arrayParameterColumn && arrayParameterColumn.Column?.ReferenceTable == _referenceTableName)
                                {
                                    isPrimaryFilter = false;
                                    break;
                                }

                            }
                        }

                        if (!isPrimaryFilter) break;
                    }
                    if (isPrimaryFilter)
                    {
                        primaryFilters.Add(mapFunction.Copy());
                    }
                    else
                    {
                        _additionalJoins.Add(mapping);
                    }
                }
            }

            if (primaryFilters.Any())
            {
                var preFilterTransform = new TransformFilter(PrimaryTransform, primaryFilters);
                PrimaryTransform = preFilterTransform;
            }

            // add a filter transform when for any mappings which involve reference table only
            var referenceFilters = new Mappings();

            foreach (var mapping in Mappings)
            {
                if (mapping is MapFilter mapFilter)
                {
                    // if either filter columns reference the joinTable, then not a reference filter.
                    if (mapFilter.Column1 != null && mapFilter.Column1.ReferenceTable != _referenceTableName ||
                        mapFilter.Column2 != null && mapFilter.Column2.ReferenceTable != _referenceTableName)
                    {
                        continue;
                    }

                    referenceFilters.Add(mapFilter.Copy());
                }

                if (mapping is MapFunction mapFunction)
                {
                    var isReferenceFilter = true;
                    foreach (var parameter in mapFunction.Parameters.Inputs)
                    {
                        if (parameter is ParameterColumn)
                        {
                            isReferenceFilter = false;
                            break;
                        }

                        if (parameter is ParameterArray parameterArray)
                        {
                            foreach (var arrayParameter in parameterArray.Parameters)
                            {
                                if (arrayParameter is ParameterColumn)
                                {
                                    isReferenceFilter = false;
                                    break;
                                }

                            }
                        }

                        if (!isReferenceFilter) break;
                    }
                    if (isReferenceFilter)
                    {
                        var newInputs = mapFunction.Parameters.Inputs.Select(c =>
                        {
                            if (c is ParameterJoinColumn parameterJoinColumn)
                            {
                                return new ParameterColumn(parameterJoinColumn.Name, parameterJoinColumn.Column);
                            }
                            else if (c is ParameterArray parameterArray)
                            {
                                var newArray = parameterArray.Parameters.Select(arrayParameter =>
                                {
                                    if (arrayParameter is ParameterJoinColumn parameterJoinColumn2)
                                    {
                                        return new ParameterColumn(parameterJoinColumn2.Name, parameterJoinColumn2.Column);
                                    }
                                    else
                                    {
                                        return arrayParameter;
                                    }
                                }).ToList();

                                return new ParameterArray(parameterArray.Name, parameterArray.DataType,
                                    parameterArray.Rank, newArray);
                            }
                            else
                            {
                                return c;
                            }

                        }).ToArray();

                        var newParameters = new Parameters() { Inputs = newInputs };
                        var newMapFunction = new MapFunction(mapFunction.Function, newParameters, EFunctionCaching.NoCache);
                        referenceFilters.Add(newMapFunction);
                    }
                }
            }

            if (referenceFilters.Any())
            {
                var preFilterTransform = new TransformFilter(ReferenceTransform, referenceFilters);
                ReferenceTransform = preFilterTransform;
            }

            var nodeMappings = Mappings.OfType<MapJoinNode>().ToArray();
            if (nodeMappings.Length == 1)
            {
                _nodeMapping = nodeMappings[0];
                var nodeColumn = _nodeMapping.NodeColumn;
                if (nodeColumn != null)
                {
                    _nodeColumnOrdinal = CacheTable.GetOrdinal(nodeColumn);
                }
            }

            if(JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
            {
                _primaryValidFromOrdinal = PrimaryTransform.GetOrdinal(EDeltaType.ValidFromDate);
                _primaryValidToOrdinal = PrimaryTransform.GetOrdinal(EDeltaType.ValidToDate);
                _referenceValidFromOrdinal = ReferenceTransform.GetOrdinal(EDeltaType.ValidFromDate);
                _referenceValidToOrdinal = ReferenceTransform.GetOrdinal(EDeltaType.ValidToDate);

                if(_primaryValidFromOrdinal < 0 || _primaryValidToOrdinal < 0 || _referenceValidFromOrdinal < 0 || _referenceValidToOrdinal < 0)
                {
                    throw new Exception("The join transform has a duplicate strategy of Merge Valid/To/From dates, however the primary or join tables do not each contain a columns with delta types valid from and valid to.");
                }

                _validFromOrdinal = CacheTable.GetOrdinal(EDeltaType.ValidFromDate);
                _validToOrdinal = CacheTable.GetOrdinal(EDeltaType.ValidToDate);
            }


            //if the joinSortField has been, we need to ensure the reference dataset is sorted for duplication resolution.
            if (JoinSortField != null)
            {
                if(!SortFieldsMatch(RequiredReferenceSortFields(), ReferenceTransform.SortFields))
                {
                    var sortTransform = new TransformSort(ReferenceTransform, RequiredReferenceSortFields());
                    ReferenceTransform = sortTransform;
                }
            }

            // _firstRead = true;

            _containsJoinColumns = Mappings.OfType<MapJoin>().Where(c => c.InputColumn != null && c.Compare == ECompare.IsEqual).Any();
            _primaryFieldCount = PrimaryTransform.BaseFieldCount;
            _referenceFieldCount = ReferenceTransform.BaseFieldCount;

            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return Task.FromResult(true);
        }

        public override bool RequiresSort => false;

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            SetRequestQuery(requestQuery, true);
            SelectQuery.Columns = null;
            
            if (_cacheLoaded) return true;

            await InitializeOutputFields();

            //only apply a sort if there is not already a sort applied.
            SelectQuery.Sorts = RequiredSortFields();

            var referenceQuery = new SelectQuery
            {
                Sorts = RequiredReferenceSortFields()
            };

            var returnValue = await PrimaryTransform.Open(auditKey, SelectQuery, cancellationToken);
            if (!returnValue) return false;

            GeneratedQuery = new SelectQuery()
            {
                Sorts = PrimaryTransform.SortFields,
                Filters = PrimaryTransform.Filters
            };

            returnValue = await ReferenceTransform.Open(auditKey, referenceQuery, cancellationToken);
            if (!returnValue)
            {
                return false;
            }

            var canUseSorted = true;
            // if one of the functions joins the two tables then we will need to default to HashJoin.
            foreach (var mapping in Mappings)
            {
                if (mapping is MapFunction mapFunction)
                {
                    if (mapFunction.Parameters.Inputs.OfType<ParameterColumn>().Any() &&
                        mapFunction.Parameters.Inputs.OfType<ParameterJoinColumn>().Any())
                    {
                        canUseSorted = false;
                        break;
                    }
                }
            }
            
            //check if the primary and reference transform are sorted in the join
            if ( canUseSorted &&
                SortFieldsMatch(RequiredSortFields(), PrimaryTransform.SortFields) && 
                SortFieldsMatch(RequiredReferenceSortFields(), ReferenceTransform.SortFields))
            {
                JoinAlgorithm = EJoinAlgorithm.Sorted;
            }
            else
            {
                JoinAlgorithm = EJoinAlgorithm.Hash;
                
                _joinHashData = new SortedDictionary<object[], List<object[]>>(new JoinKeyComparer());
                _joinReaderOpen = await ReferenceTransform.ReadAsync(cancellationToken);
                _groupsOpen = await ReadNextGroup();

                //load all the join data into an a reference dictionary
                while (_groupsOpen)
                {
                    _joinHashData.Add(Mappings.GetJoinReferenceKey(_groupData[0]), _groupData);
                    _groupsOpen = await ReadNextGroup();
                }

                _cacheLoaded = true;
            }

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            //this writes out duplicates of the primary reader when a duplicate match occurs on the join table
            //i.e. outer join.
            if (_writeGroup)
            {
                //create a new row and write the primary fields out
                var newRow = new object[FieldCount];
                var pos = SetPrimaryRow(newRow);

                object[] joinRow = null;
                if (JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
                {
                    joinRow = GetNextValidReference();
                    //if (joinRow != null)
                    //{
                    //    newRow[_primaryValidFromOrdinal] = joinRow[_referenceValidFromOrdinal];
                    //    newRow[_primaryValidToOrdinal] = joinRow[_referenceValidToOrdinal];
                    //}
                }
                else
                {
                    joinRow = _groupData[_writeGroupPosition];
                    _writeGroupPosition++;
                }

                if (joinRow != null)
                {
                    for (var i = 0; i < _referenceFieldCount; i++)
                    {
                        newRow[pos] = joinRow[i];
                        pos++;
                    }


                    //if last join record, then set the flag=false so the next read will read another primary row record.
                    if (_writeGroupPosition >= _groupData.Count)
                        _writeGroup = false;

                    return newRow;
                }
            }

            //read a new row from the primary table.
            while (await PrimaryTransform.ReadAsync(cancellationToken))
            {
                var newRow = await GetJoin(cancellationToken);
                if(newRow != null)
                {
                    return newRow;
                }
            }

            return null;
        }

        private async Task<object[]> GetJoin(CancellationToken cancellationToken = default)
        {
            var newRow = new object[FieldCount];
            var joinMatchFound = false;

            //if input is sorted, then run a sorted join
            if (JoinAlgorithm == EJoinAlgorithm.Sorted)
            {
                //first read get a row from the join table.
                if (_firstRead)
                {
                    //get the first two rows from the join table.
                    _joinReaderOpen = await ReferenceTransform.ReadAsync(cancellationToken);
                    var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, ReferenceTransform.CurrentRow, cancellationToken);
                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }
                    else
                    {
                        _groupsOpen = await ReadNextGroup();
                    }
                    _firstRead = false;
                }

                //loop through join table until we find a matching row.
                if (_containsJoinColumns)
                {
                    // update the primary row only.
                    var (isMatch, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }

                    while (_groupsOpen)
                    {
                        var done = false;

                        switch (Mappings.GetJoinCompareResult())
                        {
                            case var result when result < 0:
                                done = true;
                                break;
                            case var result when result > 0:
                                if (_groupsOpen)
                                {
                                    // now the join table has advanced, add the reference row.
                                    (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, ReferenceTransform.CurrentRow, cancellationToken);
                                    if (ignore)
                                    {
                                        TransformRowsIgnored += 1;
                                    }
                                    _groupsOpen = await ReadNextGroup();
                                }

                                break;
                            case 0:
                                joinMatchFound = true;
                                done = true;
                                break;
                        }

                        if (done)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    joinMatchFound = true;
                }
            }
            else //if input is not sorted, then run a hash join.
            {
                //                //first load the join table into memory
                //                if (_firstRead)
                //                {
                //                    _joinHashData = new SortedDictionary<object[], List<object[]>>(new JoinKeyComparer());
                //                    _joinReaderOpen = await ReferenceTransform.ReadAsync(cancellationToken);
                //                    _groupsOpen = await ReadNextGroup();
                //
                //                    //load all the join data into an a reference dictionary
                //                    while (_groupsOpen)
                //                    {
                //                        _joinHashData.Add(Mappings.GetJoinReferenceKey(_groupData[0]), _groupData);
                //                        _groupsOpen = await ReadNextGroup();
                //                    }
                //
                //                    _firstRead = false;
                //                }

                var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                if (ignore)
                {
                    TransformRowsIgnored += 1;
                }
                else
                {
                    var primaryKey = Mappings.GetJoinPrimaryKey();

                    if (_joinHashData.TryGetValue(primaryKey, out _groupData))
                    {
                        _groupsOpen = true;
                        joinMatchFound = true;
                    }
                }
            }

            var pos = SetPrimaryRow(newRow);

            if (joinMatchFound)
            {
                var groupData = _groupData;

                //if there are additional join functions, run them
                if (_additionalJoins.Any())
                {

                    groupData = new List<object[]>();

                    //filter out the current group based on the functions defined.
                    foreach (var row in _groupData)
                    {
                        var matchFound = true;
                        foreach (var mapping in _additionalJoins)
                        {
                            matchFound = await mapping.ProcessInputRowAsync(new FunctionVariables(), PrimaryTransform.CurrentRow, row, cancellationToken);
                            if (!matchFound)
                            {
                                break;
                            }
                        }

                        if (matchFound)
                        {
                            groupData.Add(row);
                        }
                    }

                }

                object[] joinRow;

                if(groupData.Count == 0)
                {
                    joinRow = null;
                }
                else
                {
                    switch (JoinDuplicateStrategy)
                    {
                        case EDuplicateStrategy.Abend:
                            if (groupData.Count > 1)
                            {
                                throw new DuplicateJoinKeyException($"The join transform {Name} failed as the selected columns on the join table {ReferenceTransform?.CacheTable?.Name} are not unique.  To continue when duplicates occur set the join strategy to first, last or all.", ReferenceTableAlias, Mappings.GetJoinPrimaryKey());
                            }
                            joinRow = groupData[0];
                            break;
                        case EDuplicateStrategy.First:
                            joinRow = groupData[0];
                            break;
                        case EDuplicateStrategy.Last:
                            joinRow = groupData.Last();
                            break;
                        case EDuplicateStrategy.All:
                            joinRow = groupData[0];
                            _writeGroup = groupData.Count > 1;
                            _writeGroupPosition = 1;
                            break;
                        case EDuplicateStrategy.MergeValidDates:
                            _groupData = _groupData.OrderBy(c => c[_referenceValidFromOrdinal]).ToList();
                            _writeGroup = true;
                            _writeGroupPosition = 0;
                            joinRow = GetNextValidReference();
                            break;
                        default:
                            throw new TransformException($"The join transform {Name}  failed due to an unknown join strategy {JoinDuplicateStrategy}");
                    }
                }

                if (joinRow == null)
                {
                    switch(JoinNotFoundStrategy)
                    {
                        case EJoinNotFoundStrategy.Abend:
                            throw new JoinNotFoundException($"The join transform {Name} failed as a matching row on the join table {ReferenceTransform?.CacheTable?.Name} are was not found.  To continue, set the join not found strategy to continue.", ReferenceTableAlias, Mappings.GetJoinPrimaryKey());
                        case EJoinNotFoundStrategy.Filter:
                            return null;
                    }
                }

                if (_nodeColumnOrdinal >= 0)
                {
                    var table = _referenceTable.Copy();
                    table.Data.Set(_groupData);
                    _nodeMapping.InputTransform = new ReaderMemory(table);
                    var outTransform = (Transform)_nodeMapping.GetOutputValue();
                    await outTransform.Open(AuditKey, null, cancellationToken);
                    newRow[_nodeColumnOrdinal] = outTransform;
                    _groupData = null;
                    _writeGroup = false;
                } else
                {
                    if (joinRow != null)
                    {
                        for (var i = 0; i < _referenceFieldCount; i++)
                        {
                            if (pos == _nodeColumnOrdinal) pos++;
                            newRow[pos] = joinRow[i];
                            pos++;
                        }
                    } else
                    {
                        if(JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
                        {
                            newRow[_validFromOrdinal] = PrimaryTransform[_primaryValidFromOrdinal];
                            newRow[_validToOrdinal] = PrimaryTransform[_primaryValidToOrdinal];
                        }
                    }
                }
              
            }
            else
            {
                switch (JoinNotFoundStrategy)
                {
                    case EJoinNotFoundStrategy.Abend:
                        throw new JoinNotFoundException($"The join transform {Name} failed as a matching row on the join table {ReferenceTransform?.CacheTable?.Name} are was not found.  To continue, set the join not found strategy to continue.", ReferenceTableAlias, Mappings.GetJoinPrimaryKey());
                    case EJoinNotFoundStrategy.Filter:
                        return null;
                }

                if (_nodeColumnOrdinal >= 0)
                {
                    _referenceTable.Data.Clear();
                    var table = _referenceTable.Copy();
                    _nodeMapping.InputTransform = new ReaderMemory(table);
                    var outTransform = (Transform)_nodeMapping.GetOutputValue();
                    await outTransform.Open(AuditKey, null, cancellationToken);
                    newRow[_nodeColumnOrdinal] = outTransform;
                }

                if (JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
                {
                    newRow[_validFromOrdinal] = PrimaryTransform[_primaryValidFromOrdinal];
                    newRow[_validToOrdinal] = PrimaryTransform[_primaryValidToOrdinal];
                }
            }

            return newRow;
        }

        private int SetPrimaryRow(object[] newRow)
        {
            var pos = 0;
            //create a new row and write the primary fields out
            for (var i = 0; i < _primaryFieldCount; i++)
            {
                if (pos == _nodeColumnOrdinal) pos++;

                // skip source validfrom/validto as these are merged in joined row.
                if (JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
                {
                    if (i == _primaryValidFromOrdinal || i == _primaryValidToOrdinal)
                    {
                        continue;
                    }
                }

                if (pos >= FieldCount) break;

                newRow[pos] = PrimaryTransform[i];
                pos++;
            }

            return pos;
        }

        /// <summary>
        /// Reads the next group of rows (based on join key) from the reference transform.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> ReadNextGroup()
        {
            if (!_joinReaderOpen)
            {
                return false;
            }

            _groupData = new List<object[]> {ReferenceTransform.CurrentRow};

            var previousGroup = Mappings.GetJoinReferenceKey(ReferenceTransform.CurrentRow);

            while (_joinReaderOpen)
            {

                // if no joins, then the whole reference table is the group
                if (!_containsJoinColumns)
                {
                    _joinReaderOpen = await ReferenceTransform.ReadAsync();
                    if (!_joinReaderOpen)
                        break; 

                    _groupData.Add(ReferenceTransform.CurrentRow);
                }
                else
                {
                    _joinReaderOpen = await ReferenceTransform.ReadAsync();
                    if (!_joinReaderOpen)
                    {
                        break;
                    }
                    
                    var currentGroup = Mappings.GetJoinReferenceKey(ReferenceTransform.CurrentRow);

                    var duplicateCheck = true;
                    for (var i = 0; i < previousGroup.Length; i++)
                    {
                        if (!Equals(previousGroup[i], currentGroup[i]))
                        {
                            duplicateCheck = false;
                            break;
                        }
                    }

                    if (duplicateCheck)
                    {
                        _groupData.Add(ReferenceTransform.CurrentRow);
                    }
                    else
                    {
                        break;
                    }

                    previousGroup = currentGroup;
                }
            }

            return true;
        }

        private object[] GetNextValidReference()
        {
            var validFrom = PrimaryTransform.GetDateTime(_primaryValidFromOrdinal);
            var validTo = PrimaryTransform.GetDateTime(_primaryValidToOrdinal);

            object[] joinRow = null;

            while (_writeGroupPosition < _groupData.Count)
            {
                joinRow = _groupData[_writeGroupPosition];

                var refValidFrom = (DateTime) joinRow[_referenceValidFromOrdinal];
                var refValidTo = (DateTime)joinRow[_referenceValidToOrdinal];

                // if the reference in past, move to next reference record
                if(refValidTo <= validFrom)
                {
                    _writeGroupPosition++;
                    continue;
                }

                // if the reference is in the future, then no match.
                if(refValidFrom > validTo)
                {
                    _writeGroup = false;
                    return null;
                }

                // if the reference range is outside the current range, then exit and use the primary range.
                if(refValidFrom >= validFrom && refValidTo <= validTo)
                {
                    _writeGroupPosition++;
                    break;
                }

                // copy the array, as we are changing elements.
                joinRow = joinRow.ToArray();

                if (refValidFrom < validFrom)
                {
                    joinRow[_referenceValidFromOrdinal] = validFrom;
                }

                if (refValidTo <= validTo)
                {
                    _writeGroupPosition++;
                }
                else
                {
                    // ignore where the validfrom/valid to are equal
                    if((DateTime)joinRow[_referenceValidFromOrdinal] == validTo)
                    {
                        _writeGroup = false;
                        return null;
                    }

                    joinRow[_referenceValidToOrdinal] = validTo;
                    _writeGroup = false;
                }

                break;
            }

            if (_writeGroupPosition >= _groupData.Count)
                _writeGroup = false;

            return joinRow;
        }

        private class JoinKeyComparer : IComparer<object[]>
        {
            public int Compare(object[] x, object[] y)
            {
                for (var i = 0; i < x.Length; i++)
                {
                    var compareResult = Operations.Compare(x[i], y[i]);
                    if (compareResult == 0)
                    {
                        continue;
                    }

                    return compareResult;
                }
                return 0;
            }
        }

        public override bool ResetTransform()
        {
            return true;
        }

        public override Sorts RequiredSortFields()
        {
            var fields = new Sorts();
            foreach (var joinPair in Mappings.OfType<MapJoin>().Where(c => c.InputColumn != null && c.Compare == ECompare.IsEqual))
            {
                fields.Add(new Sort {Column = joinPair.InputColumn, Direction = ESortDirection.Ascending});
            }

            if(JoinDuplicateStrategy == EDuplicateStrategy.MergeValidDates)
            {
                var column = CacheTable.GetColumn(EDeltaType.ValidFromDate);
                if (fields.Count(c => c.Column.Name == column.Name) > 0)
                {
                    throw new TransformException("The update strategy is merge valid dates, and the validfrom field is part of the joins.  Remove the validfrom from the joins.");
                }

                fields.Add(new Sort { Column = column, Direction = ESortDirection.Ascending });
            }
            
            return fields;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            var fields = new Sorts();
            foreach (var joinPair in Mappings.OfType<MapJoin>().Where(c => c.JoinColumn != null && c.Compare == ECompare.IsEqual))
            {
                fields.Add(new Sort {Column = joinPair.JoinColumn, Direction = ESortDirection.Ascending});
            }

            if (JoinSortField != null)
            {
                fields.Add(new Sort(JoinSortField));
            }

            return fields;
        }


    }


}
