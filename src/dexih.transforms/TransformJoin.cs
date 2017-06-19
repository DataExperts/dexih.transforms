using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    public class TransformJoin : Transform
    {
        public TransformJoin() { }

        public TransformJoin(Transform primaryTransform, Transform joinTransform, List<JoinPair> joinPairs, List<Function> functions, EDuplicateStrategy joinDuplicateResolution, TableColumn joinSortField, string referenceTableAlias)
        {
            JoinPairs = joinPairs;
            Functions = functions;
            ReferenceTableAlias = referenceTableAlias;
            JoinDuplicateStrategy = joinDuplicateResolution;
            JoinSortField = joinSortField;

            SetInTransform(primaryTransform, joinTransform);
        }

        private bool _firstRead;
        private SortedDictionary<object[], List<object[]>> _joinHashData; //stores all the reference data grouped by the join keys (used for hashjoin).

        private object[] _groupFields;
        private List<object[]> _groupData;
        private List<object[]> _filterdGroupData;
        private bool _writeGroup = false; //indicates a group is being written out
        private int _writeGroupPosition; //indicates the position in the group.
        private bool _joinReaderOpen;
        private bool _groupsOpen;
        private int[] _joinKeyOrdinals;
        private int[] _sourceKeyOrdinals;
        private string _referenceTableName;

        private readonly List<Function> joinFilters = new List<Function>();


        public enum EJoinAlgorithm
        {
            Sorted, Hash
        }
        public EJoinAlgorithm JoinAlgorithm { get; protected set; }

        private int _primaryFieldCount;
        private int _referenceFieldCount;

        public override bool InitializeOutputFields()
        {
            if (ReferenceTransform == null )
                throw new Exception("There must a join table specified.");

            CacheTable = new Table("Join");

            int pos = 0;
            foreach(var column in PrimaryTransform.CacheTable.Columns)
            {
                CacheTable.Columns.Add(column.Copy());
                pos++;
            }
            foreach (var column in ReferenceTransform.CacheTable.Columns)
            {
                var newColumn = column.Copy();
                newColumn.Schema = ReferenceTableAlias;
                newColumn.IsIncrementalUpdate = false;

                // if (CacheTable.GetOrdinal(column.SchemaColumnName()) >= 0)
                // {
                //     throw new Exception("The join could not be initialized as the column " + column.SchemaColumnName() + " could not be found in the join table.");
                // }
                CacheTable.Columns.Add(newColumn);
                pos++;
            }

            List<Function> preFilters = new List<Function>();

            _referenceTableName = string.IsNullOrEmpty(ReferenceTransform.ReferenceTableAlias) ? ReferenceTransform.CacheTable.TableName : ReferenceTransform.ReferenceTableAlias;

            //seperate out the filers that only use the reference table and add them to prefilters from the ones required for joining.
            if (Functions != null)
            {
                foreach (var function in Functions)
                {
                    bool isPrefilter = true;
                    foreach (var input in function.Inputs)
                    {
                        if (input.IsColumn)
                        {
                            if (input.Column.Schema != _referenceTableName)
                            {
                                isPrefilter = false;
                                break;
                            }
                        }

                    }
                    if (isPrefilter)
                        preFilters.Add(function);
                    else
                        joinFilters.Add(function);
                }

                if (preFilters.Count > 0)
                {
                    var preFilterTransform = new TransformFilter(ReferenceTransform, preFilters);
                    ReferenceTransform = preFilterTransform;
                }
            }

            //if the joinSortField has been, we need to enssure the reference dataset is sorted for duplication resolution.
            if(JoinSortField != null)
            {
                if(!SortFieldsMatch(RequiredReferenceSortFields(), ReferenceTransform.SortFields))
                {
                    var sortTransform = new TransformSort(ReferenceTransform, RequiredReferenceSortFields());
                    ReferenceTransform = sortTransform;
                }
            }

            _firstRead = true;

            _primaryFieldCount = PrimaryTransform.FieldCount;
            _referenceFieldCount = ReferenceTransform.FieldCount;

            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;


        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query)
        {
            AuditKey = auditKey;
            if (query == null)
                query = new SelectQuery();

            //only apply a sort if there is not already a sort applied.
            if(query.Sorts == null)
                query.Sorts = RequiredSortFields();

            var returnValue = await PrimaryTransform.Open(auditKey, query);
            if (!returnValue.Success)
                return returnValue;

            var referenceQuery = new SelectQuery()
            {
                Sorts = RequiredReferenceSortFields()
            };
            returnValue = await ReferenceTransform.Open(auditKey, referenceQuery);

            //check if the primary and reference transform are sorted in the join
            if (SortFieldsMatch(RequiredSortFields(), PrimaryTransform.SortFields) && SortFieldsMatch(RequiredReferenceSortFields(), ReferenceTransform.SortFields))
                JoinAlgorithm = EJoinAlgorithm.Sorted;
            else
                JoinAlgorithm = EJoinAlgorithm.Hash;

            //store the ordinals for the joins to improve performance.
            if (JoinPairs == null)
            {
                _joinKeyOrdinals = new int[0];
                _sourceKeyOrdinals = new int[0];
            }
            else
            {
                _joinKeyOrdinals = new int[JoinPairs.Count];
                _sourceKeyOrdinals = new int[JoinPairs.Count];
                for (int i = 0; i < JoinPairs.Count; i++)
                {
                    _joinKeyOrdinals[i] = ReferenceTransform.GetOrdinal(JoinPairs[i].JoinColumn.SchemaColumnName());
                    _sourceKeyOrdinals[i] = JoinPairs[i].SourceColumn == null ? -1 : PrimaryTransform.GetOrdinal(JoinPairs[i].SourceColumn.SchemaColumnName());
                }
            }

            return returnValue;
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            object[] newRow = null;
            int pos = 0;

            //this writes out duplicates of the primary reader when a duplicate match occurrs on the join table
            //i.e. outer join.
            if (_writeGroup)
            {
                //create a new row and write the primary fields out
                newRow = new object[FieldCount];
                for (int i = 0; i < _primaryFieldCount; i++)
                {
                    newRow[pos] = PrimaryTransform[i];
                    pos++;
                }

                var joinRow = _filterdGroupData[_writeGroupPosition];

                for (int i = 0; i < _referenceFieldCount; i++)
                {
                    newRow[pos] = joinRow[i];
                    pos++;
                }

                _writeGroupPosition++;

                //if last join record, then set the flag=false so the next read will read another primary row record.
                if (_writeGroupPosition >= _filterdGroupData.Count)
                    _writeGroup = false;

                return new ReturnValue<object[]>(true, newRow);
            }

            //read a new row from the primary table.
            if (await PrimaryTransform.ReadAsync(cancellationToken)== false)
            {
                return new ReturnValue<object[]>(false, null);
            }

            //if input is sorted, then run a sortedjoin
            if (JoinAlgorithm == EJoinAlgorithm.Sorted)
            {
                //first read get a row from the join table.
                if (_firstRead)
                {
                    //get the first two rows from the join table.
                    _joinReaderOpen = await ReferenceTransform.ReadAsync(cancellationToken);
                    _groupsOpen = await ReadNextGroup();
                    _firstRead = false;
                }

                //loop through join table until we find a matching row.
                if (JoinPairs != null)
                {

                    while (_groupsOpen)
                    {
                        bool recordMatch = true;
                        for (int i = 0; i < JoinPairs.Count; i++)
                        {
                            var joinValue = JoinPairs[i].SourceColumn == null ? JoinPairs[i].JoinValue : PrimaryTransform[_sourceKeyOrdinals[i]];
                            if (!object.Equals(joinValue, _groupFields[i]))
                            {
                                recordMatch = false;
                                break;
                            }
                        }

                        if (recordMatch == false)
                        {
                            if (_groupsOpen)
                                _groupsOpen = await ReadNextGroup();
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
            else //if input is not sorted, then run a hash join.
            {
                //first read load the join table into memory
                if (_firstRead)
                {
                    _joinHashData = new SortedDictionary<object[], List<object[]>>(new JoinKeyComparer());
                    _joinReaderOpen = await ReferenceTransform.ReadAsync(cancellationToken);
                    _groupsOpen = await ReadNextGroup();

                    //load all the join data into an a dictionary
                    while (_groupsOpen)
                    {
                        _joinHashData.Add(_groupFields, _groupData);
                        _groupsOpen = await ReadNextGroup();
                    }

                    _firstRead = false;
                }

                object[] sourceKeys;

                //set the values for the lookup
                if (JoinPairs != null)
                {
                    sourceKeys = new object[JoinPairs.Count];
                    for (int i = 0; i < JoinPairs.Count; i++)
                    {
                        sourceKeys[i] = JoinPairs[i].SourceColumn == null ? JoinPairs[i].JoinValue : PrimaryTransform[_sourceKeyOrdinals[i]];
                    }
                }
                else
                    sourceKeys = new object[0];

                if (_joinHashData.Keys.Contains(sourceKeys))
                {
                    _groupData = _joinHashData[sourceKeys];
                    _groupsOpen = true;
                }
                else
                {
                    _groupData = null;
                    _groupsOpen = false;
                }
            }

            //create a new row and write the primary fields out
            newRow = new object[FieldCount];
            for (int i = 0; i < _primaryFieldCount; i++)
            {
                newRow[pos] = PrimaryTransform[i];
                pos++;
            }

            if (_groupsOpen)
            {
                //if there are additional join functions, we run them
                if (joinFilters.Count == 0)
                {
                    _filterdGroupData = _groupData;
                }
                else {
                    _filterdGroupData = new List<object[]>();

                    //filter out the current group based on the functions defined.
                    foreach (object[] row in _groupData)
                    {
                        bool matchFound = true;
                        foreach (Function condition in joinFilters)
                        {
                            foreach (Parameter input in condition.Inputs.Where(c => c.IsColumn))
                            {
                                ReturnValue result;
                                if (input.Column.Schema == _referenceTableName)
                                {
                                    result = input.SetValue(row[ReferenceTransform.GetOrdinal(input.Column.SchemaColumnName())]);
                                }
                                else
                                {
                                    result = input.SetValue(PrimaryTransform[input.Column.SchemaColumnName()]);
                                }

                                if (result.Success == false)
                                    throw new Exception("Error setting condition values: " + result.Message);
                            }

                            var invokeresult = condition.Invoke();
                            if (invokeresult.Success == false)
                                throw new Exception("Error invoking condition function: " + invokeresult.Message);

                            if ((bool)invokeresult.Value == false)
                            {
                                matchFound = false;
                                break;
                            }
                        }

                        if (matchFound)
                            _filterdGroupData.Add(row);
                    }
                }

                object[] joinRow = null;

                if (_filterdGroupData.Count > 0)
                {
                    if (_filterdGroupData.Count > 1)
                    {
                        switch (JoinDuplicateStrategy)
                        {
                            case EDuplicateStrategy.Abend:
                                throw new DuplicateJoinKeyException("The join operation could not complete as the selected columns on the join table " + ReferenceTableAlias + " are not unique.", ReferenceTableAlias, _groupFields);
                            case EDuplicateStrategy.First:
                                joinRow = _groupData[0];
                                break;
                            case EDuplicateStrategy.Last:
                                joinRow = _groupData.Last();
                                break;
                            case EDuplicateStrategy.All:
                                joinRow = _groupData[0];
                                _writeGroup = true;
                                _writeGroupPosition = 1;
                                break;
                            default:
                                throw new Exception("Join Duplicate Resolution not recognized.");
                        }
                    }
                    else
                        joinRow = _filterdGroupData[0];

                    for (int i = 0; i < _referenceFieldCount; i++)
                    {
                        newRow[pos] = joinRow[i];
                        pos++;
                    }
                }
            }

            return new ReturnValue<object[]>(true, newRow);
        }

        private async Task<bool> ReadNextGroup()
        {
            _groupData = new List<object[]>();

            if(_joinReaderOpen)
                _groupData.Add(ReferenceTransform.CurrentRow);

            while (_joinReaderOpen)
            {

                if (JoinPairs == null)
                {
                    _joinReaderOpen = await ReferenceTransform.ReadAsync();
                    _groupFields = new object[0];
                    if (!_joinReaderOpen)
                        break; 

                    _groupData.Add(ReferenceTransform.CurrentRow);
                }
                else
                {
                    _groupFields = new object[JoinPairs.Count];
                    for (int i = 0; i < JoinPairs.Count; i++)
                        _groupFields[i] = ReferenceTransform[_joinKeyOrdinals[i]];

                    _joinReaderOpen = await ReferenceTransform.ReadAsync();
                    if (!_joinReaderOpen)
                        break;

                    bool duplicateCheck = true;
                    for (int i = 0; i < JoinPairs.Count; i++)
                    {
                        if (!object.Equals(_groupFields[i], ReferenceTransform[_joinKeyOrdinals[i]]))
                        {
                            duplicateCheck = false;
                            break;
                        }
                    }

                    if (duplicateCheck)
                        _groupData.Add(ReferenceTransform.CurrentRow);
                    else
                        break;
                }
            }

            return _groupData.Count > 0;
        }

        public class JoinKeyComparer : IComparer<object[]>
        {
            public int Compare(object[] x, object[] y)
            {
                for (int i = 0; i < x.Length; i++)
                {
                    if (object.Equals(x[i], y[i])) continue;

                    bool greater = false;

                    if (x[i] is byte)
                        greater = (byte)x[i] > (byte)y[i];
                    if (x[i] is SByte)
                        greater = (SByte)x[i] > (SByte)y[i];
                    if (x[i] is UInt16)
                        greater = (UInt16)x[i] > (UInt16)y[i];
                    if (x[i] is UInt32)
                        greater = (UInt32)x[i] > (UInt32)y[i];
                    if (x[i] is UInt64)
                        greater = (UInt64)x[i] > (UInt64)y[i];
                    if (x[i] is Int16)
                        greater = (Int16)x[i] > (Int16)y[i];
                    if (x[i] is Int32)
                        greater = (Int32)x[i] > (Int32)y[i];
                    if (x[i] is Int64)
                        greater = (Int64)x[i] > (Int64)y[i];
                    if (x[i] is Decimal)
                        greater = (Decimal)x[i] > (Decimal)y[i];
                    if (x[i] is Double)
                        greater = (Double)x[i] > (Double)y[i];
                    if (x[i] is String)
                        greater = String.Compare((String)x[i], (String)y[i]) > 0;
                    if (x[i] is Boolean)
                        greater = (Boolean)x[i] == false && (Boolean)y[i];
                    if (x[i] is DateTime)
                        greater = (DateTime)x[i] > (DateTime)y[i];

                    if (greater)
                        return 1;
                    else
                        return -1;
                }
                return 0;
            }
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true); 
        }

        public override string Details()
        {
            return "Join";
        }

        public override List<Sort> RequiredSortFields()
        {
            List<Sort> fields = new List<Sort>();
            if (JoinPairs != null)
            {
                foreach (JoinPair joinPair in JoinPairs.Where(c => c.SourceColumn != null))
                    fields.Add(new Sort { Column = joinPair.SourceColumn, Direction = Sort.EDirection.Ascending });
            }
            return fields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            List<Sort> fields = new List<Sort>();
            if (JoinPairs != null)
            {
                foreach (JoinPair joinPair in JoinPairs.Where(c => c.SourceColumn != null))
                    fields.Add(new Sort { Column = joinPair.JoinColumn, Direction = Sort.EDirection.Ascending });
            }

            if (JoinSortField != null)
                fields.Add(new Sort(JoinSortField));

            return fields;
        }


    }


}
