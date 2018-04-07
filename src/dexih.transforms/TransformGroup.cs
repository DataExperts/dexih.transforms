using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Group",
        Description = "Group columns and apply aggregation rules to other columns.",
        TransformType = TransformAttribute.ETransformType.Group
    )]
    public class TransformGroup : Transform
    {
        public TransformGroup() {  }

        public TransformGroup(Transform inReader, List<ColumnPair> groupFields, List<TransformFunction> aggregates, List<AggregatePair> aggregatePairs, bool passThroughColumns)
        {
            GroupFields = groupFields;
            Aggregates = aggregates;
            AggregatePairs = aggregatePairs;
            PassThroughColumns = passThroughColumns;

            SetInTransform(inReader);
        }

        private bool _firstRecord;
        private bool _lastRecord;

        private object[] _groupValues;

        private List<object[]> _passThroughValues;
        private int _passThroughStartIndex;
        private int _passThroughIndex;
        private int _passThroughCount;
        private object[] _nextPassThroughRow;

        public List<ColumnPair> GroupFields
        {
            get
            {
                return ColumnPairs;
            }
            set
            {
                ColumnPairs = value;
            }
        }

        public List<TransformFunction> Aggregates
        {
            get
            {
                return Functions;
            }
            set
            {
                Functions = value;
            }
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = new Table("Group");

            var i = 0;
            if (GroupFields != null)
            {
                _groupValues = new object[GroupFields.Count];
                _firstRecord = true;

                foreach (var groupField in GroupFields)
                {
                    var column = PrimaryTransform.CacheTable.Columns[groupField.SourceColumn].Copy();
                    column.ReferenceTable = "";
                    column.Name = groupField.TargetColumn.Name;
                    CacheTable.Columns.Add(column);
                    i++;
                }
            }

            if (AggregatePairs != null)
            {
                foreach (var aggregatePair in AggregatePairs)
                {
                    var column = aggregatePair.TargetColumn.Copy();
                    CacheTable.Columns.Add(column);
                    i++;
                }
            }

            if (Aggregates != null)
            {
                foreach (var aggregate in Aggregates)
                {
                    var column = new TableColumn(aggregate.TargetColumn.Name, aggregate.ReturnType);
                    CacheTable.Columns.Add(column);
                    i++;

                    if (aggregate.Outputs != null)
                    {
                        foreach (var param in aggregate.Outputs)
                        {
                            var paramColumn = new TableColumn(param.Column.Name, param.DataType);
                            CacheTable.Columns.Add(paramColumn);
                            i++;
                        }
                    }
                }
            }

            //if passthrough is set-on load any unused columns to the output.
            if (PassThroughColumns)
            {
                _passThroughStartIndex = i;

                foreach (var column in PrimaryTransform.CacheTable.Columns)
                {
                    if (!CacheTable.Columns.ContainsMatching(column))
                        CacheTable.Columns.Add(column.Copy());
                }
            }

            if(GroupFields != null)
                CacheTable.OutputSortFields = GroupFields.Select(c => new Sort { Column = c.TargetColumn, Direction = Sort.EDirection.Ascending }).ToList();

            return true;
        }

        public override bool RequiresSort
        {
            get
            {
                if (GroupFields == null || GroupFields.Count == 0)
                    return false;
                else
                    return true;
            }
        }

        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (query == null)
                query = new SelectQuery();

            var requiredSorts = RequiredSortFields();

            if(query.Sorts != null && query.Sorts.Count > 0)
            {
                for(var i =0; i<requiredSorts.Count; i++)
                {
                    if (query.Sorts[i].Column == requiredSorts[i].Column)
                        requiredSorts[i].Direction = query.Sorts[i].Direction;
                    else
                        break;
                }
            }

            query.Sorts = requiredSorts;

            var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
            return returnValue;
        }


        public override bool ResetTransform()
        {
            if (AggregatePairs != null)
            {
                foreach (var aggregate in AggregatePairs)
                {
                    aggregate.Reset();
                }
            }

            if (Aggregates != null)
            {
                foreach (var aggregate in Aggregates)
                {
                    aggregate.Reset();
                }
            }

            _firstRecord = true;
            _lastRecord = true;

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            object[] newRow = null;

            //if there are records in the passthrough cache, then empty them out before getting new records.
            if (PassThroughColumns)
            {
                if(_firstRecord)
                {
                    _passThroughValues = new List<object[]>();
                }
                else if (_passThroughIndex > 0 && _passThroughIndex < _passThroughCount)
                {
                    newRow = _passThroughValues[_passThroughIndex];
                    _passThroughIndex++;
                    return newRow;
                }
                //if all rows have been iterated through, reset the cache and add the stored row for the next group 
                else if(_passThroughIndex >= _passThroughCount && _firstRecord == false && _lastRecord == false)
                {
                    _passThroughValues.Clear();
                    _passThroughValues.Add(_nextPassThroughRow);
                }
            }

            newRow = new object[FieldCount];

            int i;
            var groupChanged = false;
            object[] newGroupValues = null;

            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                if (_lastRecord) //return false is all record have been written.
                    return null;
            }
            else
            {
                do
                {
                    _lastRecord = false;
                    i = 0;

                    //if it's the first record then the groupvalues are being set for the first time.
                    if (_firstRecord)
                    {
                        groupChanged = false;
                        _firstRecord = false;
                        if (GroupFields != null)
                        {
                            foreach (var groupField in GroupFields)
                            {
                                // _groupValues[i] = PrimaryTransform[groupField.SourceColumn]?.ToString() ?? "";
                                _groupValues[i] = PrimaryTransform[groupField.SourceColumn];
                                i++;
                            }
                        }
                        newGroupValues = _groupValues;
                    }
                    else
                    {
                        //if not first row, then check if the group values have changed from the previous row
                        if (GroupFields != null)
                        {
                            newGroupValues = new object[GroupFields.Count];

                            foreach (var groupField in GroupFields)
                            {
                                //newGroupValues[i] = PrimaryTransform[groupField.SourceColumn]?.ToString() ?? "";
                                newGroupValues[i] = PrimaryTransform[groupField.SourceColumn];
                                if ((newGroupValues[i] == null && _groupValues != null) ||
                                    (newGroupValues[i] != null && _groupValues == null) ||
                                    !Equals(newGroupValues[i], _groupValues[i]) )
                                {
                                    groupChanged = true;
                                }
                                i++;
                            }
                        }
                    }

                    //if the group values have changed, write out the previous group values.
                    if (groupChanged)
                    {
                        i = 0;

                        if (GroupFields != null)
                        {
                            for(; i < GroupFields.Count; i++)
                            {
                                newRow[i] = _groupValues[i];
                            }
                        }

                        if (PassThroughColumns == false)
                        {
                            if (AggregatePairs != null)
                            {
                                foreach (var aggregate in AggregatePairs)
                                {
                                    newRow[i] = aggregate.GetValue();
                                    i++;
                                }
                            }

                            if (Aggregates != null)
                            {
                                foreach (var mapping in Aggregates)
                                {
                                    try
                                    {
                                        newRow[i] = mapping.ReturnValue(0);
                                        i++;

                                        if (mapping.Outputs != null)
                                        {
                                            foreach (var output in mapping.Outputs)
                                            {
                                                newRow[i] = output.Value;
                                                i++;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new TransformException($"The group transform {Name} failed on function {mapping.FunctionName}.  {ex.Message}.", ex);
                                    }
                                }
                            }
                        }
                        //for passthrough, write out the aggregated values to the cached passthrough set
                        else
                        {
                            var index = 0;
                            var startColumn = i;
                            foreach(var row in _passThroughValues)
                            {
                                i = startColumn;

                                if (AggregatePairs != null)
                                {
                                    foreach (var aggregate in AggregatePairs)
                                    {
                                        row[i] = aggregate.GetValue();
                                        i++;
                                    }
                                }

                                if (Aggregates != null)
                                {
                                    foreach (var mapping in Aggregates)
                                    {
                                        try
                                        {
                                            row[i] = mapping.ReturnValue(index);
                                            i++;

                                            if (mapping.Outputs != null)
                                            {
                                                foreach (var output in mapping.Outputs)
                                                {
                                                    row[i] = output.Value;
                                                    i++;
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            throw new TransformException($"The group transform {Name} failed, retrieving results from function {mapping.FunctionName}.  {ex.Message}.", ex);
                                        }
                                    }
                                }
                                index++;
                            }

                            //the first row of the next group has been read, so this is to store it until ready to write out.
                            _nextPassThroughRow = new object[FieldCount];
                            for (var j = 0; j< newGroupValues.Length; j++)
                                _nextPassThroughRow[j] = newGroupValues[j];

                            for (var j = _passThroughStartIndex; j < FieldCount; j++)
                                _nextPassThroughRow[j] = PrimaryTransform[GetName(j)];

                            ////set the first cached row to current
                            newRow = _passThroughValues[0];
                            _passThroughIndex = 1;
                            _passThroughCount = _passThroughValues.Count;
                        }

                        //reset the functions
                        if (AggregatePairs != null)
                        {
                            foreach (var aggregate in AggregatePairs)
                            {
                                aggregate.Reset();
                            }
                        }

                        if (Aggregates != null)
                        {
                            foreach (var mapping in Aggregates)
                            {
                                mapping.Reset();
                            }
                        }

                        //store the last groupvalues read to start the next grouping.
                        _groupValues = newGroupValues;

                    }
                    else
                    {
                        if (PassThroughColumns)
                        {
                            //create a cached current row.  this will be output when the group has changed.
                            var cacheRow = new object[newRow.Length];
                            if (_groupValues != null)
                            {
                                for (var j = 0; j < _groupValues.Length; j++)
                                    cacheRow[j] = _groupValues[j];
                            }
                            for (var j = _passThroughStartIndex; j < FieldCount; j++)
                                cacheRow[j] = PrimaryTransform[GetName(j)];
                            _passThroughValues.Add(cacheRow);
                        }

                    }

                    // update the aggregate pairs
                    if (AggregatePairs != null)
                    {
                        foreach (var aggregate in AggregatePairs)
                        {
                            aggregate.AddValue(PrimaryTransform[aggregate.SourceColumn]);
                        }
                    }

                    // update the aggregate functions
                    if (Aggregates != null)
                    {
                        foreach (var mapping in Aggregates)
                        {
                            if (mapping.Inputs != null)
                            {
                                foreach (var input in mapping.Inputs.Where(c => c.IsColumn))
                                {
                                    try
                                    {
                                        input.SetValue(PrimaryTransform[input.Column]);
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new TransformException($"The group transform {Name} failed setting an incompatible value to column {input.Column.Name}.  {ex.Message}.", ex, PrimaryTransform[input.Column]);
                                    }
                                }
                            }

                            try
                            {
                                var invokeresult = mapping.Invoke();
                            }
                            catch (FunctionIgnoreRowException)
                            {
                                //TODO: Issue that some of the aggregate values may be calculated prior to the ignorerow being set.  
                                TransformRowsIgnored++;
                                continue;
                            }
                            catch (Exception ex)
                            {
                                throw new TransformException($"The group transform {Name} failed running the function {mapping.FunctionName}.  {ex.Message}.", ex);
                            }
                        }
                    }

                    if (groupChanged)
                        break;

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                i = 0;
                if (GroupFields != null)
                {
                    for(; i < GroupFields.Count; i++)
                    {
                        newRow[i] = _groupValues[i];
                    }
                }
                if (PassThroughColumns == false)
                {
                    if (AggregatePairs != null)
                    {
                        foreach (var aggregate in AggregatePairs)
                        {
                            newRow[i] = aggregate.GetValue();
                            i++;

                            aggregate.Reset();
                        }
                    }

                    if (Aggregates != null)
                    {
                        foreach (var mapping in Aggregates)
                        {
                            try
                            {
                                newRow[i] = mapping.ReturnValue(0);
                            }
                            catch (Exception ex)
                            {
                                throw new TransformException($"The group transform {Name} failed, retrieving results from function {mapping.FunctionName}.  {ex.Message}.", ex);
                            }
                            i++;

                            if (mapping.Outputs != null)
                            {
                                foreach (var output in mapping.Outputs)
                                {
                                    newRow[i] = output.Value;
                                    i++;
                                }
                            }
                            mapping.Reset();
                        }
                    }
                }
                else
                {
                    //for passthrough, write out the aggregated values to the cached passthrough set
                    var index = 0;
                    var startColumn = i;
                    foreach (var row in _passThroughValues)
                    {
                        i = startColumn;

                        if (AggregatePairs != null)
                        {
                            foreach (var aggregate in AggregatePairs)
                            {
                                newRow[i] = aggregate.GetValue();
                                i++;
                            }
                        }

                        if (Aggregates != null)
                        {
                            foreach (var mapping in Aggregates)
                            {
                                try
                                {
                                    row[i] = mapping.ReturnValue(index);
                                }
                                catch (Exception ex)
                                {
                                    throw new TransformException($"The group transform {Name} failed retrieving results from function {mapping.FunctionName}.  {ex.Message}.", ex);
                                }

                                i++;

                                if (mapping.Outputs != null)
                                {
                                    foreach (var output in mapping.Outputs)
                                    {
                                        row[i] = output.Value;
                                        i++;
                                    }
                                }
                            }
                        }
                        index++;
                    }

                    //set the first cached row to current
                    newRow = _passThroughValues[0];
                    _passThroughIndex = 1;
                    _passThroughCount = _passThroughValues.Count;
                }

                _groupValues = newGroupValues;
                _lastRecord = true;
            }
            return newRow;

        }

        public override string Details()
        {
            return "Group: " + ( PassThroughColumns ? "All columns passed through, " : "") + "Grouped Columns:" + (GroupFields?.Count.ToString() ?? "Nill") + ", Series/Aggregate Functions:" + (Functions?.Count.ToString() ?? "Nil");
        }

        public override List<Sort> RequiredSortFields()
        {
            if (GroupFields == null)
                return null;
            else
                return GroupFields.Select(c=> new Sort { Column = c.SourceColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
