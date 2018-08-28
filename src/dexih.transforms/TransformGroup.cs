using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Mappings;
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

        public TransformGroup(Transform inReader, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inReader);
        }

        private bool _firstRecord;
        private bool _lastRecord;

        private object[] _groupValues;

        private Queue<object[]> _passThroughValues;
//        private List<object[]> _passThroughValues;
//        private int _passThroughStartIndex;
//        private int _passThroughIndex;
//        private int _passThroughCount;
//        private object[] _nextPassThroughRow;

//        private MapGroup[] _groupFields;
//        private MapAggregate[] _aggregates;
//        private MapFunction[] _aggregateFunctions;

//        public List<ColumnPair> GroupFields
//        {
//            get
//            {
//                return ColumnPairs;
//            }
//            set
//            {
//                ColumnPairs = value;
//            }
//        }
//
//        public List<TransformFunction> _aggregateFunctions
//        {
//            get
//            {
//                return Functions;
//            }
//            set
//            {
//                Functions = value;
//            }
//        }

        public override bool InitializeOutputFields()
        {
            CacheTable = Mappings.Initialize(PrimaryTransform.CacheTable);
//            _groupFields = Mappings.OfType<MapGroup>().ToArray();
//            _aggregates = Mappings.OfType<MapAggregate>().ToArray();
//            _aggregateFunctions = Mappings.OfType<MapFunction>().ToArray();
//            _groupValues = new object[_groupFields.Length];
            
//            CacheTable = new Table("Group");

//            var i = 0;
//            if (GroupFields != null)
//            {
//                _groupValues = new object[GroupFields.Count];
//                _firstRecord = true;
//
//                foreach (var groupField in GroupFields)
//                {
//                    var column = PrimaryTransform.CacheTable.Columns[groupField.SourceColumn].Copy();
//                    column.ReferenceTable = "";
//                    column.Name = groupField.TargetColumn.Name;
//                    CacheTable.Columns.Add(column);
//                    i++;
//                }
//            }
//
//            if (_aggregates != null)
//            {
//                foreach (var aggregatePair in _aggregates)
//                {
//                    var column = aggregatePair.TargetColumn.Copy();
//                    CacheTable.Columns.Add(column);
//                    i++;
//                }
//            }
//
//            if (_aggregateFunctions != null)
//            {
//                foreach (var aggregate in _aggregateFunctions)
//                {
//                    if (aggregate.TargetColumn != null)
//                    {
//                        var column = new TableColumn(aggregate.TargetColumn.Name, aggregate.ReturnType);
//                        CacheTable.Columns.Add(column);
//                        i++;
//                    }
//
//                    if (aggregate.Outputs != null)
//                    {
//                        foreach (var param in aggregate.Outputs)
//                        {
//                            var paramColumn = new TableColumn(param.Column.Name, param.DataType);
//                            CacheTable.Columns.Add(paramColumn);
//                            i++;
//                        }
//                    }
//                }
//            }
//
//            //if passthrough is set-on load any unused columns to the output.
//            if (PassThroughColumns)
//            {
//                _passThroughStartIndex = i;
//
//                foreach (var column in PrimaryTransform.CacheTable.Columns)
//                {
//                    if (!CacheTable.Columns.ContainsMatching(column))
//                        CacheTable.Columns.Add(column.Copy());
//                }
//            }
//
//            if(GroupFields != null)
//                CacheTable.OutputSortFields = GroupFields.Select(c => new Sort { Column = c.TargetColumn, Direction = Sort.EDirection.Ascending }).ToList();

            return true;
        }

        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (query == null)
            {
                query = new SelectQuery();
            }

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

            var returnValue = PrimaryTransform.Open(auditKey, query, cancellationToken);
            return returnValue;
        }


        public override bool ResetTransform()
        {
            Mappings.Reset();
            
//            if (_aggregates != null)
//            {
//                foreach (var aggregate in _aggregates)
//                {
//                    aggregate.Reset();
//                }
//            }
//
//            if (_aggregateFunctions != null)
//            {
//                foreach (var aggregate in _aggregateFunctions)
//                {
//                    aggregate.Reset();
//                }
//            }

            _firstRecord = true;
            _lastRecord = true;

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            object[] outputRow ;

            //if there are records in the passthrough cache, then empty them out before getting new records.
            if (Mappings.PassThroughColumns)
            {
                if(_firstRecord)
                {
                    _passThroughValues = new Queue<object[]>();
                }
                else if( _passThroughValues.Count > 0)
                {
                    outputRow = _passThroughValues.Dequeue();
                    return outputRow;
                }
                //if all rows have been iterated through, reset the cache and add the stored row for the next group 
                else if(_firstRecord == false && _lastRecord == false)
                {
                    //_passThroughValues.Clear();
                    //_passThroughValues.Add(_nextPassThroughRow);
                    
                    //reset the aggregate functions
                    Mappings.Reset();
                    
                    //populate the parameters with the current row.
                    Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                }
            }
           

            outputRow = new object[FieldCount];

            var groupChanged = false;
            object[] newGroupValues = null;

            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                if (_lastRecord) //return false is all record have been written.
                {
                    return null;
                }
            }
            else
            {
                do
                {
                    _lastRecord = false;
                    

                    // get group values without modifying mapping values
                    newGroupValues = Mappings.GetGroupValues(PrimaryTransform.CurrentRow);

                    //if it's the first record then the groupvalues are being set for the first time.
                    if (_firstRecord)
                    {
                        groupChanged = false;
                        _firstRecord = false;
                        _groupValues = newGroupValues;
//                        foreach (var groupField in _groupFields)
//                        {
//                            // _groupValues[i] = PrimaryTransform[groupField.SourceColumn]?.ToString() ?? "";
//                            // _groupValues[i] = PrimaryTransform[groupField.SourceColumn];
//                            _groupValues[i] = groupField.GetInputValue();
//                            i++;
//                        }
//                        newGroupValues = _groupValues;
                    }
                    else
                    {
                        //if not first row, then check if the group values have changed from the previous row
                        for (var i = 0; i < newGroupValues.Length; i++)
                        {
                            if (newGroupValues[i] == null && _groupValues != null ||
                                (newGroupValues[i] != null && _groupValues == null) ||
                                !Equals(newGroupValues[i], _groupValues[i]) )
                            {
                                groupChanged = true;
                                break;
                            }
                        }
                        
                        //if not first row, then check if the group values have changed from the previous row
                        //newGroupValues = new object[_groupFields.Length];

//                        foreach (var groupField in _groupFields)
//                        {
//                            //newGroupValues[i] = PrimaryTransform[groupField.SourceColumn]?.ToString() ?? "";
//                            //newGroupValues[i] = PrimaryTransform[groupField.SourceColumn];
//                            newGroupValues[i] = groupField.GetInputValue();
//                            
//                            if ((newGroupValues[i] == null && _groupValues != null) ||
//                                (newGroupValues[i] != null && _groupValues == null) ||
//                                !Equals(newGroupValues[i], _groupValues[i]) )
//                            {
//                                groupChanged = true;
//                            }
//                            i++;
//                        }
                    }

                    //if the group values have changed, write out the previous group values.
                    if (groupChanged)
                    {
                        if (Mappings.PassThroughColumns)
                        {
//                            //the first row of the next group has been read, so this is to store it until ready to write out.
//                            _nextPassThroughRow = newRow;
//                            _passThroughIndex = 0;
//                            _passThroughCount = _passThroughValues.Count; 
                            
                            var index = 0;
                            foreach(var row in _passThroughValues)
                            {
                                Mappings.ProcessAggregateRow(index, row);
                                index++;
                            }
                            
                            ////set the first cached row to current
                            outputRow = _passThroughValues.Dequeue();

                        }
                        else
                        {
                            Mappings.ProcessOutputRow(outputRow);
                            Mappings.ProcessAggregateRow(0, outputRow);
                            Mappings.Reset();
                            Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                        }
                        
//                        i = 0;
//
//                        if (_groupFields != null)
//                        {
//                            for(; i < _groupFields.Count(); i++)
//                            {
//                                newRow[i] = _groupValues[i];
//                            }
//                        }
//
//                        if (Mappings.PassThroughColumns == false)
//                        {
//                            if (_aggregates != null)
//                            {
//                                foreach (var aggregate in _aggregates)
//                                {
//                                    newRow[i] = aggregate.GetValue();
//                                    i++;
//                                }
//                            }
//
//                            if (_aggregateFunctions != null)
//                            {
//                                foreach (var mapping in _aggregateFunctions)
//                                {
//                                    try
//                                    {
//                                        newRow[i] = mapping.ReturnValue(0);
//                                        i++;
//
//                                        if (mapping.Outputs != null)
//                                        {
//                                            foreach (var output in mapping.Outputs)
//                                            {
//                                                newRow[i] = output.Value;
//                                                i++;
//                                            }
//                                        }
//                                    }
//                                    catch (Exception ex)
//                                    {
//                                        throw new TransformException($"The group transform {Name} failed on function {mapping.FunctionName}.  {ex.Message}.", ex);
//                                    }
//                                }
//                            }
//                        }
//                        //for passthrough, write out the aggregated values to the cached passthrough set
//                        else
//                        {
//                            var index = 0;
//                            var startColumn = i;
//                            foreach(var row in _passThroughValues)
//                            {
//                                i = startColumn;
//
//                                if (_aggregates != null)
//                                {
//                                    foreach (var aggregate in _aggregates)
//                                    {
//                                        row[i] = aggregate.GetValue();
//                                        i++;
//                                    }
//                                }
//
//                                if (_aggregateFunctions != null)
//                                {
//                                    foreach (var mapping in _aggregateFunctions)
//                                    {
//                                        try
//                                        {
//                                            row[i] = mapping.ReturnValue(index);
//                                            i++;
//
//                                            if (mapping.Outputs != null)
//                                            {
//                                                foreach (var output in mapping.Outputs)
//                                                {
//                                                    row[i] = output.Value;
//                                                    i++;
//                                                }
//                                            }
//                                        }
//                                        catch (Exception ex)
//                                        {
//                                            throw new TransformException($"The group transform {Name} failed, retrieving results from function {mapping.FunctionName}.  {ex.Message}.", ex);
//                                        }
//                                    }
//                                }
//                                index++;
//                            }
//
//                            //the first row of the next group has been read, so this is to store it until ready to write out.
//                            _nextPassThroughRow = new object[FieldCount];
//                            for (var j = 0; j< newGroupValues.Length; j++)
//                                _nextPassThroughRow[j] = newGroupValues[j];
//
//                            for (var j = _passThroughStartIndex; j < FieldCount; j++)
//                                _nextPassThroughRow[j] = PrimaryTransform[GetName(j)];
//
//                            ////set the first cached row to current
//                            newRow = _passThroughValues[0];
//                            _passThroughIndex = 1;
//                            _passThroughCount = _passThroughValues.Count;
//                        }

//                        //reset the functions
//                        if (_aggregates != null)
//                        {
//                            foreach (var aggregate in _aggregates)
//                            {
//                                aggregate.Reset();
//                            }
//                        }
//
//                        if (_aggregateFunctions != null)
//                        {
//                            foreach (var mapping in _aggregateFunctions)
//                            {
//                                mapping.Reset();
//                            }
//                        }

                        //store the last groupvalues read to start the next grouping.
                        _groupValues = newGroupValues;

                    }
                    else
                    {
                        // if the group has not changed, process the input row
                        Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                        
                        if (Mappings.PassThroughColumns)
                        {
                            //create a cached current row.  this will be output when the group has changed.
                            var cacheRow = new object[outputRow.Length];
                            Mappings.ProcessOutputRow(cacheRow);
                            _passThroughValues.Enqueue(cacheRow);
                            
//                            if (_groupValues != null)
//                            {
//                                for (var j = 0; j < _groupValues.Length; j++)
//                                    cacheRow[j] = _groupValues[j];
//                            }
//                            for (var j = _passThroughStartIndex; j < FieldCount; j++)
//                                cacheRow[j] = PrimaryTransform[GetName(j)];
//                            _passThroughValues.Add(cacheRow);
                        }

                    }

//                    // update the aggregate pairs
//                    if (_aggregates != null)
//                    {
//                        foreach (var aggregate in _aggregates)
//                        {
//                            aggregate.AddValue(PrimaryTransform[aggregate.SourceColumn]);
//                        }
//                    }
//
//                    // update the aggregate functions
//                    if (_aggregateFunctions != null)
//                    {
//                        foreach (var mapping in _aggregateFunctions)
//                        {
//                            if (mapping.Inputs != null)
//                            {
//                                foreach (var input in mapping.Inputs.Where(c => c.IsColumn))
//                                {
//                                    try
//                                    {
//                                        input.SetValue(PrimaryTransform[input.Column]);
//                                    }
//                                    catch (Exception ex)
//                                    {
//                                        throw new TransformException($"The group transform {Name} failed setting an incompatible value to column {input.Column.Name}.  {ex.Message}.", ex, PrimaryTransform[input.Column]);
//                                    }
//                                }
//                            }
//
//                            try
//                            {
//                                var invokeresult = mapping.Invoke();
//                            }
//                            catch (FunctionIgnoreRowException)
//                            {
//                                //TODO: Issue that some of the aggregate values may be calculated prior to the ignorerow being set.  
//                                TransformRowsIgnored++;
//                                continue;
//                            }
//                            catch (Exception ex)
//                            {
//                                throw new TransformException($"The group transform {Name} failed running the function {mapping.FunctionName}.  {ex.Message}.", ex);
//                            }
//                        }
//                    }

                    if (groupChanged)
                        break;

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                // Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
//                i = 0;
//                if (_groupFields != null)
//                {
//                    for(; i < _groupFields.Count(); i++)
//                    {
//                        newRow[i] = _groupValues[i];
//                    }
//                }
                
                if (Mappings.PassThroughColumns == false)
                {
                    Mappings.ProcessAggregateRow(0, outputRow);
                    
//                    if (_aggregates != null)
//                    {
//                        foreach (var aggregate in _aggregates)
//                        {
//                            newRow[i] = aggregate.GetValue();
//                            i++;
//
//                            aggregate.Reset();
//                        }
//                    }
//
//                    if (_aggregateFunctions != null)
//                    {
//                        foreach (var mapping in _aggregateFunctions)
//                        {
//                            try
//                            {
//                                newRow[i] = mapping.ReturnValue(0);
//                            }
//                            catch (Exception ex)
//                            {
//                                throw new TransformException($"The group transform {Name} failed, retrieving results from function {mapping.FunctionName}.  {ex.Message}.", ex);
//                            }
//                            i++;
//
//                            if (mapping.Outputs != null)
//                            {
//                                foreach (var output in mapping.Outputs)
//                                {
//                                    newRow[i] = output.Value;
//                                    i++;
//                                }
//                            }
//                            mapping.Reset();
//                        }
//                    }
                }
                else
                {
                    //for passthrough, write out the aggregated values to the cached passthrough set
                    var index = 0;
                    //var startColumn = i;
                    foreach (var row in _passThroughValues)
                    {
                        Mappings.ProcessAggregateRow(index, row);
                        
//                        i = startColumn;
//
//                        if (_aggregates != null)
//                        {
//                            foreach (var aggregate in _aggregates)
//                            {
//                                newRow[i] = aggregate.GetValue();
//                                i++;
//                            }
//                        }
//
//                        if (_aggregateFunctions != null)
//                        {
//                            foreach (var mapping in _aggregateFunctions)
//                            {
//                                try
//                                {
//                                    row[i] = mapping.ReturnValue(index);
//                                }
//                                catch (Exception ex)
//                                {
//                                    throw new TransformException($"The group transform {Name} failed retrieving results from function {mapping.FunctionName}.  {ex.Message}.", ex);
//                                }
//
//                                i++;
//
//                                if (mapping.Outputs != null)
//                                {
//                                    foreach (var output in mapping.Outputs)
//                                    {
//                                        row[i] = output.Value;
//                                        i++;
//                                    }
//                                }
//                            }
//                        }
                        index++;
                    }
                    
                    outputRow = _passThroughValues.Dequeue();

                    //set the first cached row to current
//                    outputRow = _passThroughValues[0];
//                    _passThroughIndex = 1;
//                    _passThroughCount = _passThroughValues.Count;
                }

                _groupValues = newGroupValues;
                _lastRecord = true;
            }
            return outputRow;

        }

        public override string Details()
        {
            return "Group: " + ( Mappings.PassThroughColumns ? "All columns passed through, " : "") + "Mapped Columns:" + (Mappings.Count());
        }

        public override List<Sort> RequiredSortFields()
        {
            return Mappings.OfType<MapGroup>().Select(c=> new Sort { Column = c.InputColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
