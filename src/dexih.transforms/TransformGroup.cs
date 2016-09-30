using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;

namespace dexih.transforms
{
    public class TransformGroup : Transform
    {
        public TransformGroup() {  }

        public TransformGroup(Transform inReader, List<ColumnPair> groupFields, List<Function> aggregates, bool passThroughColumns)
        {
            GroupFields = groupFields;
            Aggregates = aggregates;
            PassThroughColumns = passThroughColumns;

            SetInTransform(inReader);
        }

        bool _firstRecord;
        bool _lastRecord;

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

        public List<Function> Aggregates
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

            int i = 0;
            if (GroupFields != null)
            {
                _groupValues = new string[GroupFields.Count];
                _firstRecord = true;

                foreach (ColumnPair groupField in GroupFields)
                {
                    var column = PrimaryTransform.CacheTable.Columns[groupField.SourceColumn];
                    column.Schema = "";
                    column.ColumnName = groupField.TargetColumn.ColumnName;
                    CacheTable.Columns.Add(column);
                    i++;
                }
            }

            foreach (Function aggregate in Aggregates)
            {
                var column = new TableColumn(aggregate.TargetColumn.ColumnName, aggregate.ReturnType);
                CacheTable.Columns.Add(column);
                i++;

                if (aggregate.Outputs != null)
                {
                    foreach (Parameter param in aggregate.Outputs)
                    {
                        var paramColumn = new TableColumn(param.Column.ColumnName, param.DataType);
                        CacheTable.Columns.Add(paramColumn);
                        i++;
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

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query)
        {
            AuditKey = auditKey;

            if (query == null)
                query = new SelectQuery();

            var requiredSorts = RequiredSortFields();

            if(query.Sorts != null && query.Sorts.Count > 0)
            {
                for(int i =0; i<requiredSorts.Count; i++)
                {
                    if (query.Sorts[i].Column == requiredSorts[i].Column)
                        requiredSorts[i].Direction = query.Sorts[i].Direction;
                    else
                        break;
                }
            }

            query.Sorts = requiredSorts;

            var returnValue = await PrimaryTransform.Open(auditKey, query);
            return returnValue;
        }


        public override ReturnValue ResetTransform()
        {
            foreach (Function aggregate in Aggregates)
                aggregate.Reset();
            _firstRecord = true;
            _lastRecord = true;
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
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
                    return new ReturnValue<object[]>(true, newRow);
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
            bool groupChanged = false;
            object[] newGroupValues = null;

            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                if (_lastRecord) //return false is all record have been written.
                    return new ReturnValue<object[]>(false, null);
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
                            foreach (ColumnPair groupField in GroupFields)
                            {
                                _groupValues[i] = PrimaryTransform[groupField.SourceColumn].ToString();
                                i++;
                            }
                        }
                    }
                    else
                    {
                        //if not first row, then check if the group values have changed from the previous row
                        if (GroupFields != null)
                        {
                            newGroupValues = new string[GroupFields.Count];

                            foreach (ColumnPair groupField in GroupFields)
                            {
                                newGroupValues[i] = PrimaryTransform[groupField.SourceColumn].ToString();
                                if ((string)newGroupValues[i] != (string)_groupValues[i])
                                    groupChanged = true;
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
                            foreach (Function mapping in Aggregates)
                            {
                                var result = mapping.ReturnValue(0);
                                if (result.Success == false)
                                    throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                                newRow[i] = result.Value;
                                i++;

                                if (mapping.Outputs != null)
                                {
                                    foreach (Parameter output in mapping.Outputs)
                                    {
                                        newRow[i] = output.Value;
                                        i++;
                                    }
                                }
                            }
                        }
                        //for passthrough, write out the aggregated values to the cached passthrough set
                        else
                        {
                            int index = 0;
                            int startColumn = i;
                            foreach(object[] row in _passThroughValues)
                            {
                                i = startColumn;
                                foreach (Function mapping in Aggregates)
                                {
                                    var result = mapping.ReturnValue(index);
                                    if (result.Success == false)
                                        throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                                    row[i] = result.Value;
                                    i++;

                                    if (mapping.Outputs != null)
                                    {
                                        foreach (Parameter output in mapping.Outputs)
                                        {
                                            row[i] = output.Value;
                                            i++;
                                        }
                                    }
                                }
                                index++;
                            }

                            //the first row of the next group has been read, so this is to store it until ready to write out.
                            _nextPassThroughRow = new object[FieldCount];
                            for (int j = 0; j< newGroupValues.Length; j++)
                                _nextPassThroughRow[j] = newGroupValues[j];

                            for (int j = _passThroughStartIndex; j < FieldCount; j++)
                                _nextPassThroughRow[j] = PrimaryTransform[GetName(j)];

                            ////set the first cached row to current
                            newRow = _passThroughValues[0];
                            _passThroughIndex = 1;
                            _passThroughCount = _passThroughValues.Count;
                        }

                        //reset the functions
                        foreach (Function mapping in Aggregates)
                            mapping.Reset();

                        //store the last groupvalues read to start the next grouping.
                        _groupValues = newGroupValues;

                    }
                    else
                    {
                        if (PassThroughColumns)
                        {
                            //create a cached current row.  this will be output when the group has changed.
                            object[] cacheRow = new object[newRow.Length];
                            if (_groupValues != null)
                            {
                                for (int j = 0; j < _groupValues.Length; j++)
                                    cacheRow[j] = _groupValues[j];
                            }
                            for (int j = _passThroughStartIndex; j < FieldCount; j++)
                                cacheRow[j] = PrimaryTransform[GetName(j)];
                            _passThroughValues.Add(cacheRow);
                        }

                    }

                    // update the aggregate values
                    foreach (Function mapping in Aggregates)
                    {
                        if (mapping.Inputs != null)
                        {
                            foreach (Parameter input in mapping.Inputs.Where(c => c.IsColumn))
                            {
                                var result = input.SetValue(PrimaryTransform[input.Column]);
                                if (result.Success == false)
                                    throw new Exception("Error setting aggregate values: " + result.Message);
                            }
                        }
                        var invokeresult = mapping.Invoke();
                        if (invokeresult.Success == false)
                            throw new Exception("Error setting aggregate values: " + invokeresult.Message);
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

                    foreach (Function mapping in Aggregates)
                    {
                        var result = mapping.ReturnValue(0);
                        if (result.Success == false)
                            throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                        newRow[i] = result.Value;
                        i++;

                        if (mapping.Outputs != null)
                        {
                            foreach (Parameter output in mapping.Outputs)
                            {
                                newRow[i] = output.Value;
                                i++;
                            }
                        }
                        mapping.Reset();
                    }
                }
                else
                {
                    //for passthrough, write out the aggregated values to the cached passthrough set
                    int index = 0;
                    int startColumn = i;
                    foreach (object[] row in _passThroughValues)
                    {
                        i = startColumn;
                        foreach (Function mapping in Aggregates)
                        {
                            var result = mapping.ReturnValue(index);
                            if (result.Success == false)
                                throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                            row[i] = result.Value;
                            i++;

                            if (mapping.Outputs != null)
                            {
                                foreach (Parameter output in mapping.Outputs)
                                {
                                    row[i] = output.Value;
                                    i++;
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
            return new ReturnValue<object[]>(true, newRow);

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
