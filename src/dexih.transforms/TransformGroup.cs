using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

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

        readonly Dictionary<int, string> _fieldNames = new Dictionary<int, string>();
        readonly Dictionary<string, int> _fieldOrdinals = new Dictionary<string, int>();
        int _fieldCount;

        bool _firstRecord;
        bool _lastRecord;

        private object[] _groupValues;

        private List<object[]> _passThroughValues;
        private int _passThroughStartIndex;
        private int _passThroughIndex;
        private int _passThroughCount;
        private object[] _nextPassThroughRow;

        private List<string> _passThroughFields;

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

        public override bool Initialize()
        {
            _fieldNames.Clear();
            _fieldOrdinals.Clear();

            int i = 0;
            if (GroupFields != null)
            {
                _groupValues = new string[GroupFields.Count];
                _firstRecord = true;

                foreach (ColumnPair groupField in GroupFields)
                {
                    _fieldNames.Add(i, groupField.TargetColumn);
                    _fieldOrdinals.Add(groupField.TargetColumn, i);
                    i++;
                }
            }

            foreach (Function aggregate in Aggregates)
            {
                _fieldNames.Add(i, aggregate.TargetColumn);
                _fieldOrdinals.Add(aggregate.TargetColumn, i);
                i++;

                if (aggregate.Outputs != null)
                {
                    foreach (Parameter param in aggregate.Outputs)
                    {
                        _fieldNames.Add(i, param.ColumnName);
                        _fieldOrdinals.Add(param.ColumnName, i);
                        i++;
                    }
                }
            }

            //if passthrough is set-on load any unused columns to the output.
            if (PassThroughColumns)
            {
                _passThroughFields = new List<string>();
                _passThroughStartIndex = i;

                for (int j = 0; j < Reader.FieldCount; j++)
                {
                    string columnName = Reader.GetName(j);
                    if (_fieldOrdinals.ContainsKey(columnName) == false)
                    {
                        _fieldNames.Add(i, columnName);
                        _fieldOrdinals.Add(columnName, i);
                        i++;
                        _passThroughFields.Add(columnName);
                    }
                }
            }

            Fields = _fieldNames.Select(c => c.Value).ToArray();
            _fieldCount = _fieldOrdinals.Count;
            return true;
        }

        public bool SetMappings(List<ColumnPair> groupFields, List<Function> aggregates)
        {
            GroupFields = groupFields;
            Aggregates = aggregates;

            return Initialize();
        }

        public override int FieldCount => _fieldCount;

        /// <summary>
        /// checks if filter can execute against the database query.
        /// </summary>
        public override bool CanRunQueries
        {
            get
            {
                return Aggregates.Exists(c => c.CanRunSql == false) && Reader.CanRunQueries;
            }
        }

        public override bool PrefersSort => true;
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


        public override string GetName(int i)
        {
            return _fieldNames[i];
        }

        public override int GetOrdinal(string columnName)
        {
            if (_fieldOrdinals.ContainsKey(columnName))
                return _fieldOrdinals[columnName];
            return -1;
        }

        public override bool ResetValues()
        {
            foreach (Function aggregate in Aggregates)
                aggregate.Reset();
            _firstRecord = true;
            _lastRecord = true;
            return true;
        }

        protected override bool ReadRecord()
        {
            //if there are records in the passthrough cache, then empty them out before getting new records.
            if (PassThroughColumns)
            {
                if(_firstRecord)
                {
                    _passThroughValues = new List<object[]>();
                }
                else if (_passThroughIndex > 0 && _passThroughIndex < _passThroughCount)
                {
                    CurrentRow = _passThroughValues[_passThroughIndex];
                    _passThroughIndex++;
                    return true;
                }
                //if all rows have been iterated through, reset the cache and add the stored row for the next group 
                else if(_passThroughIndex >= _passThroughCount && _firstRecord == false && _lastRecord == false)
                {
                    _passThroughValues.Clear();
                    _passThroughValues.Add(_nextPassThroughRow);
                }
            }

            CurrentRow = new object[_fieldCount];

            int i;
            bool groupChanged = false;
            object[] newGroupValues = null;

            if (Reader.Read() == false)
            {
                if (_lastRecord) //return false is all record have been written.
                    return false;
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
                                _groupValues[i] = Reader[groupField.SourceColumn].ToString();
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
                                newGroupValues[i] = Reader[groupField.SourceColumn].ToString();
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
                                CurrentRow[i] = _groupValues[i];
                            }
                        }

                        if (PassThroughColumns == false)
                        {
                            foreach (Function mapping in Aggregates)
                            {
                                var result = mapping.ReturnValue(0);
                                if (result.Success == false)
                                    throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                                CurrentRow[i] = result.Value;
                                i++;

                                if (mapping.Outputs != null)
                                {
                                    foreach (Parameter output in mapping.Outputs)
                                    {
                                        CurrentRow[i] = output.Value;
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
                            _nextPassThroughRow = new object[_fieldCount];
                            for (int j = 0; j< newGroupValues.Length; j++)
                                _nextPassThroughRow[j] = newGroupValues[j];

                            for (int j = _passThroughStartIndex; j < FieldCount; j++)
                                _nextPassThroughRow[j] = Reader[GetName(j)];

                            ////set the first cached row to current
                            CurrentRow = _passThroughValues[0];
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
                            object[] newRow = new object[CurrentRow.Length];
                            if (_groupValues != null)
                            {
                                for (int j = 0; j < _groupValues.Length; j++)
                                    newRow[j] = _groupValues[j];
                            }
                            for (int j = _passThroughStartIndex; j < FieldCount; j++)
                                newRow[j] = Reader[GetName(j)];
                            _passThroughValues.Add(newRow);
                        }

                    }

                    // update the aggregate values
                    foreach (Function mapping in Aggregates)
                    {
                        if (mapping.Inputs != null)
                        {
                            foreach (Parameter input in mapping.Inputs.Where(c => c.IsColumn))
                            {
                                var result = input.SetValue(Reader[input.ColumnName]);
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

                } while (Reader.Read());
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                i = 0;
                if (GroupFields != null)
                {
                    for(; i < GroupFields.Count; i++)
                    {
                        CurrentRow[i] = _groupValues[i];
                    }
                }
                if (PassThroughColumns == false)
                {

                    foreach (Function mapping in Aggregates)
                    {
                        var result = mapping.ReturnValue(0);
                        if (result.Success == false)
                            throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                        CurrentRow[i] = result.Value;
                        i++;

                        if (mapping.Outputs != null)
                        {
                            foreach (Parameter output in mapping.Outputs)
                            {
                                CurrentRow[i] = output.Value;
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
                    CurrentRow = _passThroughValues[0];
                    _passThroughIndex = 1;
                    _passThroughCount = _passThroughValues.Count;
                }

                _groupValues = newGroupValues;
                _lastRecord = true;
            }
            return true;

        }

        public override string Details()
        {
            return "Group: " + ( PassThroughColumns ? "All columns passed through, " : "") + "Grouped Columns:" + (GroupFields?.Count.ToString() ?? "Nill") + ", Series/Aggregate Functions:" + (Functions?.Count.ToString() ?? "Nill");
        }

        public override List<Sort> RequiredSortFields()
        {
            return GroupFields.Select(c=> new Sort { Column = c.SourceColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

        //group will return sorted by the group fields.
        public override List<Sort> OutputSortFields()
        {
            return GroupFields.Select(c=> new Sort { Column = c.TargetColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override Task<ReturnValue> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }
    }
}
