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
    public class TransformMapping : Transform 
    {
        readonly Dictionary<int, string> _fieldNames = new Dictionary<int, string>();
        readonly Dictionary<string, int> _fieldOrdinals = new Dictionary<string, int>();
        int _fieldCount;

        List<string> _passThroughFields;

        public List<Function> Mappings
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

        public List<ColumnPair> MapFields
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
        public override bool Initialize()
        {
            _fieldNames.Clear();
            _fieldOrdinals.Clear();

            int i = 0;
            if (MapFields != null)
            {
                foreach (ColumnPair mapField in MapFields)
                {
                    _fieldNames.Add(i, mapField.TargetColumn);
                    _fieldOrdinals.Add(mapField.TargetColumn, i);
                    i++;
                }
            }

            if (Mappings != null)
            {
                foreach (Function mapping in Mappings)
                {
                    if (mapping.TargetColumn != "")
                    {
                        _fieldNames.Add(i, mapping.TargetColumn);
                        _fieldOrdinals.Add(mapping.TargetColumn, i);
                        i++;
                    }

                    if (mapping.Outputs != null)
                    {
                        foreach (Parameter param in mapping.Outputs)
                        {
                            if (param.ColumnName != "")
                            {
                                _fieldNames.Add(i, param.ColumnName);
                                _fieldOrdinals.Add(param.ColumnName, i);
                                i++;
                            }
                        }
                    }
                }
            }

            //if passthrough is set-on load any unused columns to the output.
            if(PassThroughColumns)
            {
                _passThroughFields = new List<string>();

                for(int j = 0; j< Reader.FieldCount; j++)
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
            _fieldCount = _fieldOrdinals.Count;

            Fields = _fieldNames.Select(c => c.Value).ToArray();

            return true;
        }

        public override string Details()
        {
            return "Mapping:  Columns Mapped:" + (MapFields?.Count.ToString() ?? "Nill") + ", Functions Mapped:" + (Mappings?.Count.ToString() ?? "Nill");
        }

        public bool SetMappings(List<ColumnPair> mapFields, List<Function> mappings)
        {
            Mappings = mappings;
            MapFields = mapFields;

            //return Initialize();
            return true;
        }

        #region Transform Implementations
        public override int FieldCount => _fieldCount;

        /// <summary>
        /// checks if filter can execute against the database query.
        /// </summary>
        public override bool CanRunQueries
        {
            get
            {
                return Mappings.Exists(c => c.CanRunSql == false) && Reader.CanRunQueries;
            }
        }

        public override bool PrefersSort => false;
        public override bool RequiresSort => false;


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
            return true;
        }

        protected override bool ReadRecord()
        {
            int i = 0;
            CurrentRow = new object[_fieldCount];

            if (Reader.Read() == false)
                return false;
            if (MapFields != null)
            {
                foreach (ColumnPair mapField in MapFields)
                {
                    CurrentRow[i] = Reader[mapField.SourceColumn];
                    i = i + 1;
                }
            }
            //processes the mappings
            if (Mappings != null)
            {
                foreach (Function mapping in Mappings)
                {
                    foreach (Parameter input in mapping.Inputs.Where(c => c.IsColumn))
                    {
                        var result = input.SetValue(Reader[input.ColumnName]);
                        if (result.Success == false)
                            throw new Exception("Error setting mapping values: " + result.Message);
                    }
                    var invokeresult = mapping.Invoke();
                    if(invokeresult.Success== false)
                        throw new Exception("Error invoking mapping function: " + invokeresult.Message);

                    if (mapping.TargetColumn != "")
                    {
                        CurrentRow[i] = invokeresult.Value;
                        i = i + 1;
                    }

                    if (mapping.Outputs != null)
                    {
                        foreach (Parameter output in mapping.Outputs)
                        {
                            if (output.ColumnName != "")
                            {
                                CurrentRow[i] = output.Value;
                                i = i + 1;
                            }
                        }
                    }
                }
            }

            if (PassThroughColumns)
            {
                foreach (string columnName in _passThroughFields)
                {
                    CurrentRow[i] = Reader[columnName];
                    i = i + 1;
                }
            }

            return true;
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }
        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

        //mapping will maintain sort order.
        public override List<Sort> OutputSortFields()
        {
            if (Reader.OutputSortFields() == null)
                return null;

            //pass through the previous sort order, however limit to fields which have been mapped.
            List<Sort> fields = new List<Sort>();
            foreach (Sort t in Reader.OutputSortFields())
            {
                ColumnPair mapping = ColumnPairs?.FirstOrDefault(c => c.SourceColumn == t.Column);
                if (mapping == null)
                {
                    //if passthrough column is on, and non of the function mappings override the target field then it is included.
                    if (PassThroughColumns && !Functions.Any(c => c.TargetColumn == t.Column || c.Inputs.Any(d => d.ColumnName == t.Column)))
                    {
                        fields.Add(t);
                    }
                    else
                        break;
                }
                else
                    fields.Add(t);
            }

            return fields;
        }


        #endregion
    }
}
