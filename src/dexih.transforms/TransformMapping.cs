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
            int i = 0;
            CachedTable = new Table("Mapping");

            if (MapFields != null)
            {
                foreach (ColumnPair mapField in MapFields)
                {
                    var column = Reader.CachedTable.Columns.Single(c => c.ColumnName == mapField.SourceColumn);
                    column.ColumnName = mapField.TargetColumn;
                    CachedTable.Columns.Add(column);
                    i++;
                }
            }

            if (Mappings != null)
            {
                foreach (Function mapping in Mappings)
                {
                    if (mapping.TargetColumn != "")
                    {
                        var column = new TableColumn(mapping.TargetColumn, mapping.ReturnType);
                        CachedTable.Columns.Add(column);

                        i++;
                    }

                    if (mapping.Outputs != null)
                    {
                        foreach (Parameter param in mapping.Outputs)
                        {
                            if (param.ColumnName != "")
                            {
                                var column = new TableColumn(param.ColumnName, param.DataType);
                                CachedTable.Columns.Add(column);
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

                foreach (var column in Reader.CachedTable.Columns)
                {
                    if (CachedTable.Columns.SingleOrDefault(c => c.ColumnName == column.ColumnName) == null)
                    {
                        CachedTable.Columns.Add(column.Copy());
                        _passThroughFields.Add(column.ColumnName);
                    }
                }
            }


            if (Reader.CachedTable.OutputSortFields != null)
            {
                //pass through the previous sort order, however limit to fields which have been mapped.
                List<Sort> fields = new List<Sort>();
                foreach (Sort t in Reader.CachedTable.OutputSortFields)
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

                CachedTable.OutputSortFields = fields;
            }

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


        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
        }

        protected override bool ReadRecord()
        {
            int i = 0;
            CurrentRow = new object[FieldCount];

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


        #endregion
    }
}
