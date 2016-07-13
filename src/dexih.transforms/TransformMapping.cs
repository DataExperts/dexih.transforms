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
    public class TransformMapping : Transform 
    {
        public TransformMapping() { }

        public TransformMapping(Transform inTransform, bool passThroughColumns, List<ColumnPair> mapFields, List<Function> mappings)
        {
            Mappings = mappings;
            MapFields = mapFields;
            PassThroughColumns = passThroughColumns;

            SetInTransform(inTransform, null);
        }

        List<int> _passThroughFields;
        List<int> _mapFieldOrdinals; 
        List<int> _functionInputOrdinals;

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

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            List<Filter> newFilters = null;
            List<Sort> newSorts = null;

            //we need to translate filters and sorts to source column names before passing them through.
            if(query != null && query.Filters != null)
            {
                newFilters = new List<Filter>();
                foreach(var filter in query.Filters)
                {
                    string column1 = null;
                    string column2 = null;
                    if (!String.IsNullOrEmpty(filter.Column1))
                    {
                        column1 = TranslateColumnName(filter.Column1);
                        if (string.IsNullOrEmpty(column1))
                            continue;
                    }

                    if (!String.IsNullOrEmpty(filter.Column2))
                    {
                        column2 = TranslateColumnName(filter.Column2);
                        if (string.IsNullOrEmpty(column2))
                            continue;
                    }

                    Filter newFilter = new Filter();
                    newFilter.Column1 = column1;
                    newFilter.Column2 = column2;
                    newFilter.Operator = filter.Operator;
                    newFilter.Value1 = filter.Value1;
                    newFilter.Value2 = filter.Value2;
                    newFilter.CompareDataType = newFilter.CompareDataType;

                    newFilters.Add(newFilter);
                }
            }

            //we need to translate filters and sorts to source column names before passing them through.
            if (query != null && query.Sorts != null)
            {
                newSorts = new List<Sort>();
                foreach (var sort in query.Sorts)
                {
                    string column = null;
                    if (!String.IsNullOrEmpty(sort.Column))
                    {
                        column = TranslateColumnName(sort.Column);
                        if (string.IsNullOrEmpty(column))
                            continue;
                    }


                    Sort newSort = new Sort();
                    newSort.Column = column;
                    newSort.Direction = sort.Direction;
                    newSorts.Add(newSort);
                }
            }

            var returnValue = await PrimaryTransform.Open(query);
            return returnValue;
        }

        public string TranslateColumnName(string outputColumn)
        {
            if (String.IsNullOrEmpty(outputColumn))
                return outputColumn;
            else
            {
                if (MapFields != null)
                {
                    var mapping = MapFields.SingleOrDefault(c => c.TargetColumn == outputColumn);
                    if (mapping != null)
                        return mapping.SourceColumn;
                }

                if(PassThroughColumns)
                {
                    var column = CacheTable.Columns.SingleOrDefault(c => c.ColumnName == outputColumn);
                    if (column != null)
                        return outputColumn;
                }
            }

            return null;
        }

        public override bool InitializeOutputFields()
        {
            int i = 0;
            CacheTable = new Table("Mapping");

            if (MapFields != null)
            {
                _mapFieldOrdinals = new List<int>();

                foreach (ColumnPair mapField in MapFields)
                {
                    var column = PrimaryTransform.CacheTable.Columns.Single(c => c.ColumnName == mapField.SourceColumn);
                    column.ColumnName = mapField.TargetColumn;
                    CacheTable.Columns.Add(column);
                    //store an mapFieldOrdinal to improve performance.
                    _mapFieldOrdinals.Add(PrimaryTransform.GetOrdinal(mapField.SourceColumn));
                    i++;
                }
            }

            if (Mappings != null)
            {
                _functionInputOrdinals = new List<int>();

                foreach (Function mapping in Mappings)
                {
                    //store the ordinals for each parameter to improve performance
                    foreach(var parameter in mapping.Inputs)
                    {
                        if (parameter.IsColumn)
                            _functionInputOrdinals.Add(PrimaryTransform.GetOrdinal(parameter.ColumnName));
                    }
                    if (mapping.TargetColumn != "")
                    {
                        var column = new TableColumn(mapping.TargetColumn, mapping.ReturnType);
                        CacheTable.Columns.Add(column);

                        i++;
                    }

                    if (mapping.Outputs != null)
                    {
                        foreach (Parameter param in mapping.Outputs)
                        {
                            if (param.ColumnName != "")
                            {
                                var column = new TableColumn(param.ColumnName, param.DataType);
                                CacheTable.Columns.Add(column);
                                i++;
                            }
                        }
                    }
                }
            }

            //if passthrough is set-on load any unused columns to the output.
            if(PassThroughColumns)
            {
                _passThroughFields = new List<int>();

                for(int j = 0; j< PrimaryTransform.CacheTable.Columns.Count; j++)
                {
                    var column = PrimaryTransform.CacheTable.Columns[j];
                    if (CacheTable.Columns.SingleOrDefault(c => c.ColumnName == column.ColumnName) == null)
                    {
                        CacheTable.Columns.Add(column.Copy());
                        _passThroughFields.Add(j);
                    }
                }
            }


            if (PrimaryTransform.CacheTable.OutputSortFields != null)
            {
                //pass through the previous sort order, however limit to fields which have been mapped.
                List<Sort> fields = new List<Sort>();
                foreach (Sort t in PrimaryTransform.CacheTable.OutputSortFields)
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

                CacheTable.OutputSortFields = fields;
            }

            return true;
        }

        public override string Details()
        {
            return "Mapping:  Columns Mapped:" + (MapFields?.Count.ToString() ?? "Nill") + ", Functions Mapped:" + (Mappings?.Count.ToString() ?? "Nill");
        }

        #region Transform Implementations

        public override bool RequiresSort => false;


        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            int i = 0;
            var newRow = new object[FieldCount];

            var readResult = await PrimaryTransform.ReadAsync(cancellationToken);
            if (readResult == false)
                return new ReturnValue<object[]>(false, null);
            if (MapFields != null)
            {
                foreach (int mapField in _mapFieldOrdinals)
                {
                    newRow[i] = PrimaryTransform[mapField];
                    i = i + 1;
                }
            }
            //processes the mappings
            if (Mappings != null)
            {
                int parameterInputCount = 0;
                foreach (Function mapping in Mappings)
                {
                    foreach (Parameter input in mapping.Inputs.Where(c => c.IsColumn))
                    {
                        var result = input.SetValue(PrimaryTransform[_functionInputOrdinals[parameterInputCount]]);
                        parameterInputCount++;
                        if (result.Success == false)
                            throw new Exception("Error setting mapping values: " + result.Message);
                    }
                    var invokeresult = mapping.Invoke();
                    if(invokeresult.Success== false)
                        throw new Exception("Error invoking mapping function: " + invokeresult.Message);

                    if (mapping.TargetColumn != "")
                    {
                        newRow[i] = invokeresult.Value;
                        i = i + 1;
                    }

                    if (mapping.Outputs != null)
                    {
                        foreach (Parameter output in mapping.Outputs)
                        {
                            if (output.ColumnName != "")
                            {
                                newRow[i] = output.Value;
                                i = i + 1;
                            }
                        }
                    }
                }
            }

            if (PassThroughColumns)
            {
                foreach (int index in _passThroughFields)
                {
                    newRow[i] = PrimaryTransform[index];
                    i = i + 1;
                }
            }

            return new ReturnValue<object[]>(true, newRow);
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }
        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }


        #endregion
    }
}
