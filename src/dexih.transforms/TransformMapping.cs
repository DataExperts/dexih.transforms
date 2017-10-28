using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

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

        private List<int> _passThroughFields;
        private List<int> _mapFieldOrdinals;
        private List<int> _functionInputOrdinals;

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

        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            List<Filter> newFilters = null;
            List<Sort> newSorts = null;

            //we need to translate filters and sorts to source column names before passing them through.
            if(query != null && query.Filters != null)
            {
                newFilters = new List<Filter>();
                foreach(var filter in query.Filters)
                {
                    TableColumn column1 = null;
                    TableColumn column2 = null;
                    if (filter.Column1 != null)
                    {
                        column1 = TranslateColumnName(filter.Column1);
                        if (column1 == null)
                            continue;
                    }

                    if (filter.Column2 != null)
                    {
                        column2 = TranslateColumnName(filter.Column2);
                        if (column2 == null)
                            continue;
                    }

					var newFilter = new Filter
					{
						Column1 = column1,
						Column2 = column2,
						Operator = filter.Operator,
						Value1 = filter.Value1,
						Value2 = filter.Value2,
						CompareDataType = filter.CompareDataType
					};

					newFilters.Add(newFilter);
				}

				query.Filters = newFilters;
			}

			//we need to translate filters and sorts to source column names before passing them through.
			if (query != null && query.Sorts != null)
			{
				newSorts = new List<Sort>();
				foreach (var sort in query.Sorts)
				{
					TableColumn column = null;
					if (sort.Column != null)
					{
						column = TranslateColumnName(sort.Column);
						if (column == null)
							continue;
					}


					var newSort = new Sort
					{
						Column = column,
						Direction = sort.Direction
					};
					newSorts.Add(newSort);
				}

				query.Sorts = newSorts;
			}

			var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
			return returnValue;
        }

        public TableColumn TranslateColumnName(TableColumn outputColumn)
        {
            if (outputColumn == null)
                return null;
            else
            {
                if (MapFields != null)
                {
                    var mapping = MapFields.SingleOrDefault(c => c.TargetColumn.TableColumnName() == outputColumn.TableColumnName());
                    if (mapping != null)
                        return mapping.SourceColumn.Copy();
                }

                if(PassThroughColumns)
                {
                    var column = CacheTable.Columns.SingleOrDefault(c => c.TableColumnName() == outputColumn.TableColumnName());
                    if (column != null)
                        return outputColumn;
                }
            }

            return null;
        }

        public override bool InitializeOutputFields()
        {
            var i = 0;
            CacheTable = new Table("Mapping");

            if (MapFields != null)
            {
                _mapFieldOrdinals = new List<int>();

                foreach (var mapField in MapFields)
                {
                    var column = PrimaryTransform.CacheTable.Columns[mapField.SourceColumn];

                    if(column == null)
                    {
                        throw new Exception("The mapping " + mapField.SourceColumn.Name + " to " + mapField.TargetColumn.Name + " could not be completed, as the source field was missing.");
                    }
					var columnCopy = column.Copy();
					columnCopy.Name = mapField.TargetColumn.Name;
					CacheTable.Columns.Add(columnCopy);
                    //store an mapFieldOrdinal to improve performance.
                    _mapFieldOrdinals.Add(PrimaryTransform.GetOrdinal(mapField.SourceColumn));
                    i++;
                }
            }

            if (Mappings != null)
            {
                _functionInputOrdinals = new List<int>();

                foreach (var mapping in Mappings)
                {
                    //store the ordinals for each parameter to improve performance
                    foreach(var parameter in mapping.Inputs)
                    {
                        if (parameter.IsColumn)
                        {
                            if (parameter.Column == null)
                                throw new Exception("The mapping " + mapping.FunctionDetail() + " could not be executed as there was an error with one of the parameters.");

							var ordinal = PrimaryTransform.GetOrdinal(parameter.Column);

							if(ordinal < 0) 
							{
								throw new TransformException($"The mapping {mapping.FunctionDetail()} could not be executed as the input column {parameter.Column.TableColumnName()} could not be found in the source transform or table.");	
							}
                            _functionInputOrdinals.Add(PrimaryTransform.GetOrdinal(parameter.Column));
                        }
                    }
                    if (mapping.TargetColumn != null)
                    {
                        var column = new TableColumn(mapping.TargetColumn.Name, mapping.ReturnType);
                        CacheTable.Columns.Add(column);

                        i++;
                    }

                    if (mapping.Outputs != null)
                    {
                        foreach (var param in mapping.Outputs)
                        {
                            if (param.Column != null)
                            {
                                var column = new TableColumn(param.Column.Name, param.DataType);
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

                for(var j = 0; j< PrimaryTransform.CacheTable.Columns.Count; j++)
                {
                    var column = PrimaryTransform.CacheTable.Columns[j];
                    if (CacheTable.Columns.SingleOrDefault(c => c.Name == column.Name) == null)
                    {
                        CacheTable.Columns.Add(column.Copy());
                        _passThroughFields.Add(j);
                    }
                }
            }

            if (PrimaryTransform.CacheTable.OutputSortFields != null)
            {
                //pass through the previous sort order, however limit to fields which have been mapped.
                var fields = new List<Sort>();
                foreach (var t in PrimaryTransform.CacheTable.OutputSortFields)
                {
                    var mapping = ColumnPairs?.FirstOrDefault(c => c.SourceColumn == t.Column);
                    if (mapping == null)
                    {
                        //if passthrough column is on, and none of the function mappings override the target field then it is included.
                        if (PassThroughColumns && !Functions.Any(c => c.TargetColumn?.Name == t.Column.Name || c.Inputs.Any(d => d.Column?.Name == t.Column.Name)))
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
			return "Mapping(" + Name + "):  Columns Mapped:" + (MapFields?.Count.ToString() ?? "Nill") + ", Functions Mapped:" + (Mappings?.Count.ToString() ?? "Nill");
        }

        #region Transform Implementations

        public override bool RequiresSort => false;


        public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
			while (true) // while loop is used to allow a function with skiprecord to work.
			{
				var skipRecord = false;

				var i = 0;
				var newRow = new object[FieldCount];

				var readResult = await PrimaryTransform.ReadAsync(cancellationToken);
				if (readResult == false)
				{
					return null;
				}

				if (MapFields != null)
				{
					foreach (var mapField in _mapFieldOrdinals)
					{
						newRow[i] = PrimaryTransform[mapField];
						i = i + 1;
					}
				}
				//processes the mappings
				if (Mappings != null)
				{
					var parameterInputCount = 0;
					foreach (var mapping in Mappings)
					{
						foreach (var input in mapping.Inputs.Where(c => c.IsColumn))
						{
							try
							{
								input.SetValue(PrimaryTransform[_functionInputOrdinals[parameterInputCount]]);
							}
							catch (Exception ex)
							{
								throw new TransformException($"The mapping transform failed setting parameters on the function {mapping.FunctionName} parameter {input.Name}.  {ex.Message}.", ex.Message, PrimaryTransform[_functionInputOrdinals[parameterInputCount]]);
							}

							parameterInputCount++;
						}

						try
						{
							var invokeresult = mapping.Invoke();
							if (mapping.TargetColumn != null)
							{
								newRow[i] = invokeresult;
								i = i + 1;
							}
						}
						catch (FunctionIgnoreRowException)
						{
							TransformRowsIgnored++;
							skipRecord = true;
							continue;
						}
						catch (Exception ex)
						{
							throw new TransformException($"The mapping transform failed calling the function {mapping.FunctionName}.  {ex.Message}.", ex);
						}

						if (mapping.Outputs != null)
						{
							foreach (var output in mapping.Outputs)
							{
								if (output.Column != null)
								{
									newRow[i] = output.Value;
									i = i + 1;
								}
							}
						}
					}

					if(skipRecord) 
					{
						continue;
					}
				}

				if (PassThroughColumns)
				{
					foreach (var index in _passThroughFields)
					{
						newRow[i] = PrimaryTransform[index];
						i = i + 1;
					}
				}

				return newRow;
			}
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
