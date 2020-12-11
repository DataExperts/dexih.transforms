using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Exceptions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;

namespace dexih.transforms
{
	[Transform(
		Name = "Mapping",
		Description = "Apply mapping rules from source to target.",
		TransformType = ETransformType.Mapping
	)]
    public class TransformMapping : Transform 
    {
        public TransformMapping() { }

        public TransformMapping(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inTransform);
        }
        
        public override string TransformName { get; } = "Map";

        public override Dictionary<string, object> TransformProperties()
        {
	        return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            if (requestQuery?.Rows > 0 && requestQuery.Rows < MaxOutputRows)
            {
                MaxOutputRows = requestQuery.Rows;
            }
            var newSelectQuery = requestQuery?.CloneProperties() ?? new SelectQuery();
            
            // TODO Allow mapping to translate group columns and push-down and group-by.
            newSelectQuery.Groups = new List<TableColumn>();
            newSelectQuery.GroupFilters = new Filters();
            
            // get only the required columns
            newSelectQuery.Columns = new SelectColumns(Mappings.GetRequiredColumns());
            
            var mappedFilters = new Dictionary<Filter, Filter>();
            
	        //we need to translate filters and sorts to source column names before passing them through.
            if(requestQuery?.Filters != null)
            {
                var newFilters = new Filters();
                foreach(var filter in requestQuery.Filters)
                {
                    TableColumn column1 = null;
                    TableColumn column2 = null;
                    var value1 = filter.Value1;
                    var value2 = filter.Value2;
                    
                    if (filter.Column1 != null)
                    {
	                    bool found;
                        (found, column1, value1) = TranslateTargetColumn(filter.Column1, filter.Value1);
                        if (!found)
                            continue;
                    }

                    if (filter.Column2 != null)
                    {
	                    bool found;
                        (found, column2, value2) = TranslateTargetColumn(filter.Column2, filter.Value2);
                        if (!found)
                            continue;
                    }

					var newFilter = new Filter
					{
						Column1 = column1,
						Column2 = column2,
						Operator = filter.Operator,
						Value1 = value1,
						Value2 = value2,
						CompareDataType = filter.CompareDataType
					};

					if (mappedFilters.TryAdd(newFilter, filter))
					{
						newFilters.Add(newFilter);
					}
                }

                newSelectQuery.Filters = newFilters;
			}

            var mappedSorts = new Dictionary<Sort, Sort>();
            
			//we need to translate filters and sorts to source column names before passing them through.
			if (requestQuery?.Sorts != null)
			{
				var newSorts = new Sorts();
				foreach (var sort in requestQuery.Sorts)
				{
					TableColumn column = null;
					if (sort.Column != null)
					{
						bool found;
						(found, column, _) = TranslateTargetColumn(sort.Column, null);
						if (!found)
							continue;
					}

					var newSort = new Sort
					{
						Column = column,
						Direction = sort.Direction
					};
					if (mappedSorts.TryAdd(newSort, sort))
					{
						newSorts.Add(newSort);
						// throw new TransformException($"The column {column.Name} is duplicated in the sort.  Check sorts and groups and avoid duplicate keys.");
					}
					
				}

				newSelectQuery.Sorts = newSorts;
			}

            SetRequestQuery(newSelectQuery, false);

            var returnValue = await PrimaryTransform.Open(auditKey, newSelectQuery, cancellationToken);
			
            GeneratedQuery = new SelectQuery();

            // if the primary transform is sorted, pass the sorts through based on the stored transalations
            if (PrimaryTransform.SortFields?.Count > 0)
            {
	            var sorts = PrimaryTransform.SortFields.Where(c => mappedSorts.ContainsKey(c)).Select(c => mappedSorts[c]).ToArray();
	            if (sorts.Length > 0)
	            {
		            GeneratedQuery.Sorts = new Sorts(sorts);
	            }
            }
            
            // if the primary transform is fil, pass the sorts through based on the stored translations
            if (PrimaryTransform.Filters?.Count > 0)
            {
	            var filters = PrimaryTransform.Filters.Where(c => mappedFilters.ContainsKey(c)).Select(c => mappedFilters[c]);
		        GeneratedQuery.Filters = new Filters(filters);
            }
            
			return returnValue;
        }
        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="targetColumn">Converts a target column into the mapped source column.</param>
        /// <param name="value"></param>
        /// <returns></returns>
        public (bool found, TableColumn column, object value) TranslateTargetColumn(TableColumn targetColumn, object value)
        {
	        if (targetColumn == null)
	        {
		        return (false, null, value);
	        }

	        if (Mappings != null)
	        {
		        foreach (var mapping in Mappings)
		        {
			        switch (mapping)
			        {
				        case MapColumn mapColumn:
					        if (mapColumn.InputColumn != null && mapColumn.OutputColumn.Compare(targetColumn))
					        {
						        return (true, mapColumn.InputColumn.Copy(), null);
					        }

					        break;
				        case MapInputColumn mapInputColumn:
					        if (mapInputColumn.InputColumn != null && mapInputColumn.InputColumn.Compare(targetColumn))
					        {
						        return (true, null, mapInputColumn.InputValue);
					        }

					        break;
				        default:
					        // if the column is mapped through another type of mapping, then it can't be translated.
					        var columns = mapping.GetOutputColumns(false);
					        if (columns.Any(c => c.Compare(targetColumn)))
					        {
						        return (false, null, null);
					        }

					        break;
			        }

		        }
		        
		        if (Mappings.PassThroughColumns)
		        {
			        var column = PrimaryTransform.CacheTable.Columns.SingleOrDefault(c => c.Name == targetColumn.Name);
			        if (column != null)
			        {
				        return (true, targetColumn, null);
			        }
		        }
			}

	        return (false, null, null);
        }

        #region Transform Implementations

        public override bool RequiresSort => false;
        
	    public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
			while (true) // while loop is used to allow a function with skip record to work.
			{
				var newRow = new object[FieldCount];

				var readResult = await PrimaryTransform.ReadAsync(cancellationToken);
				if (readResult == false)
				{
					return null;
				}

				try
				{
					var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
					if (ignore)
					{
						TransformRowsIgnored++;
						continue;
					}
					Mappings.MapOutputRow(newRow);
				}
				catch (FunctionIgnoreRowException)
				{
					TransformRowsIgnored++;
					continue;
				}

				return newRow;
			}
        }

        public override Sorts RequiredSortFields()
        {
            return null;
        }
        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }


        #endregion
    }
}
