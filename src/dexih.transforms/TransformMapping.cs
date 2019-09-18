using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Exceptions;
using dexih.functions.Query;
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

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            if (selectQuery?.Rows > 0 && selectQuery.Rows < MaxOutputRows)
            {
                MaxOutputRows = selectQuery.Rows;
            }
            selectQuery = selectQuery?.CloneProperties<SelectQuery>() ?? new SelectQuery();
            
            // get only the required columns
            selectQuery.Columns = Mappings.GetRequiredColumns()?.Select(c => new SelectColumn(c)).ToList();
            
	        //we need to translate filters and sorts to source column names before passing them through.
            if(selectQuery?.Filters != null)
            {
                var newFilters = new List<Filter>();
                foreach(var filter in selectQuery.Filters)
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

					newFilters.Add(newFilter);
				}

				selectQuery.Filters = newFilters;
			}

			//we need to translate filters and sorts to source column names before passing them through.
			if (selectQuery?.Sorts != null)
			{
				var newSorts = new Sorts();
				foreach (var sort in selectQuery.Sorts)
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
					newSorts.Add(newSort);
				}

				selectQuery.Sorts = newSorts;
			}

            SetSelectQuery(selectQuery, false);

            var returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
			
			return returnValue;
        }

	    /// <summary>
	    /// 
	    /// </summary>
	    /// <param name="targetColumn">Converts a target column into the mapped source column.</param>
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
			        if (mapping is MapColumn mapColumn)
			        {
				        if (mapColumn.InputColumn != null && mapColumn.OutputColumn.Compare(targetColumn))
				        {
					        return (true, mapColumn.InputColumn.Copy(), null);
				        }
			        }

			        if (mapping is MapInputColumn mapInputColumn)
			        {
				        if (mapInputColumn.InputColumn != null && mapInputColumn.InputColumn.Compare(targetColumn))
				        {
					        return (true, null, mapInputColumn.InputValue);
				        }
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

	    public override Sorts SortFields => CacheTable.OutputSortFields;

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
