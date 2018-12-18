using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Exceptions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
	[Transform(
		Name = "Mapping",
		Description = "Apply mapping rules from source to target.",
		TransformType = TransformAttribute.ETransformType.Mapping
	)]
    public class TransformMapping : Transform 
    {
        public TransformMapping() { }

        public TransformMapping(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inTransform);
        }

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

	        //we need to translate filters and sorts to source column names before passing them through.
            if(query?.Filters != null)
            {
                var newFilters = new List<Filter>();
                foreach(var filter in query.Filters)
                {
                    TableColumn column1 = null;
                    TableColumn column2 = null;
                    if (filter.Column1 != null)
                    {
                        column1 = TranslateTargetColumn(filter.Column1);
                        if (column1 == null)
                            continue;
                    }

                    if (filter.Column2 != null)
                    {
                        column2 = TranslateTargetColumn(filter.Column2);
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
			if (query?.Sorts != null)
			{
				var newSorts = new List<Sort>();
				foreach (var sort in query.Sorts)
				{
					TableColumn column = null;
					if (sort.Column != null)
					{
						column = TranslateTargetColumn(sort.Column);
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

	    /// <summary>
	    /// 
	    /// </summary>
	    /// <param name="targetColumn">Converts a target column into the mapped source column.</param>
	    /// <returns></returns>
        public TableColumn TranslateTargetColumn(TableColumn targetColumn)
        {
	        if (targetColumn == null)
	        {
		        return null;
	        }

	        if (Mappings != null)
	        {
		        foreach (var mapping in Mappings)
		        {
			        if (mapping is MapColumn mapColumn)
			        {
				        if (mapColumn.InputColumn != null && mapColumn.OutputColumn.Compare(targetColumn))
				        {
					        return mapColumn.InputColumn.Copy();
				        }
			        }
		        }
		        
		        if (Mappings.PassThroughColumns)
		        {
			        var column = PrimaryTransform.CacheTable.Columns.SingleOrDefault(c => c.Name == targetColumn.Name);
			        if (column != null)
			        {
				        return targetColumn;
			        }
		        }
			}

	        return null;
        }

        public override string Details()
        {
			return "Mapping(" + Name + "):  Mappings:" + (Mappings?.Count.ToString() ?? "None");
        }

        #region Transform Implementations

        public override bool RequiresSort => false;

	    public override List<Sort> SortFields => CacheTable.OutputSortFields;

	    public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
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
					await Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
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
