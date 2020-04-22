using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Group",
        Description = "Group columns and apply specific aggregation rules to other columns.",
        TransformType = ETransformType.Group
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
        
        private Queue<object[]> _cachedRows;

        // indicates the database is executing the group by function.
        private bool _isPushDownQuery;

        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override string TransformName { get; } = "Group";

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

            var newSelectQuery = new SelectQuery
            {
                Columns = new SelectColumns(Mappings.GetRequiredColumns(includeAggregate: true))
            };
            
            var requiredSorts = RequiredSortFields();

            if (requestQuery?.Sorts?.Count > 0)
            {
                for(var i =0; i < requiredSorts.Count; i++)
                {
                    if (requestQuery.Sorts.Count > i && requestQuery.Sorts[i].Column.Name == requiredSorts[i].Column.Name)
                    {
                        requiredSorts[i].Direction = requestQuery.Sorts[i].Direction;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            var groups = new List<TableColumn>();

            // check if all the data can be pushed to a query.
            var canPushDownGroup = true;
            foreach (var mapping in Mappings)
            {
                switch (mapping)
                {
                    case MapGroup mapGroup:
                        groups.Add(mapGroup.InputColumn);
                        break;
                    case MapAggregate mapAggregate:
                        // first & last aggregates are not supported for push down
                        if (mapAggregate.Aggregate == EAggregate.First ||
                            mapAggregate.Aggregate == EAggregate.Last)
                            canPushDownGroup = false;
                        break;
                    default:
                        canPushDownGroup = false;
                        break;
                }

                if (!canPushDownGroup) break;
            }

            if (canPushDownGroup)
            {
                newSelectQuery.Groups = groups;

                // transform any requested filters, to group filters.
                if (requestQuery?.Filters?.Count > 0)
                {
                    foreach (var filter in requestQuery.Filters)
                    {
                        var groupFilter = new Filter()
                        {
                            Value1 = filter.Value1,
                            Value2 = filter.Value2,
                            Operator = filter.Operator
                        };
                        if (filter.Column1 != null)
                        {
                            var col = newSelectQuery.Columns.SingleOrDefault(c =>
                                c.OutputColumn?.Name == filter.Column1.Name || c.Column.Name == filter.Column1.Name);
                            if (col == null)
                            {
                                continue;
                            }

                            groupFilter.Column1 = col.OutputColumn ?? col.Column;
                        }
                        if (filter.Column2 != null)
                        {
                            var col = newSelectQuery.Columns.SingleOrDefault(c =>
                                c.OutputColumn?.Name == filter.Column2.Name || c.Column.Name == filter.Column2.Name);
                            if (col == null)
                            {
                                continue;
                            }

                            groupFilter.Column2 = col.OutputColumn ?? col.Column;
                        }

                        newSelectQuery.GroupFilters.Add(groupFilter);
                    }
                }
            }
            else
            {
                var newColumns = new HashSet<SelectColumn>();
                foreach (var column in newSelectQuery.Columns)
                {
                    column.Aggregate = EAggregate.None;
                    column.OutputColumn = null;
                    newColumns.Add(column);
                }

                newSelectQuery.Columns = new SelectColumns(newColumns);
            }

            newSelectQuery.Sorts = requiredSorts;
            
            SetRequestQuery(newSelectQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, newSelectQuery, cancellationToken);

            // the group will maintain the sorts
            GeneratedQuery = new SelectQuery()
            {
                Sorts = newSelectQuery.Sorts,
            };
                
            // confirm if the query was successfully pushed down by matching the generated query columns with expected columns
            if (canPushDownGroup && PrimaryTransform.GeneratedQuery != null)
            {
                var generatedQuery = PrimaryTransform.GeneratedQuery;
                var matched = true;
                foreach (var mapping in Mappings)
                {
                    switch (mapping)
                    {
                        case MapGroup mapGroup:
                            if (!(generatedQuery?.Groups?.Exists(c => c.Name == mapGroup.InputColumn.Name) ?? true))
                            {
                                matched = false;
                            }
                            break;
                        case MapAggregate mapAggregate:
                            if (!(generatedQuery?.Columns?.Exists(c => c.Column.Name == mapAggregate.InputColumn.Name && c.Aggregate == mapAggregate.Aggregate) ?? true))
                            {
                                matched = false;
                            }
                            break;
                        default:
                            canPushDownGroup = false;
                            break;
                    }

                    if (!matched) break;
                }

                if (matched)
                {
                    _isPushDownQuery = true;
                    GeneratedQuery.Filters = requestQuery.GroupFilters;
                }
            }
            
            return returnValue;
        }

        protected override SelectQuery GetGeneratedQuery(SelectQuery requestQuery)
        {
            return base.GetGeneratedQuery(requestQuery);
        }

        public override bool ResetTransform()
        {
            Mappings.Reset(EFunctionType.Aggregate);
            Mappings.Reset(EFunctionType.Series);

            _firstRecord = true;
            _lastRecord = true;

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_isPushDownQuery)
            {
                var returnValue = PrimaryTransform.Read();
                return returnValue ? PrimaryTransform.CurrentRow : null;
            }
            
            var outputRow = new object[FieldCount];

            if (_firstRecord)
            {
                _cachedRows = new Queue<object[]>();
            } else if (_cachedRows.Any())
            {
                outputRow = _cachedRows.Dequeue();
                return outputRow;
            } else if (!_firstRecord && !_lastRecord)
            {
                var (_, ignore)  = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                if (ignore)
                {
                    TransformRowsIgnored += 1;
                }
            }
            
            // used to track if the group fields have changed
            var groupChanged = false;
            
            if (PrimaryTransform.IsReaderFinished || await PrimaryTransform.ReadAsync(cancellationToken) == false)
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

                    // get group values of the new row
                    var nextGroupValues = Mappings.GetGroupValues(PrimaryTransform.CurrentRow);
                    
                    //if it's the first record then the group values are being set for the first time.
                    if (_firstRecord)
                    {
                        _groupValues = nextGroupValues;
                    }
                    else
                    {
                        //if not first row, then check if the group values have changed from the previous row
                        for (var i = 0; i < nextGroupValues.Length; i++)
                        {
                            if ((nextGroupValues[i] == null && _groupValues?[i] != null) ||
                                (nextGroupValues[i] != null && _groupValues == null) ||
                                !Equals(nextGroupValues[i], _groupValues[i]) )
                            {
                                groupChanged = true;
                                break;
                            }
                        }
                    }
                    
                    if (!groupChanged)
                    {
                        // if the group has not changed, process the input row
                        var (_, ignore)  = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);

                        if (ignore)
                        {
                            TransformRowsIgnored += 1;
                            continue;
                        }
                            
                    }
                    // when group has changed
                    else
                    {
                        await ProcessGroupChange(outputRow, cancellationToken);
                        
                        //store the last groupValues read to start the next grouping.
                        _groupValues = nextGroupValues;

                    }
                    
                    _firstRecord = false;

                    if (groupChanged)
                    {
                        break;
                    }

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            if (_firstRecord)
            {
                return null;
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                await ProcessGroupChange(outputRow, cancellationToken);

                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow;

        }

        private async Task ProcessGroupChange(object[] outputRow, CancellationToken cancellationToken)
        {
            Mappings.MapOutputRow(outputRow);
            var (moreRows, ignore)  = await Mappings.ProcessAggregateRow(new FunctionVariables(), outputRow, EFunctionType.Aggregate, cancellationToken);

            if (ignore && !moreRows)
            {
                TransformRowsIgnored += 1;
                return;
            }

            // if the aggregate function wants to provide more rows, store them in a separate collection.
            while (moreRows)
            {
                var rowCopy = new object[FieldCount];
                outputRow.CopyTo(rowCopy, 0);
                (moreRows, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), rowCopy, EFunctionType.Aggregate, cancellationToken);
                
                if (ignore)
                {
                    TransformRowsIgnored += 1;
                }
                else
                {
                    _cachedRows.Enqueue(rowCopy);    
                }
            }
            
            Mappings.Reset(EFunctionType.Aggregate);
        }

        public override Sorts RequiredSortFields()
        {
            var sortFields = new Sorts(Mappings.OfType<MapGroup>().Select(c=> new Sort { Column = c.InputColumn, Direction = ESortDirection.Ascending }));

            var seriesMapping = (MapSeries) Mappings.SingleOrDefault(c => c is MapSeries _);
            if (seriesMapping != null)
            {
                sortFields.Add(new Sort { Column = seriesMapping.InputColumn, Direction = ESortDirection.Ascending });
            }
            
            return sortFields;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
