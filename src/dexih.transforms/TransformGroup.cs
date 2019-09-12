using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;

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

        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override string TransformName { get; } = "Group";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        
        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            if (selectQuery.Rows > 0 && selectQuery.Rows < MaxOutputRows)
            {
                MaxOutputRows = selectQuery.Rows;
            }
            selectQuery = selectQuery?.CloneProperties<SelectQuery>() ?? new SelectQuery();
            
            // get only the required columns
            selectQuery.Columns = Mappings.GetRequiredColumns()?.Select(c => new SelectColumn(c)).ToList();


            var requiredSorts = RequiredSortFields();

            if(selectQuery.Sorts != null && selectQuery.Sorts.Count > 0)
            {
                for(var i =0; i<requiredSorts.Count; i++)
                {
                    if (selectQuery.Sorts[i].Column == requiredSorts[i].Column)
                        requiredSorts[i].Direction = selectQuery.Sorts[i].Direction;
                    else
                        break;
                }
            }

            selectQuery.Sorts = requiredSorts;

            SetSelectQuery(selectQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
            return returnValue;
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
                            if (nextGroupValues[i] == null && _groupValues != null ||
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

        public override List<Sort> RequiredSortFields()
        {
            var sortFields = Mappings.OfType<MapGroup>().Select(c=> new Sort { Column = c.InputColumn, Direction = Sort.EDirection.Ascending }).ToList();

            var seriesMapping = (MapSeries) Mappings.SingleOrDefault(c => c is MapSeries _);
            if (seriesMapping != null)
            {
                sortFields.Add(new Sort { Column = seriesMapping.InputColumn, Direction = Sort.EDirection.Ascending });
            }
            
            return sortFields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
